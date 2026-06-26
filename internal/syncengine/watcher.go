package syncengine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// ============================================================
// DBInterface — 数据库抽象接口，避免与 internal/db 循环依赖
// ============================================================

// DBInterface 定义同步引擎所需的全部数据库操作
type DBInterface interface {
	// UpsertFile 插入或更新文件元数据（路径、大小、哈希、版本号等）
	UpsertFile(path string, size int64, hash string, version int, deviceID string) error
	// GetFile 根据相对路径获取文件元数据
	GetFile(path string) (*FileRecord, error)
	// DeleteFile 标记文件为已删除（tombstone）
	DeleteFile(path string, deviceID string) error
	// RenameFile 将旧路径重命名为新路径
	RenameFile(oldPath, newPath string, deviceID string) error
	// AddEvent 写入一条同步事件日志
	AddEvent(event SyncEvent) error
	// GetLastSeq 返回当前最大事件序号
	GetLastSeq() (int64, error)
	// ListFiles 返回所有未删除的文件记录
	ListFiles() ([]*FileRecord, error)
}

// FileRecord 文件元数据记录
type FileRecord struct {
	Path     string `json:"path"`
	Size     int64  `json:"size"`
	Hash     string `json:"hash"`
	Version  int    `json:"version"`
	DeviceID string `json:"device_id"`
	Deleted  bool   `json:"deleted"`
	ModTime  int64  `json:"mod_time"`
}

// SyncEvent 同步事件
type SyncEvent struct {
	Type     string `json:"type"`               // create, update, delete, rename
	RelPath  string `json:"rel_path"`           // 相对于 syncDir 的路径
	OldPath  string `json:"old_path,omitempty"` // rename 时的旧路径
	Size     int64  `json:"size"`
	Hash     string `json:"hash"`
	Version  int    `json:"version"`
	DeviceID string `json:"device_id"`
}

// EventJSON 返回 SyncEvent 的 JSON 字节，用于写入日志
func (e SyncEvent) EventJSON() []byte {
	b, _ := json.Marshal(e)
	return b
}

// ============================================================
// Watcher — 服务端文件监控器
// ============================================================

// Watcher 监控服务端同步目录变化
type Watcher struct {
	db       DBInterface            // 数据库接口，避免直接依赖
	dataDir  string                 // 数据根目录 /data/.syncbox
	syncDir  string                 // 同步目录 /data/.syncbox/sync
	watcher  *fsnotify.Watcher      // fsnotify 底层监控实例
	debounce map[string]*time.Timer // 去抖定时器（key = 相对路径）
	mu       sync.Mutex             // 保护 debounce map
	onChange func(SyncEvent)        // 变更回调（通知 WebSocket）
	stopCh   chan struct{}          // 停止信号
	wg       sync.WaitGroup        // 等待后台 goroutine 退出
}

// NewWatcher 创建文件监控器实例
// dataDir 为数据根目录，syncDir = filepath.Join(dataDir, "sync")
func NewWatcher(db DBInterface, dataDir string) *Watcher {
	return &Watcher{
		db:       db,
		dataDir:  dataDir,
		syncDir:  filepath.Join(dataDir, "sync"),
		debounce: make(map[string]*time.Timer),
		stopCh:   make(chan struct{}),
	}
}

// SetOnChange 设置变更回调函数，当文件发生同步事件时触发
func (w *Watcher) SetOnChange(fn func(SyncEvent)) {
	w.onChange = fn
}

// Start 启动 fsnotify 监控 syncDir 及其所有子目录
func (w *Watcher) Start() error {
	// 确保同步目录存在
	if err := os.MkdirAll(w.syncDir, 0755); err != nil {
		return err
	}

	// 创建 fsnotify watcher
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	w.watcher = fw

	// 递归添加所有子目录到监控列表
	if err := w.addWatchRecursive(w.syncDir); err != nil {
		fw.Close()
		return err
	}

	// 启动事件处理循环
	w.wg.Add(1)
	go w.eventLoop()

	log.Printf("[watcher] 监控已启动，目录: %s", w.syncDir)
	return nil
}

// Stop 停止文件监控并释放资源
func (w *Watcher) Stop() {
	close(w.stopCh)
	w.wg.Wait()

	w.mu.Lock()
	// 取消所有未触发的去抖定时器
	for _, t := range w.debounce {
		t.Stop()
	}
	w.debounce = make(map[string]*time.Timer)
	w.mu.Unlock()

	if w.watcher != nil {
		w.watcher.Close()
	}
	log.Println("[watcher] 监控已停止")
}

// ============================================================
// 核心事件循环
// ============================================================

// eventLoop 主事件循环，监听 fsnotify 事件和错误
func (w *Watcher) eventLoop() {
	defer w.wg.Done()

	for {
		select {
		case <-w.stopCh:
			return

		case ev, ok := <-w.watcher.Events:
			if !ok {
				return
			}
			w.handleEvent(ev)

		case err, ok := <-w.watcher.Errors:
			if !ok {
				return
			}
			log.Printf("[watcher] fsnotify 错误: %v", err)
		}
	}
}

// handleEvent 处理单个 fsnotify 事件
func (w *Watcher) handleEvent(ev fsnotify.Event) {
	// 计算相对路径
	relPath, err := filepath.Rel(w.syncDir, ev.Name)
	if err != nil {
		log.Printf("[watcher] 计算相对路径失败: %v", err)
		return
	}
	// 统一使用正斜杠，保持跨平台一致性
	relPath = filepath.ToSlash(relPath)

	// 检查是否是目录事件
	info, statErr := os.Stat(ev.Name)

	// 新创建的目录需要自动加入监控
	if ev.Op&fsnotify.Create != 0 && statErr == nil && info.IsDir() {
		if err := w.addWatchRecursive(ev.Name); err != nil {
			log.Printf("[watcher] 递归添加目录监控失败 %s: %v", ev.Name, err)
		}
		log.Printf("[watcher] 新目录已加入监控: %s", relPath)
		return
	}

	// 只处理文件事件，忽略目录的写入/删除等
	if statErr == nil && info.IsDir() {
		return
	}

	switch {
	case ev.Op&fsnotify.Create != 0:
		// 创建事件：500ms 去抖后处理
		w.debounceEvent(relPath, ev.Name, "create")

	case ev.Op&fsnotify.Write != 0:
		// 写入事件：500ms 去抖后处理
		w.debounceEvent(relPath, ev.Name, "update")

	case ev.Op&fsnotify.Remove != 0:
		// 删除事件：立即处理（无需去抖）
		w.handleRemove(relPath)

	case ev.Op&fsnotify.Rename != 0:
		// 重命名事件：立即处理
		w.handleRename(relPath, ev.Name)
	}
}

// ============================================================
// 去抖处理
// ============================================================

// debounceEvent 对 create/write 事件进行 500ms 去抖，避免频繁触发
func (w *Watcher) debounceEvent(relPath, absPath, eventType string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 如果已有定时器，先取消
	if t, ok := w.debounce[relPath]; ok {
		t.Stop()
	}

	// 设置新的 500ms 定时器
	w.debounce[relPath] = time.AfterFunc(500*time.Millisecond, func() {
		// 定时器触发后清理 map 中的引用
		w.mu.Lock()
		delete(w.debounce, relPath)
		w.mu.Unlock()

		switch eventType {
		case "create":
			w.handleCreate(relPath, absPath)
		case "update":
			w.handleWrite(relPath, absPath)
		}
	})
}

// ============================================================
// 各类事件处理器
// ============================================================

// handleCreate 处理文件创建事件：计算哈希、入库、触发回调
func (w *Watcher) handleCreate(relPath, absPath string) {
	// 计算文件 SHA256
	hash, err := w.fileHash(absPath)
	if err != nil {
		// 文件可能已被删除（竞争条件），跳过
		if os.IsNotExist(err) {
			return
		}
		log.Printf("[watcher] 计算哈希失败 %s: %v", relPath, err)
		return
	}

	// 获取文件大小
	info, err := os.Stat(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Printf("[watcher] 获取文件信息失败 %s: %v", relPath, err)
		return
	}

	// 查询旧版本号
	newVersion := 1
	rec, err := w.db.GetFile(relPath)
	if err == nil && rec != nil {
		newVersion = rec.Version + 1
	}

	// 写入数据库
	if err := w.db.UpsertFile(relPath, info.Size(), hash, newVersion, ""); err != nil {
		log.Printf("[watcher] UpsertFile 失败 %s: %v", relPath, err)
		return
	}

	// 构造同步事件
	eventType := "create"
	if rec != nil {
		eventType = "update"
	}
	evt := SyncEvent{
		Type:    eventType,
		RelPath: relPath,
		Size:    info.Size(),
		Hash:    hash,
		Version: newVersion,
	}

	// 记录事件到数据库
	if err := w.db.AddEvent(evt); err != nil {
		log.Printf("[watcher] AddEvent 失败: %v", err)
	}

	// 触发变更回调
	w.triggerOnChange(evt)

	log.Printf("[watcher] %s: %s (hash=%s, size=%d)", eventType, relPath, shortHash(hash), info.Size())
}

// handleWrite 处理文件写入事件：重新计算哈希并与旧值比较
func (w *Watcher) handleWrite(relPath, absPath string) {
	hash, err := w.fileHash(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Printf("[watcher] 计算哈希失败 %s: %v", relPath, err)
		return
	}

	// 查询旧记录，如果哈希没变则跳过
	rec, err := w.db.GetFile(relPath)
	if err == nil && rec != nil && rec.Hash == hash {
		// 文件内容未变化，跳过（可能是 chmod 等元数据变更）
		return
	}

	info, err := os.Stat(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Printf("[watcher] 获取文件信息失败 %s: %v", relPath, err)
		return
	}

	newVersion := 1
	if rec != nil {
		newVersion = rec.Version + 1
	}

	if err := w.db.UpsertFile(relPath, info.Size(), hash, newVersion, ""); err != nil {
		log.Printf("[watcher] UpsertFile 失败 %s: %v", relPath, err)
		return
	}

	evt := SyncEvent{
		Type:    "update",
		RelPath: relPath,
		Size:    info.Size(),
		Hash:    hash,
		Version: newVersion,
	}

	if err := w.db.AddEvent(evt); err != nil {
		log.Printf("[watcher] AddEvent 失败: %v", err)
	}

	w.triggerOnChange(evt)
	log.Printf("[watcher] update: %s (hash=%s, size=%d)", relPath, shortHash(hash), info.Size())
}

// handleRemove 处理文件删除事件：标记 tombstone
func (w *Watcher) handleRemove(relPath string) {
	// 取消该文件可能存在的去抖定时器
	w.mu.Lock()
	if t, ok := w.debounce[relPath]; ok {
		t.Stop()
		delete(w.debounce, relPath)
	}
	w.mu.Unlock()

	// 检查数据库中是否存在该文件
	rec, err := w.db.GetFile(relPath)
	if err != nil || rec == nil {
		return // 未知文件，忽略
	}

	// 标记删除（tombstone）
	if err := w.db.DeleteFile(relPath, ""); err != nil {
		log.Printf("[watcher] DeleteFile 失败 %s: %v", relPath, err)
		return
	}

	evt := SyncEvent{
		Type:    "delete",
		RelPath: relPath,
		Size:    rec.Size,
		Hash:    rec.Hash,
		Version: rec.Version + 1,
	}

	if err := w.db.AddEvent(evt); err != nil {
		log.Printf("[watcher] AddEvent 失败: %v", err)
	}

	w.triggerOnChange(evt)
	log.Printf("[watcher] delete: %s", relPath)
}

// handleRename 处理文件重命名事件
// 注意：fsnotify 在某些平台上会同时发送 rename 和 create 事件
// 我们依赖数据库中已有旧记录，通过文件系统扫描确认新路径
func (w *Watcher) handleRename(relPath, absPath string) {
	// 取消去抖定时器
	w.mu.Lock()
	if t, ok := w.debounce[relPath]; ok {
		t.Stop()
		delete(w.debounce, relPath)
	}
	w.mu.Unlock()

	// 检查数据库中是否存在旧记录
	rec, err := w.db.GetFile(relPath)
	if err != nil || rec == nil {
		return
	}

	// fsnotify 的 rename 事件不携带新路径
	// 在 Linux 上通常会伴随一个 create 事件，该事件会自动处理新文件
	// 这里先标记旧路径为已删除
	if err := w.db.DeleteFile(relPath, ""); err != nil {
		log.Printf("[watcher] rename 时删除旧记录失败 %s: %v", relPath, err)
	}

	evt := SyncEvent{
		Type:    "delete",
		RelPath: relPath,
		OldPath: relPath,
		Size:    rec.Size,
		Hash:    rec.Hash,
		Version: rec.Version + 1,
	}

	if err := w.db.AddEvent(evt); err != nil {
		log.Printf("[watcher] AddEvent 失败: %v", err)
	}

	w.triggerOnChange(evt)
	log.Printf("[watcher] rename(旧路径已删除): %s", relPath)

	// 注意：新路径的创建会由 create 事件的 handleCreate 处理
}

// ============================================================
// 全量扫描
// ============================================================

// ScanDir 全量扫描同步目录，比对文件系统和数据库，发现新增/修改/删除
// 在启动时或 WebSocket 重连时调用
func (w *Watcher) ScanDir() error {
	log.Printf("[watcher] 开始全量扫描: %s", w.syncDir)

	// 收集文件系统中的所有文件（相对路径 -> 文件信息）
	fsFiles := make(map[string]os.FileInfo)
	err := filepath.Walk(w.syncDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// 跳过目录本身，只记录文件
		if info.IsDir() {
			return nil
		}
		relPath, err := filepath.Rel(w.syncDir, path)
		if err != nil {
			return err
		}
		relPath = filepath.ToSlash(relPath)
		fsFiles[relPath] = info
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// 获取数据库中的所有文件记录
	dbFiles, err := w.db.ListFiles()
	if err != nil {
		return err
	}
	dbMap := make(map[string]*FileRecord)
	for _, rec := range dbFiles {
		if !rec.Deleted {
			dbMap[rec.Path] = rec
		}
	}

	// 检查新增和修改的文件
	for relPath, info := range fsFiles {
		absPath := filepath.Join(w.syncDir, filepath.FromSlash(relPath))
		hash, err := w.fileHash(absPath)
		if err != nil {
			log.Printf("[watcher] 扫描时计算哈希失败 %s: %v", relPath, err)
			continue
		}

		rec, exists := dbMap[relPath]
		if !exists {
			// 新增文件：数据库中不存在
			if err := w.db.UpsertFile(relPath, info.Size(), hash, 1, ""); err != nil {
				log.Printf("[watcher] 扫描入库失败 %s: %v", relPath, err)
				continue
			}
			evt := SyncEvent{
				Type:    "create",
				RelPath: relPath,
				Size:    info.Size(),
				Hash:    hash,
				Version: 1,
			}
			if err := w.db.AddEvent(evt); err != nil {
				log.Printf("[watcher] AddEvent 失败: %v", err)
			}
			w.triggerOnChange(evt)
			log.Printf("[watcher] 扫描发现新增: %s", relPath)
		} else if rec.Hash != hash {
			// 修改文件：哈希不一致
			newVersion := rec.Version + 1
			if err := w.db.UpsertFile(relPath, info.Size(), hash, newVersion, ""); err != nil {
				log.Printf("[watcher] 扫描更新失败 %s: %v", relPath, err)
				continue
			}
			evt := SyncEvent{
				Type:    "update",
				RelPath: relPath,
				Size:    info.Size(),
				Hash:    hash,
				Version: newVersion,
			}
			if err := w.db.AddEvent(evt); err != nil {
				log.Printf("[watcher] AddEvent 失败: %v", err)
			}
			w.triggerOnChange(evt)
			log.Printf("[watcher] 扫描发现修改: %s", relPath)
		}
		// 哈希一致：无需更新
		delete(dbMap, relPath)
	}

	// dbMap 中剩余的记录：文件系统中已不存在，标记删除
	for relPath, rec := range dbMap {
		if rec.Deleted {
			continue
		}
		if err := w.db.DeleteFile(relPath, ""); err != nil {
			log.Printf("[watcher] 扫描删除标记失败 %s: %v", relPath, err)
			continue
		}
		evt := SyncEvent{
			Type:    "delete",
			RelPath: relPath,
			Size:    rec.Size,
			Hash:    rec.Hash,
			Version: rec.Version + 1,
		}
		if err := w.db.AddEvent(evt); err != nil {
			log.Printf("[watcher] AddEvent 失败: %v", err)
		}
		w.triggerOnChange(evt)
		log.Printf("[watcher] 扫描发现已删除: %s", relPath)
	}

	log.Printf("[watcher] 全量扫描完成，文件系统文件数: %d", len(fsFiles))
	return nil
}

// ============================================================
// 工具方法
// ============================================================

// addWatchRecursive 递归地将目录及其所有子目录加入 fsnotify 监控
func (w *Watcher) addWatchRecursive(dir string) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if err := w.watcher.Add(path); err != nil {
				log.Printf("[watcher] 添加监控目录失败 %s: %v", path, err)
				return err
			}
		}
		return nil
	})
}

// fileHash 计算文件的 SHA256 哈希值，返回十六进制字符串
func (w *Watcher) fileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// triggerOnChange 安全触发变更回调（防止 nil panic）
func (w *Watcher) triggerOnChange(evt SyncEvent) {
	if w.onChange != nil {
		w.onChange(evt)
	}
}

// shortHash 返回哈希的前 12 个字符，用于日志显示
func shortHash(hash string) string {
	if len(hash) > 12 {
		return hash[:12]
	}
	return hash
}

// ============================================================
// 辅助函数（包级别，可被外部调用）
// ============================================================

// SyncDirPath 根据 dataDir 拼接出 sync 目录路径
func SyncDirPath(dataDir string) string {
	return filepath.Join(dataDir, "sync")
}

// RelToSync 将绝对路径转换为相对于 syncDir 的正斜杠路径
func RelToSync(syncDir, absPath string) string {
	rel, err := filepath.Rel(syncDir, absPath)
	if err != nil {
		return absPath
	}
	return filepath.ToSlash(rel)
}

// IgnorePath 检查路径是否应被忽略（临时文件、隐藏文件等）
func IgnorePath(relPath string) bool {
	base := filepath.Base(relPath)
	// 忽略隐藏文件（以 . 开头）
	if strings.HasPrefix(base, ".") {
		return true
	}
	// 忽略常见的临时文件后缀
	if strings.HasSuffix(base, "~") || strings.HasSuffix(base, ".tmp") || strings.HasSuffix(base, ".swp") {
		return true
	}
	// 忽略系统回收站等特殊目录
	if strings.HasPrefix(relPath, ".Trash") || strings.HasPrefix(relPath, "$RECYCLE.BIN") {
		return true
	}
	return false
}