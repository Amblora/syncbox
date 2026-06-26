package client

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/gorilla/websocket"
)

// SyncState 同步状态
type SyncState string

const (
	StateIdle     SyncState = "idle"
	StateSyncing  SyncState = "syncing"
	StatePaused   SyncState = "paused"
	StateError    SyncState = "error"
	StateStopping SyncState = "stopping"
)

// SyncEvent 同步事件，用于通知 GUI 等上层组件
type SyncEvent struct {
	Time    time.Time `json:"time"`
	Type    string    `json:"type"` // upload, download, delete, rename, error, info
	RelPath string    `json:"rel_path"`
	Message string    `json:"message"`
}

// ConflictInfo 冲突信息
type ConflictInfo struct {
	RelPath      string `json:"rel_path"`
	LocalHash    string `json:"local_hash"`
	ServerHash   string `json:"server_hash"`
	LocalModTime string `json:"local_mod_time"`
	ServerModTime string `json:"server_mod_time"`
}

// FileState 本地文件同步状态
type FileState struct {
	RelPath     string `json:"rel_path"`
	Hash        string `json:"hash"`
	Size        int64  `json:"size"`
	Version     int64  `json:"version"`
	SyncedAt    string `json:"synced_at"`
	IsDirectory bool   `json:"is_directory,omitempty"`
}

// SyncDir 同步目录信息（从服务端获取）
type SyncDir struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	RootPath string `json:"root_path,omitempty"`
}

// SyncStatus 同步状态快照（供 GUI 读取）
type SyncStatus struct {
	State        SyncState    `json:"state"`
	ServerURL    string       `json:"server_url"`
	LocalDir     string       `json:"local_dir"`
	DeviceID     string       `json:"device_id"`
	SyncDirID    string       `json:"sync_dir_id"`
	TotalFiles   int          `json:"total_files"`
	SyncedFiles  int          `json:"synced_files"`
	LastSyncTime string       `json:"last_sync_time"`
	Conflicts    []ConflictInfo `json:"conflicts,omitempty"`
	LastError    string       `json:"last_error,omitempty"`
}

// SyncClient 同步客户端
type SyncClient struct {
	ServerURL string
	Token     string
	LocalDir  string
	SyncDirID string
	DeviceID  string

	state        SyncState
	watcher      *fsnotify.Watcher
	wsConn       *websocket.Conn
	httpClient   *http.Client
	fileStates   map[string]*FileState // rel_path -> 文件状态
	conflicts    []ConflictInfo
	lastSyncTime time.Time
	lastError    string
	totalFiles   int
	syncedFiles  int

	// 事件通道，供 GUI 等上层消费
	eventCh chan SyncEvent

	// 去抖定时器
	debounceTimer *time.Timer
	debounceMu    sync.Mutex
	pendingPaths  map[string]struct{}
	renamedPaths  map[string]struct{} // 记录Rename事件的旧路径

	// 控制并发
	mu       sync.RWMutex
	wg       sync.WaitGroup
	stopCh   chan struct{}
	pausedCh chan struct{} // 非 nil 表示已暂停，关闭则恢复
	pauseMu  sync.Mutex

	// 扫描间隔
	scanInterval time.Duration

	// 状态持久化文件路径
	stateFile string

	// 远端目录名
	SyncDirName string

	// 速度统计
	uploadBytes   int64
	downloadBytes int64
	speedMu       sync.Mutex
}

// NewSyncClient 创建新的同步客户端
func NewSyncClient(serverURL, token, localDir string) *SyncClient {
	// 确保 serverURL 不以 / 结尾
	serverURL = strings.TrimRight(serverURL, "/")
	// 确保 localDir 为绝对路径
	absDir, err := filepath.Abs(localDir)
	if err != nil {
		absDir = localDir
	}
	return &SyncClient{
		ServerURL:    serverURL,
		Token:        token,
		LocalDir:     absDir,
		httpClient:   &http.Client{Timeout: 0, Transport: &http.Transport{MaxIdleConns: 10, MaxIdleConnsPerHost: 5, IdleConnTimeout: 30 * time.Second}},
		fileStates:   make(map[string]*FileState),
		eventCh:      make(chan SyncEvent, 64),
		pendingPaths: make(map[string]struct{}),
		renamedPaths:  make(map[string]struct{}),
		stopCh:       make(chan struct{}),
		scanInterval: 5 * time.Minute,
		stateFile:    filepath.Join(absDir, ".syncbox_state.json"),
	}
}

// Events 返回事件通道（只读）
func (c *SyncClient) Events() <-chan SyncEvent {
	return c.eventCh
}

// Status 返回当前同步状态快照
func (c *SyncClient) Status() SyncStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return SyncStatus{
		State:        c.state,
		ServerURL:    c.ServerURL,
		LocalDir:     c.LocalDir,
		DeviceID:     c.DeviceID,
		SyncDirID:    c.SyncDirID,
		TotalFiles:   c.totalFiles,
		SyncedFiles:  c.syncedFiles,
		LastSyncTime: c.lastSyncTime.Format(time.RFC3339),
		Conflicts:    append([]ConflictInfo(nil), c.conflicts...),
		LastError:    c.lastError,
	}
}

// Conflicts 返回当前冲突列表
func (c *SyncClient) Conflicts() []ConflictInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]ConflictInfo, len(c.conflicts))
	copy(result, c.conflicts)
	return result
}

// 发送事件
func (c *SyncClient) emitEvent(eventType, relPath, message string) {
	evt := SyncEvent{
		Time:    time.Now(),
		Type:    eventType,
		RelPath: relPath,
		Message: message,
	}
	select {
	case c.eventCh <- evt:
	default:
		// 通道满时丢弃旧事件
	}
	log.Printf("[事件] %s: %s — %s", eventType, relPath, message)
}

// Register 向服务端注册设备，获取 token 和 device_id
// Register 向服务端注册设备，需要提供管理员创建的 device_token
func (c *SyncClient) Register(deviceName string) error {
	log.Printf("[注册] 向服务端注册设备: %s", deviceName)

	// 发送设备名称、平台和管理员创建的接入令牌
	body := map[string]string{
		"name":         deviceName,
		"platform":     runtime.GOOS,
		"device_token": c.Token, // Token字段存储的是管理员创建的接入令牌
	}
	jsonData, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("序列化注册信息失败: %w", err)
	}

	req, err := http.NewRequest("POST", c.ServerURL+"/api/client/register", bytes.NewReader(jsonData))
	if err != nil {
		return fmt.Errorf("创建注册请求失败: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("注册请求失败: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		// 尝试解析错误信息
		var errResp struct {
			Error string `json:"error"`
		}
		json.Unmarshal(respBody, &errResp)
		if errResp.Error != "" {
			return fmt.Errorf("注册失败: %s", errResp.Error)
		}
		return fmt.Errorf("注册失败，服务端返回 %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		ID          string `json:"id"`
		DeviceToken string `json:"device_token"`
		Name        string `json:"name"`
		Existed     bool   `json:"existed"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return fmt.Errorf("解析注册响应失败: %w", err)
	}

	// DeviceID 使用服务端返回的设备ID，Token保持接入令牌不变
	c.DeviceID = result.ID
	// c.Token 已经是管理员创建的接入令牌，不需要替换
	log.Printf("[注册] 成功，设备ID: %s，已存在: %v", c.DeviceID, result.Existed)
	c.emitEvent("info", "", fmt.Sprintf("设备注册成功，ID: %s", c.DeviceID))
	return nil
}

// Connect 连接服务端，获取同步目录列表，创建本地同步目录
func (c *SyncClient) Connect() error {
	log.Printf("[连接] 连接服务端: %s", c.ServerURL)

	// 获取同步目录列表
	req, err := http.NewRequest("GET", c.ServerURL+"/api/client/sync_dirs", nil)
	if err != nil {
		return fmt.Errorf("创建请求失败: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.DeviceID+":"+c.Token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("获取同步目录失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("获取同步目录失败，服务端返回 %d: %s", resp.StatusCode, string(respBody))
	}

	var dirs []SyncDir
	if err := json.NewDecoder(resp.Body).Decode(&dirs); err != nil {
		return fmt.Errorf("解析同步目录失败: %w", err)
	}

	// 根据 SyncDirName 匹配远端目录
	if len(dirs) == 0 {
		return fmt.Errorf("服务端无可用同步目录")
	}
	matched := false
	if c.SyncDirName != "" {
		for _, d := range dirs {
			if d.Name == c.SyncDirName {
				c.SyncDirID = d.ID
				matched = true
				log.Printf("[连接] 使用远端目录: %s (ID: %s)", d.Name, c.SyncDirID)
				break
			}
		}
	}
	if !matched {
		c.SyncDirID = dirs[0].ID
		c.SyncDirName = dirs[0].Name
		log.Printf("[连接] 使用默认目录: %s (ID: %s)", dirs[0].Name, c.SyncDirID)
	}

	// 确保本地目录存在
	if err := os.MkdirAll(c.LocalDir, 0755); err != nil {
		return fmt.Errorf("创建本地目录失败: %w", err)
	}

	c.emitEvent("info", "", fmt.Sprintf("连接成功，同步目录: %s", dirs[0].Name))
	return nil
}

// Hello 向服务端发送心跳，确认连接存活
func (c *SyncClient) Hello() error {
	req, err := http.NewRequest("POST", c.ServerURL+"/api/client/hello", nil)
	if err != nil {
		return fmt.Errorf("创建心跳请求失败: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.DeviceID+":"+c.Token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("心跳失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("心跳失败，服务端返回 %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// StopCh 返回停止信号通道（供外部监听）
func (c *SyncClient) StopCh() <-chan struct{} {
	return c.stopCh
}

// Start 启动同步
func (c *SyncClient) Start() error {
	c.mu.Lock()
	if c.state == StateSyncing {
		c.mu.Unlock()
		return fmt.Errorf("同步已在运行中")
	}
	c.state = StateSyncing
	c.stopCh = make(chan struct{})
	c.mu.Unlock()

	log.Printf("[同步] 启动同步，本地目录: %s", c.LocalDir)
	c.emitEvent("info", "", "同步已启动")

	// 加载本地状态
	c.loadState()

	// 1. 启动 fsnotify 监控本地目录
	if err := c.startWatcher(); err != nil {
		return fmt.Errorf("启动文件监控失败: %w", err)
	}

	// 2. 启动 WebSocket 连接
	c.wg.Add(1)
	go c.websocketLoop()

	// 3. 启动全量扫描（首次启动）
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		// 等待一小段时间让 WebSocket 连接建立
		time.Sleep(1 * time.Second)
		c.ScanAndSync()
	}()

	// 4. 启动兜底扫描（每5分钟）
	c.wg.Add(1)
	go c.periodicScanLoop()

	return nil
}

// Stop 停止所有同步操作
func (c *SyncClient) Stop() {
	c.mu.Lock()
	if c.state == StateStopping || c.state == StateIdle {
		c.mu.Unlock()
		return
	}
	c.state = StateStopping
	c.mu.Unlock()

	log.Printf("[同步] 正在停止同步...")
	c.emitEvent("info", "", "正在停止同步...")

	close(c.stopCh)

	// 恢复暂停（如果有），避免死锁
	c.pauseMu.Lock()
	if c.pausedCh != nil {
		close(c.pausedCh)
		c.pausedCh = nil
	}
	c.pauseMu.Unlock()

	// 停止文件监控
	if c.watcher != nil {
		c.watcher.Close()
	}

	// 关闭 WebSocket
	if c.wsConn != nil {
		c.wsConn.Close()
	}

	// 等待所有 goroutine 结束
	c.wg.Wait()

	// 保存状态
	c.saveState()

	c.mu.Lock()
	c.state = StateIdle
	c.mu.Unlock()

	log.Printf("[同步] 同步已停止")
	c.emitEvent("info", "", "同步已停止")
}

// Pause 暂停同步
func (c *SyncClient) Pause() {
	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()

	c.mu.RLock()
	isSyncing := c.state == StateSyncing
	c.mu.RUnlock()

	if !isSyncing {
		return
	}

	if c.pausedCh != nil {
		return // 已经暂停
	}
	c.pausedCh = make(chan struct{})

	c.mu.Lock()
	c.state = StatePaused
	c.mu.Unlock()

	log.Printf("[同步] 同步已暂停")
	c.emitEvent("info", "", "同步已暂停")
}

// Resume 继续同步
func (c *SyncClient) Resume() {
	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()

	if c.pausedCh == nil {
		return // 未暂停
	}
	close(c.pausedCh)
	c.pausedCh = nil

	c.mu.Lock()
	c.state = StateSyncing
	c.mu.Unlock()

	log.Printf("[同步] 同步已恢复")
	c.emitEvent("info", "", "同步已恢复")
}

// waitIfPaused 检查是否暂停，如果暂停则等待恢复
func (c *SyncClient) waitIfPaused() {
	c.pauseMu.Lock()
	ch := c.pausedCh
	c.pauseMu.Unlock()

	if ch != nil {
		<-ch
	}
}

// isStopped 检查是否已停止
func (c *SyncClient) isStopped() bool {
	select {
	case <-c.stopCh:
		return true
	default:
		return false
	}
}

// startWatcher 启动 fsnotify 文件监控
func (c *SyncClient) startWatcher() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	c.watcher = watcher

	// 递归添加目录监控
	if err := c.addWatchRecursive(c.LocalDir); err != nil {
		return err
	}

	c.wg.Add(1)
	go c.watchLoop()

	return nil
}

// addWatchRecursive 递归添加目录监控
func (c *SyncClient) addWatchRecursive(dir string) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			// 跳过隐藏目录
			name := info.Name()
			if strings.HasPrefix(name, ".") && name != "." {
				return filepath.SkipDir
			}
			return c.watcher.Add(path)
		}
		return nil
	})
}

// watchLoop 文件监控循环
func (c *SyncClient) watchLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.stopCh:
			return
		case event, ok := <-c.watcher.Events:
			if !ok {
				return
			}
			// 忽略隐藏文件和 .syncbox_state.json
			base := filepath.Base(event.Name)
			if strings.HasPrefix(base, ".") {
				continue
			}
			// 跳过临时文件
			if strings.HasSuffix(base, ".syncbox_tmp") {
				continue
			}

			// 新目录被创建时，添加监控
			if event.Op&fsnotify.Create != 0 {
				info, err := os.Stat(event.Name)
				if err == nil && info.IsDir() {
					c.watcher.Add(event.Name)
				}
			}

			// Rename事件：记录旧路径用于删除同步
			if event.Op&fsnotify.Rename != 0 {
				c.debounceMu.Lock()
				c.renamedPaths[event.Name] = struct{}{}
				c.debounceMu.Unlock()
			}

			// 记录变化路径，触发去抖处理
			c.debounceMu.Lock()
			c.pendingPaths[event.Name] = struct{}{}
			if c.debounceTimer != nil {
				c.debounceTimer.Stop()
			}
			c.debounceTimer = time.AfterFunc(500*time.Millisecond, c.processPendingChanges)
			c.debounceMu.Unlock()

		case err, ok := <-c.watcher.Errors:
			if !ok {
				return
			}
			log.Printf("[监控] 错误: %v", err)
		}
	}
}

// processPendingChanges 处理去抖后的文件变化
func (c *SyncClient) processPendingChanges() {
	c.debounceMu.Lock()
	paths := make(map[string]struct{}, len(c.pendingPaths))
	for k, v := range c.pendingPaths {
		paths[k] = v
	}
	c.pendingPaths = make(map[string]struct{})
	c.debounceMu.Unlock()

	if c.isStopped() {
		return
	}
	c.waitIfPaused()

	// 提取renamedPaths
	c.debounceMu.Lock()
	renamed := make(map[string]struct{}, len(c.renamedPaths))
	for k, v := range c.renamedPaths {
		renamed[k] = v
	}
	c.renamedPaths = make(map[string]struct{})
	c.debounceMu.Unlock()

	for path := range paths {
		relPath, err := filepath.Rel(c.LocalDir, path)
		if err != nil {
			continue
		}
		relPath = filepath.ToSlash(relPath)

		// 检查文件是否还存在
		info, err := os.Stat(path)
		if os.IsNotExist(err) {
			// 文件已删除或被移动
			if _, wasRenamed := renamed[path]; wasRenamed {
				// 这是Rename事件的旧路径，文件被移动了，通知服务端删除旧文件
				log.Printf("[移动] 检测到文件移动(旧路径): %s", relPath)
			}
			c.handleLocalDelete(relPath)
			continue
		}
		if err != nil {
			continue
		}
		if info.IsDir() {
			// 新建目录：通知服务端创建目录（通过创建一个空的.gitkeep文件）
			c.handleLocalDirCreate(relPath)
			continue
		}

		// 跳过临时文件
		if strings.HasSuffix(relPath, ".syncbox_tmp") {
			continue
		}
		// 文件变化或创建
		c.handleLocalChange(relPath, path, info)
	}
}

// handleLocalChange 处理本地文件变化（上传）
func (c *SyncClient) handleLocalChange(relPath, absPath string, info os.FileInfo) {
	// 跳过临时文件
	if strings.HasSuffix(relPath, ".syncbox_tmp") {
		return
	}
	// 计算文件 hash
	hash, err := c.computeFileHash(absPath)
	if err != nil {
		log.Printf("[上传] 计算 hash 失败: %s, 错误: %v", relPath, err)
		return
	}

	// 检查是否与已同步的状态相同
	c.mu.RLock()
	existing, exists := c.fileStates[relPath]
	c.mu.RUnlock()
	if exists && existing.Hash == hash && existing.Size == info.Size() {
		return // 文件未实际变化
	}

	log.Printf("[上传] 检测到变化: %s (hash: %s, size: %d)", relPath, hash[:8], info.Size())
	c.emitEvent("upload", relPath, fmt.Sprintf("正在上传: %s", relPath))

	// Seafile风格：直接上传，不需要 baseVersion
	// 服务端始终接受上传并递增版本号
	newVersion, err := c.uploadFile(relPath, absPath, hash, info.Size(), 0)
	if err != nil {
		log.Printf("[上传] 失败: %s, 错误: %v", relPath, err)
		c.emitEvent("error", relPath, fmt.Sprintf("上传失败: %v", err))
		return
	}

	// 更新本地状态
	c.mu.Lock()
	c.fileStates[relPath] = &FileState{
		RelPath:  relPath,
		Hash:     hash,
		Size:     info.Size(),
		Version:  newVersion,
		SyncedAt: time.Now().Format(time.RFC3339),
	}
	c.syncedFiles = len(c.fileStates)
	c.lastSyncTime = time.Now()
	c.mu.Unlock()

	c.emitEvent("info", relPath, fmt.Sprintf("上传成功，版本: %d", newVersion))
	c.saveState()
}

// handleLocalDirCreate 处理本地目录创建（在服务端创建对应目录）
func (c *SyncClient) handleLocalDirCreate(relPath string) {
	log.Printf("[目录] 同步新建目录: %s", relPath)
	c.emitEvent("info", relPath, fmt.Sprintf("同步新建目录: %s", relPath))
	// 通过上传一个.gitkeep占位文件来在服务端建立目录结构
	absDir := filepath.Join(c.LocalDir, filepath.FromSlash(relPath))
	os.MkdirAll(absDir, 0777)
	keepFile := filepath.Join(absDir, ".gitkeep")
	if _, err := os.Stat(keepFile); os.IsNotExist(err) {
		os.WriteFile(keepFile, []byte{}, 0666)
		// 上传.gitkeep到服务端
		keepRelPath := relPath + "/.gitkeep"
		c.uploadFile(keepRelPath, keepFile, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0, 0)
	}
}

// handleLocalDelete 处理本地文件或目录删除
func (c *SyncClient) handleLocalDelete(relPath string) {
	// 检查是否是已同步的文件或其子文件
	c.mu.RLock()
	_, exists := c.fileStates[relPath]
	// 也检查是否有以该路径为前缀的子文件（目录删除时）
	hasChildren := false
	if !exists {
		prefix := relPath + "/"
		for k := range c.fileStates {
			if strings.HasPrefix(k, prefix) {
				hasChildren = true
				break
			}
		}
	}
	c.mu.RUnlock()

	if !exists && !hasChildren {
		return // 从未同步过的文件或目录，忽略
	}

	log.Printf("[删除] 通知服务端删除: %s", relPath)
	c.emitEvent("delete", relPath, fmt.Sprintf("正在同步删除: %s", relPath))

	// 通知服务端删除 - 使用POST /api/client/delete
	delPath := relPath
		if c.SyncDirName != "" && c.SyncDirName != "/" {
			delPath = c.SyncDirName + "/" + relPath
		}
	deleteBody, _ := json.Marshal(map[string]string{"path": delPath})
	req, err := http.NewRequest("POST", c.ServerURL+"/api/client/delete", bytes.NewReader(deleteBody))
	if err != nil {
		log.Printf("[删除] 创建请求失败: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.DeviceID+":"+c.Token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.Printf("[删除] 请求失败: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent {
		c.mu.Lock()
		// 删除自身
		delete(c.fileStates, relPath)
		// 删除所有子文件（目录删除时）
		prefix := relPath + "/"
		for k := range c.fileStates {
			if strings.HasPrefix(k, prefix) {
				delete(c.fileStates, k)
			}
		}
		c.syncedFiles = len(c.fileStates)
		c.lastSyncTime = time.Now()
		c.mu.Unlock()

		c.emitEvent("info", relPath, "服务端删除同步完成")
		c.saveState()
	} else {
		log.Printf("[删除] 服务端返回: %d", resp.StatusCode)
	}
}

// uploadFile 上传文件到服务端
func (c *SyncClient) uploadFile(relPath, absPath, hash string, size, baseVersion int64) (int64, error) {
	// 分块流式上传，参考 Seafile
	file, err := os.Open(absPath)
	if err != nil {
		return 0, fmt.Errorf("打开文件失败: %w", err)
	}
	defer file.Close()

	pr, pw := io.Pipe()
	writer := multipart.NewWriter(pw)

	go func() {
		defer pw.Close()
		uploadPath := relPath
		if c.SyncDirName != "" && c.SyncDirName != "/" {
			uploadPath = c.SyncDirName + "/" + relPath
		}
		writer.WriteField("path", uploadPath)
		writer.WriteField("hash", hash)
		writer.WriteField("size", fmt.Sprintf("%d", size))
		writer.WriteField("sync_dir_id", c.SyncDirID)
		part, err := writer.CreateFormFile("file", filepath.Base(absPath))
		if err != nil {
			return
		}
		// 流式写入，不一次性加载到内存
		buf := make([]byte, 256*1024) // 256KB 缓冲
		for {
			n, err := file.Read(buf)
			if n > 0 {
				_, wErr := part.Write(buf[:n])
				if wErr != nil {
					break
				}
				// 统计上传字节
				c.speedMu.Lock()
				c.uploadBytes += int64(n)
				c.speedMu.Unlock()
			}
			if err != nil {
				break
			}
		}
		writer.Close()
	}()

	req, err := http.NewRequest("PUT", c.ServerURL+"/api/client/upload", pr)
	if err != nil {
		return 0, fmt.Errorf("创建上传请求失败: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+c.DeviceID+":"+c.Token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("上传请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("上传失败，服务端返回 %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Version int64 `json:"version"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("解析上传响应失败: %w", err)
	}

	return result.Version, nil
}

// downloadFile 从服务端下载文件
func (c *SyncClient) downloadFile(relPath string) error {
	dlPath := relPath
	if c.SyncDirName != "" && c.SyncDirName != "/" {
		dlPath = c.SyncDirName + "/" + relPath
	}
	url := fmt.Sprintf("%s/api/client/download?path=%s",
		c.ServerURL, url.QueryEscape(dlPath))

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("创建下载请求失败: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.DeviceID+":"+c.Token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("下载请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("下载失败，服务端返回 %d: %s", resp.StatusCode, string(respBody))
	}

	// 确保目标目录存在
	absPath := filepath.Join(c.LocalDir, filepath.FromSlash(relPath))
	dir := filepath.Dir(absPath)
	if err := os.MkdirAll(dir, 0777); err != nil {
		return fmt.Errorf("创建目录失败: %w", err)
	}

	// 写入临时文件后重命名，保证原子性
	tmpPath := absPath + ".syncbox_tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("创建临时文件失败: %w", err)
	}

	hasher := sha256.New()
	writer := io.MultiWriter(tmpFile, hasher)
	// 分块读取并统计下载速度
	buf := make([]byte, 256*1024)
	dlTotal := int64(0)
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if _, wErr := writer.Write(buf[:n]); wErr != nil {
				tmpFile.Close()
				os.Remove(tmpPath)
				return fmt.Errorf("写入文件失败: %w", wErr)
			}
			dlTotal += int64(n)
			c.RecordDownloadBytes(int64(n))
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			tmpFile.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("读取响应失败: %w", readErr)
		}
	}
	tmpFile.Close()

	// 重命名
	if err := os.Rename(tmpPath, absPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("重命名文件失败: %w", err)
	}
	// 确保文件权限正确（跨平台兼容）
	os.Chmod(absPath, 0666)
	// Windows下还需要修改目录ACL，让当前用户有完全控制权限
	if runtime.GOOS == "windows" {
		fixWindowsPermissions(filepath.Dir(absPath))
	}

	// 从响应头获取服务端的 hash 和 version（Seafile风格：快速同步）
	serverHash := resp.Header.Get("X-File-Hash")
	serverVersionStr := resp.Header.Get("X-File-Version")
	var serverVersion int64
	if serverVersionStr != "" {
		fmt.Sscanf(serverVersionStr, "%d", &serverVersion)
	}
	localHash := hex.EncodeToString(hasher.Sum(nil))
	stat, _ := os.Stat(absPath)

	// 使用服务端返回的 hash（如果有的话），否则用本地计算的
	if serverHash == "" {
		serverHash = localHash
	}

	c.mu.Lock()
	c.fileStates[relPath] = &FileState{
		RelPath:  relPath,
		Hash:     serverHash,
		Size:     stat.Size(),
		Version:  serverVersion,
		SyncedAt: time.Now().Format(time.RFC3339),
	}
	c.syncedFiles = len(c.fileStates)
	c.lastSyncTime = time.Now()
	c.mu.Unlock()

	c.saveState()
	return nil
}

// handleConflict 处理冲突
func (c *SyncClient) handleConflict(relPath, localHash string, localSize int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 避免重复添加
	for _, cf := range c.conflicts {
		if cf.RelPath == relPath {
			cf.LocalHash = localHash
			return
		}
	}

	c.conflicts = append(c.conflicts, ConflictInfo{
		RelPath:   relPath,
		LocalHash: localHash,
	})
	c.lastError = fmt.Sprintf("文件冲突: %s", relPath)
}

// ResolveConflict 解决冲突（以本地版本为准上传）
func (c *SyncClient) ResolveConflict(relPath string, useLocal bool) error {
	absPath := filepath.Join(c.LocalDir, filepath.FromSlash(relPath))

	if useLocal {
		// 以本地版本重新上传
		info, err := os.Stat(absPath)
		if err != nil {
			return fmt.Errorf("本地文件不存在: %w", err)
		}
		hash, err := c.computeFileHash(absPath)
		if err != nil {
			return err
		}
		_, err = c.uploadFile(relPath, absPath, hash, info.Size(), 0) // base_version=0 强制覆盖
		if err != nil {
			return err
		}
	} else {
		// 以服务端版本下载覆盖
		if err := c.downloadFile(relPath); err != nil {
			return err
		}
	}

	// 移除冲突记录
	c.mu.Lock()
	newConflicts := make([]ConflictInfo, 0, len(c.conflicts))
	for _, cf := range c.conflicts {
		if cf.RelPath != relPath {
			newConflicts = append(newConflicts, cf)
		}
	}
	c.conflicts = newConflicts
	c.mu.Unlock()

	c.emitEvent("info", relPath, fmt.Sprintf("冲突已解决（使用%s版本）", map[bool]string{true: "本地", false: "服务端"}[useLocal]))
	return nil
}

// websocketLoop WebSocket 连接循环（含断线重连）
func (c *SyncClient) websocketLoop() {
	defer c.wg.Done()

	for {
		if c.isStopped() {
			return
		}

		err := c.connectWebSocket()
		if err != nil {
			if c.isStopped() {
				return
			}
			log.Printf("[WebSocket] 连接失败: %v，5秒后重试", err)
			c.mu.Lock()
			c.state = StateError
			c.lastError = fmt.Sprintf("WebSocket 连接失败: %v", err)
			c.mu.Unlock()

			select {
			case <-c.stopCh:
				return
			case <-time.After(5 * time.Second):
			}

			// 重连后执行全量扫描
			c.mu.Lock()
			c.state = StateSyncing
			c.mu.Unlock()

			c.ScanAndSync()
			continue
		}

		// 连接成功，执行全量扫描
		c.ScanAndSync()

		// 监听消息
		c.listenWebSocket()

		if c.isStopped() {
			return
		}

		// 断线重连
		log.Printf("[WebSocket] 连接断开，5秒后重连")
		select {
		case <-c.stopCh:
			return
		case <-time.After(5 * time.Second):
		}
	}
}

// connectWebSocket 建立 WebSocket 连接
func (c *SyncClient) connectWebSocket() error {
	wsURL := strings.Replace(c.ServerURL, "http://", "ws://", 1)
	wsURL = strings.Replace(wsURL, "https://", "wss://", 1)
	wsURL += "/api/client/ws?token=" + c.DeviceID + ":" + c.Token

	log.Printf("[WebSocket] 连接: %s", wsURL)

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.wsConn = conn
	c.mu.Unlock()

	log.Printf("[WebSocket] 连接成功")
	c.emitEvent("info", "", "WebSocket 连接已建立")
	return nil
}

// listenWebSocket 监听 WebSocket 消息
func (c *SyncClient) listenWebSocket() {
	for {
		if c.isStopped() {
			return
		}

		_, message, err := c.wsConn.ReadMessage()
		if err != nil {
			if c.isStopped() {
				return
			}
			log.Printf("[WebSocket] 读取消息失败: %v", err)
			return
		}

		// 服务端消息格式: {"type":"xxx","data":{"rel_path":"xxx",...}}
		var msg struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(message, &msg); err != nil {
			log.Printf("[WebSocket] 解析消息失败: %v", err)
			continue
		}
		var event struct {
			Type    string `json:"type"`
			RelPath string `json:"rel_path"`
			NewPath string `json:"new_path,omitempty"`
			Version int64  `json:"version,omitempty"`
			Hash    string `json:"hash,omitempty"`
			Size    int64  `json:"size,omitempty"`
		}
		event.Type = msg.Type
		if msg.Data != nil {
			json.Unmarshal(msg.Data, &event)
		}

		c.waitIfPaused()
		c.handleServerEvent(event)
	}
}

// handleServerEvent 处理服务端推送的事件
func (c *SyncClient) handleServerEvent(event struct {
	Type    string `json:"type"`
	RelPath string `json:"rel_path"`
	NewPath string `json:"new_path,omitempty"`
	Version int64  `json:"version,omitempty"`
	Hash    string `json:"hash,omitempty"`
	Size    int64  `json:"size,omitempty"`
}) {
	// 按 SyncDirName 过滤服务端事件，非所选目录的事件直接忽略
	if c.SyncDirName != "" && c.SyncDirName != "/" {
		prefix := c.SyncDirName + "/"
		if !strings.HasPrefix(event.RelPath, prefix) {
			log.Printf("[服务端事件] 跳过非所选目录的事件: %s", event.RelPath)
			return
		}
		event.RelPath = strings.TrimPrefix(event.RelPath, prefix)
		if event.NewPath != "" {
			event.NewPath = strings.TrimPrefix(event.NewPath, prefix)
		}
	}
	log.Printf("[服务端事件] %s: %s", event.Type, event.RelPath)

	switch event.Type {
	case "file_change":
		// 下载文件到本地
		c.emitEvent("download", event.RelPath, fmt.Sprintf("正在下载: %s", event.RelPath))
		if err := c.downloadFile(event.RelPath); err != nil {
			log.Printf("[下载] 失败: %s, 错误: %v", event.RelPath, err)
			c.emitEvent("error", event.RelPath, fmt.Sprintf("下载失败: %v", err))
			return
		}
		// 更新版本号
		if event.Version > 0 {
			c.mu.Lock()
			if fs, ok := c.fileStates[event.RelPath]; ok {
				fs.Version = event.Version
			}
			c.mu.Unlock()
		}
		c.emitEvent("info", event.RelPath, fmt.Sprintf("下载完成: %s", event.RelPath))

	case "file_delete":
		// 删除本地文件或目录（支持递归删除目录）
		absPath := filepath.Join(c.LocalDir, filepath.FromSlash(event.RelPath))
		if err := os.RemoveAll(absPath); err != nil && !os.IsNotExist(err) {
			log.Printf("[删除] 本地删除失败: %s, 错误: %v", event.RelPath, err)
			c.emitEvent("error", event.RelPath, fmt.Sprintf("本地删除失败: %v", err))
			return
		}
		// 清理以该路径为前缀的所有文件状态（目录删除时需要清理子文件）
		c.mu.Lock()
		prefix := event.RelPath + "/"
		for k := range c.fileStates {
			if k == event.RelPath || strings.HasPrefix(k, prefix) {
				delete(c.fileStates, k)
			}
		}
		c.syncedFiles = len(c.fileStates)
		c.mu.Unlock()


		c.emitEvent("delete", event.RelPath, fmt.Sprintf("已删除: %s", event.RelPath))
		c.saveState()

	case "file_rename":
		// 重命名本地文件
		oldPath := filepath.Join(c.LocalDir, filepath.FromSlash(event.RelPath))
		newPath := filepath.Join(c.LocalDir, filepath.FromSlash(event.NewPath))

		// 确保目标目录存在
		if err := os.MkdirAll(filepath.Dir(newPath), 0755); err != nil {
			log.Printf("[重命名] 创建目录失败: %v", err)
			return
		}

		if err := os.Rename(oldPath, newPath); err != nil && !os.IsNotExist(err) {
			log.Printf("[重命名] 失败: %s -> %s, 错误: %v", event.RelPath, event.NewPath, err)
			c.emitEvent("error", event.RelPath, fmt.Sprintf("重命名失败: %v", err))
			return
		}

		c.mu.Lock()
		if fs, ok := c.fileStates[event.RelPath]; ok {
			delete(c.fileStates, event.RelPath)
			fs.RelPath = event.NewPath
			c.fileStates[event.NewPath] = fs
		}
		c.mu.Unlock()

		c.emitEvent("rename", event.RelPath, fmt.Sprintf("已重命名: %s -> %s", event.RelPath, event.NewPath))
		c.saveState()

	default:
		log.Printf("[服务端事件] 未知事件类型: %s", event.Type)
	}
}

// ScanAndSync 全量扫描并同步
func (c *SyncClient) ScanAndSync() {
	log.Printf("[扫描] 开始全量扫描...")
	c.emitEvent("info", "", "开始全量扫描...")

	// 获取服务端文件列表
	serverFiles, err := c.getServerFileList()
	if err != nil {
		log.Printf("[扫描] 获取服务端文件列表失败: %v", err)
		c.emitEvent("error", "", fmt.Sprintf("扫描失败: %v", err))
		return
	}

	// 扫描本地文件
	localFiles := make(map[string]os.FileInfo)
	filepath.Walk(c.LocalDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		// 跳过隐藏文件和目录
		base := filepath.Base(path)
		if strings.HasPrefix(base, ".") {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(base, ".syncbox_tmp") {
				return nil
			}
			if !info.IsDir() {
				relPath, _ := filepath.Rel(c.LocalDir, path)
				relPath = filepath.ToSlash(relPath)
				localFiles[relPath] = info
			}
		return nil
	})

	// 对比并同步
	for relPath, serverFile := range serverFiles {
		if c.isStopped() {
			return
		}
		c.waitIfPaused()

		localInfo, exists := localFiles[relPath]
		if !exists {
			// 本地不存在，下载
			log.Printf("[扫描] 下载新文件: %s", relPath)
			c.emitEvent("download", relPath, fmt.Sprintf("下载: %s", relPath))
			if err := c.downloadFile(relPath); err != nil {
				log.Printf("[扫描] 下载失败: %s, %v", relPath, err)
			} else {
				c.mu.Lock()
				if fs, ok := c.fileStates[relPath]; ok {
					fs.Version = serverFile.Version
				}
				c.mu.Unlock()
			}
			continue
		}

		// 本地存在，比较 hash
		localHash, err := c.computeFileHash(filepath.Join(c.LocalDir, filepath.FromSlash(relPath)))
		if err != nil {
			continue
		}

		if localHash == serverFile.Hash {
			// 一致，更新状态
			c.mu.Lock()
			c.fileStates[relPath] = &FileState{
				RelPath:  relPath,
				Hash:     localHash,
				Size:     localInfo.Size(),
				Version:  serverFile.Version,
				SyncedAt: time.Now().Format(time.RFC3339),
			}
			c.mu.Unlock()
		} else {
			// hash不一致 — Seafile风格智能同步
			// 判断：本地文件是否在上次同步后被修改过？
			absPath := filepath.Join(c.LocalDir, filepath.FromSlash(relPath))
			localMtime := localInfo.ModTime()

			// 获取上次同步时间
			c.mu.RLock()
			lastSync := c.fileStates[relPath]
			c.mu.RUnlock()

			var syncedAt time.Time
			if lastSync != nil && lastSync.SyncedAt != "" {
				syncedAt, _ = time.Parse(time.RFC3339, lastSync.SyncedAt)
			}

			if localMtime.After(syncedAt) && !syncedAt.IsZero() {
				// 本地文件在上次同步后被修改 → 上传本地版本
				log.Printf("[扫描] 本地修改，上传: %s", relPath)
				c.emitEvent("upload", relPath, fmt.Sprintf("本地已修改，上传: %s", relPath))
				c.handleLocalChange(relPath, absPath, localInfo)
			} else {
				// 本地文件未修改 → 下载服务端版本（其他设备的修改）
				log.Printf("[扫描] 远端更新，下载: %s", relPath)
				c.emitEvent("download", relPath, fmt.Sprintf("远端已更新，下载: %s", relPath))
				if err := c.downloadFile(relPath); err != nil {
					log.Printf("[扫描] 下载失败: %s, %v", relPath, err)
				}
			}
		}
	}

	// 检查本地存在但服务端不存在的文件 → 上传
	for relPath, localInfo := range localFiles {
		if c.isStopped() {
			return
		}
		c.waitIfPaused()

		if _, exists := serverFiles[relPath]; !exists {
			log.Printf("[扫描] 上传新文件: %s", relPath)
			absPath := filepath.Join(c.LocalDir, filepath.FromSlash(relPath))
			c.handleLocalChange(relPath, absPath, localInfo)
		}
	}

	c.mu.Lock()
	c.totalFiles = len(serverFiles)
	if len(localFiles) > c.totalFiles {
		c.totalFiles = len(localFiles)
	}
	c.lastSyncTime = time.Now()
	c.mu.Unlock()

	log.Printf("[扫描] 全量扫描完成，服务端 %d 个文件，本地 %d 个文件", len(serverFiles), len(localFiles))
	c.emitEvent("info", "", fmt.Sprintf("全量扫描完成（服务端: %d, 本地: %d）", len(serverFiles), len(localFiles)))
	c.saveState()
}

// serverFileInfo 服务端文件信息
type serverFileInfo struct {
	RelPath string `json:"rel_path"`
	Hash    string `json:"hash"`
	Size    int64  `json:"size"`
	Version int64  `json:"version"`
}

// getServerFileList 获取服务端文件列表
func (c *SyncClient) getServerFileList() (map[string]*serverFileInfo, error) {
	url := fmt.Sprintf("%s/api/client/files", c.ServerURL)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.DeviceID+":"+c.Token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("服务端返回 %d: %s", resp.StatusCode, string(body))
	}

	var files []serverFileInfo
	if err := json.NewDecoder(resp.Body).Decode(&files); err != nil {
		return nil, err
	}

	result := make(map[string]*serverFileInfo, len(files))
	for i := range files {
		files[i].RelPath = filepath.ToSlash(files[i].RelPath)
		// 按 SyncDirName 过滤，只同步选择的远端目录
		if c.SyncDirName != "" && c.SyncDirName != "/" {
			prefix := c.SyncDirName + "/"
			if !strings.HasPrefix(files[i].RelPath, prefix) {
				continue
			}
			files[i].RelPath = strings.TrimPrefix(files[i].RelPath, prefix)
		}
		result[files[i].RelPath] = &files[i]
	}
	return result, nil
}

// periodicScanLoop 兜底扫描循环
func (c *SyncClient) periodicScanLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.waitIfPaused()
			if !c.isStopped() {
				log.Printf("[扫描] 执行定时兜底扫描")
				c.ScanAndSync()
			}
		}
	}
}

// computeFileHash 计算文件 SHA256 hash
func (c *SyncClient) computeFileHash(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	buf := make([]byte, 64*1024)
	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
		}
		if readErr != nil {
			break
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
// saveState 保存同步状态到 JSON 文件
func (c *SyncClient) saveState() {
	c.mu.RLock()
	// 深拷贝fileStates，避免序列化时其他goroutine并发修改导致panic
	statesCopy := make(map[string]*FileState, len(c.fileStates))
	for k, v := range c.fileStates {
		cp := *v
		statesCopy[k] = &cp
	}
	state := struct {
		DeviceID     string                `json:"device_id"`
		SyncDirID    string                `json:"sync_dir_id"`
		FileStates   map[string]*FileState `json:"file_states"`
		LastSyncTime string                `json:"last_sync_time"`
	}{
		DeviceID:     c.DeviceID,
		SyncDirID:    c.SyncDirID,
		FileStates:   statesCopy,
		LastSyncTime: c.lastSyncTime.Format(time.RFC3339),
	}
	c.mu.RUnlock()

	data, err := json.Marshal(state)
	if err != nil {
		log.Printf("[状态] 序列化失败: %v", err)
		return
	}

	if err := os.WriteFile(c.stateFile, data, 0666); err != nil {
		log.Printf("[状态] 保存失败: %v", err)
	}
}

// loadState 从 JSON 文件加载同步状态
func (c *SyncClient) loadState() {
	data, err := os.ReadFile(c.stateFile)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("[状态] 读取失败: %v", err)
		}
		return
	}

	var state struct {
		DeviceID     string                `json:"device_id"`
		SyncDirID    string                `json:"sync_dir_id"`
		FileStates   map[string]*FileState `json:"file_states"`
		LastSyncTime string                `json:"last_sync_time"`
	}
	if err := json.Unmarshal(data, &state); err != nil {
		log.Printf("[状态] 解析失败: %v", err)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 只在当前DeviceID为空时才从状态文件加载，避免覆盖调用者设置的正确值
	if c.DeviceID == "" && state.DeviceID != "" {
		c.DeviceID = state.DeviceID
	}
	if state.SyncDirID != "" {
		c.SyncDirID = state.SyncDirID
	}
	if state.FileStates != nil {
		c.fileStates = state.FileStates
		c.syncedFiles = len(c.fileStates)
	}
	if t, err := time.Parse(time.RFC3339, state.LastSyncTime); err == nil {
		c.lastSyncTime = t
	}

	log.Printf("[状态] 已加载 %d 个文件状态", len(c.fileStates))
}

// SetScanInterval 修改扫描间隔
func (c *SyncClient) SetScanInterval(d time.Duration) {
	c.scanInterval = d
	log.Printf("[设置] 扫描间隔已修改为: %v", d)
}

// fixWindowsPermissions 在Windows上给目录添加当前用户的完全控制权限
func fixWindowsPermissions(dir string) {
	cmd := exec.Command("icacls", dir, "/grant", "Authenticated Users:(OI)(CI)F", "/T", "/Q")
	// 隐藏子进程窗口，防止弹出cmd窗口
	cmd.SysProcAttr = getHideWindowAttr()
	cmd.Run()
}
// GetSpeedStats 获取当前上传/下载速度（字节/秒）并重置计数器
func (c *SyncClient) GetSpeedStats() (uploadBps, downloadBps int64) {
	c.speedMu.Lock()
	uploadBps = c.uploadBytes
	downloadBps = c.downloadBytes
	c.uploadBytes = 0
	c.downloadBytes = 0
	c.speedMu.Unlock()
	return
}

// RecordDownloadBytes 记录下载字节数
func (c *SyncClient) RecordDownloadBytes(n int64) {
	c.speedMu.Lock()
	c.downloadBytes += n
	c.speedMu.Unlock()
}
