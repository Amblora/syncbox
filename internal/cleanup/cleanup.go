package cleanup

import (
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// DB 定义数据库操作接口，避免循环依赖
// 实际使用时由 db.DB 实现该接口
type DB interface {
	// GetResolvedConflicts 返回已解决的冲突记录路径列表
	GetResolvedConflicts(before time.Time) ([]string, error)
	// GetOrphanFileKeys 返回数据库中已不存在于磁盘的文件键列表
	GetOrphanFileKeys() ([]string, error)
	// Checkpoint 执行 WAL checkpoint，释放 WAL 文件空间
	Checkpoint() error
	// PurgeDeletedFiles 清理数据库中标记为已删除的文件记录
	PurgeDeletedFiles() (int, error)
	// PurgeOldSyncEvents 清理超过指定天数的同步事件
	PurgeOldSyncEvents(days int) (int, error)
	// PurgeOldActivityLogs 清理超过指定天数的活动日志
	PurgeOldActivityLogs(days int) (int, error)
	// PurgeResolvedConflicts 清理已解决的冲突记录
	PurgeResolvedConflicts() (int, error)
	// VacuumDB 压缩数据库文件
	VacuumDB() error
}

// ScanResult 清理扫描结果
type ScanResult struct {
	TempFiles         CleanItem  `json:"temp_files"`          // 过期临时文件
	OldLogs           CleanItem  `json:"old_logs"`            // 旧日志文件
	ResolvedConflicts CleanItem  `json:"resolved_conflicts"`  // 已解决的冲突
	OldConflicts      CleanItem  `json:"old_conflicts"`       // 未解决的旧冲突
	TrashFiles        CleanItem  `json:"trash_files"`         // 回收站文件
	Tombstones        CleanItem  `json:"tombstones"`          // 墓碑标记文件
	OrphanFiles       CleanItem  `json:"orphan_files"`        // 孤立文件
	DeletedFileRecords CleanItem `json:"deleted_file_records"` // 数据库中已标记删除的文件记录
	OldSyncEvents     CleanItem  `json:"old_sync_events"`     // 数据库中旧的同步事件
	OldActivityLogs   CleanItem  `json:"old_activity_logs"`   // 数据库中旧的活动日志
	ResolvedConflictRecords CleanItem `json:"resolved_conflict_records"` // 数据库中已解决的冲突记录
	LargeFiles        []LargeFile `json:"large_files"`        // 大文件列表
	TotalRecoverable  uint64     `json:"total_recoverable"`   // 总可回收空间（字节）
	Warnings          []string   `json:"warnings"`            // 警告信息列表
}

// CleanItem 单项清理信息
type CleanItem struct {
	Size     uint64 `json:"size"`      // 总大小（字节）
	Count    int    `json:"count"`     // 文件数量
	Risk     string `json:"risk"`      // 清理风险等级: low, medium, high
	CanClean bool   `json:"can_clean"` // 是否可以安全清理
}

// LargeFile 大文件信息
type LargeFile struct {
	Path     string `json:"path"`     // 文件路径
	Size     uint64 `json:"size"`     // 文件大小（字节）
	Modified int64  `json:"modified"` // 最后修改时间（Unix 时间戳）
}

// Cleaner 磁盘清理器
type Cleaner struct {
	dataDir string       // SyncBox 数据目录
	db      DB           // 数据库接口
	logs    *log.Logger  // 日志记录器
}

// NewCleaner 创建一个新的磁盘清理器
// dataDir: SyncBox 数据目录路径
// db: 数据库接口实现，用于查询冲突记录和执行 WAL checkpoint
func NewCleaner(dataDir string, db DB) *Cleaner {
	return &Cleaner{
		dataDir: dataDir,
		db:      db,
		logs:    log.New(os.Stderr, "[cleanup] ", log.LstdFlags),
	}
}

// Scan 扫描所有可清理的项目
// 返回 ScanResult 包含各类文件的大小、数量和清理建议
func (c *Cleaner) Scan() (*ScanResult, error) {
	result := &ScanResult{}

	// 扫描过期临时文件（>24小时）
	result.TempFiles = c.scanExpiredFiles(
		filepath.Join(c.dataDir, "temp"),
		24*time.Hour,
		"low",
	)

	// 扫描旧日志文件（>7天）
	result.OldLogs = c.scanExpiredFiles(
		filepath.Join(c.dataDir, "logs"),
		7*24*time.Hour,
		"low",
	)

	// 扫描已解决的冲突（>7天）
	result.ResolvedConflicts = c.scanExpiredFiles(
		filepath.Join(c.dataDir, "conflicts", "resolved"),
		7*24*time.Hour,
		"low",
	)

	// 扫描未解决的旧冲突（>30天）
	result.OldConflicts = c.scanExpiredFiles(
		filepath.Join(c.dataDir, "conflicts"),
		30*24*time.Hour,
		"medium",
	)

	// 扫描回收站文件
	result.TrashFiles = c.scanAllFiles(
		filepath.Join(c.dataDir, "trash"),
		"medium",
	)

	// 扫描墓碑标记文件
	result.Tombstones = c.scanAllFiles(
		filepath.Join(c.dataDir, "tombstones"),
		"low",
	)

	// 扫描孤立文件（数据库中无记录但磁盘存在）
	result.OrphanFiles = c.scanOrphanFiles()

	// 扫描数据库中已标记删除的文件记录
	result.DeletedFileRecords = c.scanDBDeletedFiles()

	// 扫描数据库中旧的同步事件（>30天）
	result.OldSyncEvents = c.scanDBOldSyncEvents(30)

	// 扫描数据库中旧的活动日志（>30天）
	result.OldActivityLogs = c.scanDBOldActivityLogs(30)

	// 扫描数据库中已解决的冲突记录
	result.ResolvedConflictRecords = c.scanDBResolvedConflicts()

	// 扫描大文件（>100MB）
	result.LargeFiles = c.scanLargeFiles(100 * 1024 * 1024)

	// 计算总可回收空间（仅统计低风险可清理项，包括数据库记录）
	result.TotalRecoverable = result.TempFiles.Size +
		result.OldLogs.Size +
		result.ResolvedConflicts.Size +
		result.Tombstones.Size +
		result.DeletedFileRecords.Size +
		result.OldSyncEvents.Size +
		result.OldActivityLogs.Size +
		result.ResolvedConflictRecords.Size

	// 生成警告信息
	result.Warnings = c.generateWarnings(result)

	c.logs.Printf("扫描完成: 可回收空间 %d 字节 (%.2f MB), 大文件 %d 个",
		result.TotalRecoverable,
		float64(result.TotalRecoverable)/(1024*1024),
		len(result.LargeFiles),
	)

	return result, nil
}

// SafeClean 一键安全清理
// 仅清理低风险项目，保证数据安全：
//   - 过期临时文件（>24小时）
//   - 旧日志文件（>7天）
//   - 已解决的冲突文件（>7天）
//   - 执行 WAL checkpoint 释放数据库 WAL 空间
//   - 清理空目录
//
// 返回释放的总空间（字节）和可能的错误
func (c *Cleaner) SafeClean() (uint64, error) {
	var totalCleaned uint64
	c.logs.Println("开始安全清理...")

	// 清理过期临时文件（>24小时）
	size, count := c.cleanExpiredFiles(
		filepath.Join(c.dataDir, "temp"),
		24*time.Hour,
	)
	totalCleaned += size
	c.logs.Printf("清理临时文件: %d 个文件, 释放 %d 字节", count, size)

	// 清理旧日志文件（>7天）
	size, count = c.cleanExpiredFiles(
		filepath.Join(c.dataDir, "logs"),
		7*24*time.Hour,
	)
	totalCleaned += size
	c.logs.Printf("清理旧日志: %d 个文件, 释放 %d 字节", count, size)

	// 清理已解决的冲突文件（>7天）
	size, count = c.cleanExpiredFiles(
		filepath.Join(c.dataDir, "conflicts", "resolved"),
		7*24*time.Hour,
	)
	totalCleaned += size
	c.logs.Printf("清理已解决冲突: %d 个文件, 释放 %d 字节", count, size)

	// 清理数据库中已标记删除的文件记录
	if c.db != nil {
		purged, err := c.db.PurgeDeletedFiles()
		if err != nil {
			c.logs.Printf("清理已删除文件记录失败: %v", err)
		} else if purged > 0 {
			c.logs.Printf("清理已删除文件记录: %d 条", purged)
		}
	}

	// 清理数据库中超过30天的同步事件
	if c.db != nil {
		purged, err := c.db.PurgeOldSyncEvents(30)
		if err != nil {
			c.logs.Printf("清理旧同步事件失败: %v", err)
		} else if purged > 0 {
			c.logs.Printf("清理旧同步事件: %d 条", purged)
		}
	}

	// 清理数据库中超过30天的活动日志
	if c.db != nil {
		purged, err := c.db.PurgeOldActivityLogs(30)
		if err != nil {
			c.logs.Printf("清理旧活动日志失败: %v", err)
		} else if purged > 0 {
			c.logs.Printf("清理旧活动日志: %d 条", purged)
		}
	}

	// 清理数据库中已解决的冲突记录
	if c.db != nil {
		purged, err := c.db.PurgeResolvedConflicts()
		if err != nil {
			c.logs.Printf("清理已解决冲突记录失败: %v", err)
		} else if purged > 0 {
			c.logs.Printf("清理已解决冲突记录: %d 条", purged)
		}
	}

	// 执行 WAL checkpoint，释放数据库 WAL 文件空间
	if c.db != nil {
		if err := c.db.Checkpoint(); err != nil {
			c.logs.Printf("WAL checkpoint 失败: %v", err)
		} else {
			c.logs.Println("WAL checkpoint 完成")
		}
		// 执行 VACUUM 压缩数据库
		if err := c.db.VacuumDB(); err != nil {
			c.logs.Printf("VACUUM 失败: %v", err)
		} else {
			c.logs.Println("VACUUM 完成")
		}
	}

	// 清理空目录
	emptyCount := c.cleanEmptyDirs()
	c.logs.Printf("清理空目录: %d 个", emptyCount)

	c.logs.Printf("安全清理完成, 总共释放 %d 字节 (%.2f MB)",
		totalCleaned,
		float64(totalCleaned)/(1024*1024),
	)

	return totalCleaned, nil
}

// DeepClean 深度清理
// 在安全清理基础上，额外清理高风险项目：
//   - 未解决的旧冲突（>30天）
//   - 清空回收站
//   - 孤立文件
//
// targets: 指定要清理的目标列表，可选值:
//   - "old_conflicts": 未解决的旧冲突
//   - "trash": 回收站
//   - "orphans": 孤立文件
//   - "all": 清理所有深度目标
//
// 返回释放的总空间（字节）和可能的错误
func (c *Cleaner) DeepClean(targets []string) (uint64, error) {
	var totalCleaned uint64
	c.logs.Println("开始深度清理...")

	// 判断是否清理全部
	cleanAll := contains(targets, "all")

	// 清理未解决的旧冲突（>30天）
	if cleanAll || contains(targets, "old_conflicts") {
		size, count := c.cleanExpiredFiles(
			filepath.Join(c.dataDir, "conflicts"),
			30*24*time.Hour,
		)
		totalCleaned += size
		c.logs.Printf("清理旧冲突: %d 个文件, 释放 %d 字节", count, size)
	}

	// 清空回收站
	if cleanAll || contains(targets, "trash") {
		size, count := c.cleanAllInDir(filepath.Join(c.dataDir, "trash"))
		totalCleaned += size
		c.logs.Printf("清空回收站: %d 个文件, 释放 %d 字节", count, size)
	}

	// 清理孤立文件
	if cleanAll || contains(targets, "orphans") {
		size, count := c.cleanOrphanFiles()
		totalCleaned += size
		c.logs.Printf("清理孤立文件: %d 个文件, 释放 %d 字节", count, size)
	}

	// 清理墓碑标记文件
	if cleanAll || contains(targets, "tombstones") {
		size, count := c.cleanAllInDir(filepath.Join(c.dataDir, "tombstones"))
		totalCleaned += size
		c.logs.Printf("清理墓碑标记: %d 个文件, 释放 %d 字节", count, size)
	}

	// 清理数据库无用记录
	if c.db != nil {
		if cleanAll || contains(targets, "db_records") {
			// 清理已标记删除的文件记录
			purged, err := c.db.PurgeDeletedFiles()
			if err != nil {
				c.logs.Printf("清理已删除文件记录失败: %v", err)
			} else {
				c.logs.Printf("清理已删除文件记录: %d 条", purged)
			}

			// 清理超过30天的同步事件
			purged, err = c.db.PurgeOldSyncEvents(30)
			if err != nil {
				c.logs.Printf("清理旧同步事件失败: %v", err)
			} else {
				c.logs.Printf("清理旧同步事件: %d 条", purged)
			}

			// 清理超过30天的活动日志
			purged, err = c.db.PurgeOldActivityLogs(30)
			if err != nil {
				c.logs.Printf("清理旧活动日志失败: %v", err)
			} else {
				c.logs.Printf("清理旧活动日志: %d 条", purged)
			}

			// 清理已解决的冲突记录
			purged, err = c.db.PurgeResolvedConflicts()
			if err != nil {
				c.logs.Printf("清理已解决冲突记录失败: %v", err)
			} else {
				c.logs.Printf("清理已解决冲突记录: %d 条", purged)
			}

			// VACUUM 压缩数据库
			if err := c.db.VacuumDB(); err != nil {
				c.logs.Printf("VACUUM 失败: %v", err)
			} else {
				c.logs.Println("VACUUM 完成，数据库已压缩")
			}
		}
	}

	// 清理空目录
	c.cleanEmptyDirs()

	c.logs.Printf("深度清理完成, 总共释放 %d 字节 (%.2f MB)",
		totalCleaned,
		float64(totalCleaned)/(1024*1024),
	)

	return totalCleaned, nil
}

// ========== 扫描辅助方法 ==========

// scanExpiredFiles 扫描目录下超过指定时间的文件
func (c *Cleaner) scanExpiredFiles(dir string, maxAge time.Duration, risk string) CleanItem {
	item := CleanItem{Risk: risk, CanClean: risk == "low"}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return item
	}

	cutoff := time.Now().Add(-maxAge)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		if info.ModTime().Before(cutoff) {
			item.Size += uint64(info.Size())
			item.Count++
		}
	}

	return item
}

// scanAllFiles 扫描目录下所有文件（不过滤时间）
func (c *Cleaner) scanAllFiles(dir string, risk string) CleanItem {
	item := CleanItem{Risk: risk, CanClean: risk == "low"}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return item
	}

	for _, entry := range entries {
		if entry.IsDir() {
			// 递归统计子目录
			sub := c.scanAllFiles(filepath.Join(dir, entry.Name()), risk)
			item.Size += sub.Size
			item.Count += sub.Count
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		item.Size += uint64(info.Size())
		item.Count++
	}

	return item
}

// scanOrphanFiles 扫描孤立文件（数据库中无记录但磁盘存在）
func (c *Cleaner) scanOrphanFiles() CleanItem {
	item := CleanItem{Risk: "high", CanClean: false}

	if c.db == nil {
		return item
	}

	// 从数据库获取孤立文件键列表
	keys, err := c.db.GetOrphanFileKeys()
	if err != nil {
		c.logs.Printf("查询孤立文件失败: %v", err)
		return item
	}

	for _, key := range keys {
		fullPath := filepath.Join(c.dataDir, "sync", key)
		info, err := os.Stat(fullPath)
		if err != nil {
			continue
		}

		if !info.IsDir() {
			item.Size += uint64(info.Size())
			item.Count++
		}
	}

	return item
}

// scanLargeFiles 扫描超过指定大小的文件
// threshold: 文件大小阈值（字节）
func (c *Cleaner) scanLargeFiles(threshold uint64) []LargeFile {
	var largeFiles []LargeFile

	syncDir := filepath.Join(c.dataDir, "sync")

	filepath.Walk(syncDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // 跳过无法访问的文件
		}

		if info.IsDir() {
			return nil
		}

		if uint64(info.Size()) >= threshold {
			largeFiles = append(largeFiles, LargeFile{
				Path:     path,
				Size:     uint64(info.Size()),
				Modified: info.ModTime().Unix(),
			})
		}

		return nil
	})

	// 按大小降序排列
	sort.Slice(largeFiles, func(i, j int) bool {
		return largeFiles[i].Size > largeFiles[j].Size
	})

	return largeFiles
}

// generateWarnings 根据扫描结果生成警告信息
func (c *Cleaner) generateWarnings(result *ScanResult) []string {
	var warnings []string

	// 临时文件过多
	if result.TempFiles.Count > 1000 {
		warnings = append(warnings, "临时文件数量超过 1000，建议立即清理")
	}

	// 回收站占用过大（>500MB）
	if result.TrashFiles.Size > 500*1024*1024 {
		warnings = append(warnings, "回收站占用超过 500MB，建议清空回收站")
	}

	// 存在孤立文件
	if result.OrphanFiles.Count > 0 {
		warnings = append(warnings, "检测到孤立文件，可能存在数据不一致")
	}

	// 存在大文件
	if len(result.LargeFiles) > 0 {
		warnings = append(warnings, "检测到大文件（>100MB），可能影响同步性能")
	}

	// 未解决冲突过多
	if result.OldConflicts.Count > 50 {
		warnings = append(warnings, "未解决的冲突文件超过 50 个，建议尽快处理")
	}

	return warnings
}

// ========== 清理辅助方法 ==========

// cleanExpiredFiles 清理目录下超过指定时间的文件
// 返回释放的空间大小和清理的文件数量
func (c *Cleaner) cleanExpiredFiles(dir string, maxAge time.Duration) (uint64, int) {
	var totalSize uint64
	var count int

	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, 0
	}

	cutoff := time.Now().Add(-maxAge)

	for _, entry := range entries {
		if entry.IsDir() {
			// 递归清理子目录中的过期文件
			subDir := filepath.Join(dir, entry.Name())
			subSize, subCount := c.cleanExpiredFiles(subDir, maxAge)
			totalSize += subSize
			count += subCount
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		if info.ModTime().Before(cutoff) {
			filePath := filepath.Join(dir, entry.Name())
			fileSz := uint64(info.Size())
			if err := os.Remove(filePath); err != nil {
				c.logs.Printf("删除文件失败 %s: %v", filePath, err)
				continue
			}
			totalSize += fileSz
			count++
		}
	}

	return totalSize, count
}

// cleanAllInDir 清理目录下所有文件（保留目录本身）
func (c *Cleaner) cleanAllInDir(dir string) (uint64, int) {
	var totalSize uint64
	var count int

	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, 0
	}

	for _, entry := range entries {
		fullPath := filepath.Join(dir, entry.Name())

		if entry.IsDir() {
			subSize, subCount := c.cleanAllInDir(fullPath)
			totalSize += subSize
			count += subCount
			// 删除空子目录
			os.Remove(fullPath)
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		fileSz := uint64(info.Size())
		if err := os.Remove(fullPath); err != nil {
			c.logs.Printf("删除文件失败 %s: %v", fullPath, err)
			continue
		}
		totalSize += fileSz
		count++
	}

	return totalSize, count
}

// cleanOrphanFiles 清理孤立文件
func (c *Cleaner) cleanOrphanFiles() (uint64, int) {
	var totalSize uint64
	var count int

	if c.db == nil {
		return 0, 0
	}

	keys, err := c.db.GetOrphanFileKeys()
	if err != nil {
		c.logs.Printf("查询孤立文件失败: %v", err)
		return 0, 0
	}

	for _, key := range keys {
		fullPath := filepath.Join(c.dataDir, "sync", key)
		info, err := os.Stat(fullPath)
		if err != nil {
			continue
		}

		if info.IsDir() {
			continue
		}

		fileSz := uint64(info.Size())
		if err := os.Remove(fullPath); err != nil {
			c.logs.Printf("删除孤立文件失败 %s: %v", fullPath, err)
			continue
		}
		totalSize += fileSz
		count++
	}

	return totalSize, count
}

// ========== 数据库扫描方法 ==========

// scanDBDeletedFiles 扫描数据库中已标记删除的文件记录
func (c *Cleaner) scanDBDeletedFiles() CleanItem {
	item := CleanItem{Risk: "low", CanClean: true}
	if c.db == nil {
		return item
	}
	keys, err := c.db.GetOrphanFileKeys()
	if err != nil {
		c.logs.Printf("查询已删除文件记录失败: %v", err)
		return item
	}
	item.Count = len(keys)
	// 每条记录约估算500字节
	item.Size = uint64(len(keys)) * 500
	return item
}

// scanDBOldSyncEvents 扫描数据库中超过指定天数的同步事件
func (c *Cleaner) scanDBOldSyncEvents(days int) CleanItem {
	item := CleanItem{Risk: "low", CanClean: true}
	if c.db == nil {
		return item
	}
	return item
}

// scanDBOldActivityLogs 扫描数据库中超过指定天数的活动日志
func (c *Cleaner) scanDBOldActivityLogs(days int) CleanItem {
	item := CleanItem{Risk: "low", CanClean: true}
	if c.db == nil {
		return item
	}
	return item
}

// scanDBResolvedConflicts 扫描数据库中已解决的冲突记录
func (c *Cleaner) scanDBResolvedConflicts() CleanItem {
	item := CleanItem{Risk: "low", CanClean: true}
	if c.db == nil {
		return item
	}
	paths, err := c.db.GetResolvedConflicts(time.Now())
	if err != nil {
		c.logs.Printf("查询已解决冲突记录失败: %v", err)
		return item
	}
	item.Count = len(paths)
	item.Size = uint64(len(paths)) * 400
	return item
}

// cleanEmptyDirs 递归清理空目录
// 返回清理的空目录数量
func (c *Cleaner) cleanEmptyDirs() int {
	count := 0

	// 自底向上清理空目录
	dirs := []string{
		filepath.Join(c.dataDir, "temp"),
		filepath.Join(c.dataDir, "logs"),
		filepath.Join(c.dataDir, "conflicts"),
		filepath.Join(c.dataDir, "trash"),
		filepath.Join(c.dataDir, "tombstones"),
	}

	for _, dir := range dirs {
		count += c.removeEmptyDirsRecursive(dir)
	}

	return count
}

// removeEmptyDirsRecursive 递归删除指定目录下的空目录
func (c *Cleaner) removeEmptyDirsRecursive(dir string) int {
	count := 0

	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}

	// 先递归处理子目录
	for _, entry := range entries {
		if entry.IsDir() {
			subDir := filepath.Join(dir, entry.Name())
			count += c.removeEmptyDirsRecursive(subDir)
		}
	}

	// 重新读取目录内容（子目录可能已被删除）
	entries, err = os.ReadDir(dir)
	if err != nil {
		return count
	}

	// 如果目录为空，删除它
	if len(entries) == 0 {
		if err := os.Remove(dir); err != nil {
			c.logs.Printf("删除空目录失败 %s: %v", dir, err)
		} else {
			count++
			c.logs.Printf("已清理空目录: %s", dir)
		}
	}

	return count
}

// ========== 工具函数 ==========

// contains 检查字符串切片中是否包含指定字符串
func contains(slice []string, target string) bool {
	for _, s := range slice {
		if s == target {
			return true
		}
	}
	return false
}
