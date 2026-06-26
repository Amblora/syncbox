package monitor

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
)

// Metrics 完整的系统资源指标
type Metrics struct {
	CPU     CPUMetrics     `json:"cpu"`
	Memory  MemoryMetrics  `json:"memory"`
	Swap    SwapMetrics    `json:"swap"`
	Disk    DiskMetrics    `json:"disk"`
	SyncBox SyncBoxMetrics `json:"syncbox"`
	Process ProcessMetrics `json:"process"`
}

// CPUMetrics CPU 使用指标
type CPUMetrics struct {
	UsagePercent float64 `json:"usage_percent"` // CPU 总使用率百分比
	Cores        int     `json:"cores"`         // CPU 核心数
	Load1        float64 `json:"load1"`         // 1 分钟负载
	Load5        float64 `json:"load5"`         // 5 分钟负载
	Load15       float64 `json:"load15"`        // 15 分钟负载
}

// MemoryMetrics 内存使用指标
type MemoryMetrics struct {
	Total       uint64  `json:"total"`        // 总内存（字节）
	Used        uint64  `json:"used"`         // 已使用内存（字节）
	Available   uint64  `json:"available"`    // 可用内存（字节）
	UsedPercent float64 `json:"used_percent"` // 内存使用率百分比
}

// SwapMetrics 交换分区指标
type SwapMetrics struct {
	Total       uint64  `json:"total"`        // 总交换空间（字节）
	Used        uint64  `json:"used"`         // 已使用交换空间（字节）
	Free        uint64  `json:"free"`         // 可用交换空间（字节）
	UsedPercent float64 `json:"used_percent"` // 交换空间使用率百分比
}

// DiskMetrics 磁盘使用指标
type DiskMetrics struct {
	Total       uint64  `json:"total"`        // 总磁盘空间（字节）
	Used        uint64  `json:"used"`         // 已使用磁盘空间（字节）
	Free        uint64  `json:"free"`         // 可用磁盘空间（字节）
	UsedPercent float64 `json:"used_percent"` // 磁盘使用率百分比
}

// SyncBoxMetrics SyncBox 应用各目录占用指标
type SyncBoxMetrics struct {
	SyncDirSize   uint64 `json:"sync_dir_size"`   // 同步目录总大小（字节）
	ConflictsSize uint64 `json:"conflicts_size"`   // 冲突文件大小（字节）
	TempSize      uint64 `json:"temp_size"`        // 临时文件大小（字节）
	LogsSize      uint64 `json:"logs_size"`        // 日志文件大小（字节）
	TrashSize     uint64 `json:"trash_size"`       // 回收站大小（字节）
	DBSize        uint64 `json:"db_size"`          // 数据库文件大小（字节）
}

// ProcessMetrics 进程运行指标
type ProcessMetrics struct {
	Uptime     string `json:"uptime"`      // 进程运行时长（可读格式）
	Goroutines int    `json:"goroutines"`  // 当前 goroutine 数量
	MemAlloc   uint64 `json:"mem_alloc"`   // Go 堆已分配内存（字节）
	MemSys     uint64 `json:"mem_sys"`     // Go 运行时从系统获取的内存（字节）
	NumGC      uint32 `json:"num_gc"`      // GC 次数
}

// DiskWarning 磁盘保护警告
type DiskWarning struct {
	Level   string `json:"level"`   // 警告级别: info, warn, critical
	Message string `json:"message"` // 警告描述
	Action  string `json:"action"`  // 建议的操作
}

// Collector 资源指标收集器
type Collector struct {
	dataDir   string       // SyncBox 数据目录
	startTime time.Time    // 进程启动时间
	mu        sync.RWMutex // 读写锁，保护并发访问
}

// NewCollector 创建一个新的资源指标收集器
// dataDir: SyncBox 数据目录路径
func NewCollector(dataDir string) *Collector {
	return &Collector{
		dataDir:   dataDir,
		startTime: time.Now(),
	}
}

// Collect 收集所有系统资源指标
// 返回完整的 Metrics 结构体和可能的错误
func (c *Collector) Collect() (*Metrics, error) {
	m := &Metrics{}

	// 并发收集各子系统指标
	var wg sync.WaitGroup
	var cpuErr, memErr, swapErr, diskErr error

	wg.Add(4)

	// 收集 CPU 指标
	go func() {
		defer wg.Done()
		m.CPU, cpuErr = c.collectCPU()
	}()

	// 收集内存指标
	go func() {
		defer wg.Done()
		m.Memory, memErr = c.collectMemory()
	}()

	// 收集 Swap 指标
	go func() {
		defer wg.Done()
		m.Swap, swapErr = c.collectSwap()
	}()

	// 收集磁盘指标
	go func() {
		defer wg.Done()
		m.Disk, diskErr = c.collectDisk()
	}()

	// 同步收集 SyncBox 和进程指标（不依赖外部 IO）
	m.SyncBox = c.collectSyncBox()
	m.Process = c.collectProcess()

	wg.Wait()

	// 返回第一个遇到的错误
	if cpuErr != nil {
		return m, cpuErr
	}
	if memErr != nil {
		return m, memErr
	}
	if swapErr != nil {
		return m, swapErr
	}
	if diskErr != nil {
		return m, diskErr
	}

	return m, nil
}

// collectCPU 收集 CPU 使用率和负载信息
func (c *Collector) collectCPU() (CPUMetrics, error) {
	m := CPUMetrics{}

	// 获取 CPU 使用率（阻塞 1 秒采样）
	percents, err := cpu.Percent(time.Second, false)
	if err != nil {
		return m, err
	}
	if len(percents) > 0 {
		m.UsagePercent = percents[0]
	}

	// 获取 CPU 核心数
	cores, err := cpu.Counts(true)
	if err == nil {
		m.Cores = cores
	}

	// 获取系统负载（1/5/15 分钟）
	avg, err := load.Avg()
	if err == nil {
		m.Load1 = avg.Load1
		m.Load5 = avg.Load5
		m.Load15 = avg.Load15
	}

	return m, nil
}

// collectMemory 收集内存使用信息
func (c *Collector) collectMemory() (MemoryMetrics, error) {
	m := MemoryMetrics{}

	vmem, err := mem.VirtualMemory()
	if err != nil {
		return m, err
	}

	m.Total = vmem.Total
	m.Used = vmem.Used
	m.Available = vmem.Available
	m.UsedPercent = vmem.UsedPercent

	return m, nil
}

// collectSwap 收集交换分区信息
func (c *Collector) collectSwap() (SwapMetrics, error) {
	m := SwapMetrics{}

	swap, err := mem.SwapMemory()
	if err != nil {
		return m, err
	}

	m.Total = swap.Total
	m.Used = swap.Used
	m.Free = swap.Free
	m.UsedPercent = swap.UsedPercent

	return m, nil
}

// collectDisk 收集磁盘使用信息（以 SyncBox 数据目录所在分区为准）
func (c *Collector) collectDisk() (DiskMetrics, error) {
	m := DiskMetrics{}

	usage, err := disk.Usage(c.dataDir)
	if err != nil {
		return m, err
	}

	m.Total = usage.Total
	m.Used = usage.Used
	m.Free = usage.Free
	m.UsedPercent = usage.UsedPercent

	return m, nil
}

// collectSyncBox 收集 SyncBox 各子目录的占用大小
func (c *Collector) collectSyncBox() SyncBoxMetrics {
	return SyncBoxMetrics{
		SyncDirSize:   dirSize(filepath.Join(c.dataDir, "sync")),
		ConflictsSize: dirSize(filepath.Join(c.dataDir, "conflicts")),
		TempSize:      dirSize(filepath.Join(c.dataDir, "temp")),
		LogsSize:      dirSize(filepath.Join(c.dataDir, "logs")),
		TrashSize:     dirSize(filepath.Join(c.dataDir, "trash")),
		DBSize:        fileSize(filepath.Join(c.dataDir, "syncbox.db")),
	}
}

// collectProcess 收集 Go 进程运行指标
func (c *Collector) collectProcess() ProcessMetrics {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	uptime := time.Since(c.startTime)

	return ProcessMetrics{
		Uptime:     formatDuration(uptime),
		Goroutines: runtime.NumGoroutine(),
		MemAlloc:   memStats.Alloc,
		MemSys:     memStats.Sys,
		NumGC:      memStats.NumGC,
	}
}

// CheckDiskWarnings 检查磁盘保护逻辑，返回警告列表
// 根据剩余空间大小判断：
//   - 剩余 < 5GB: 黄色警告（建议清理）
//   - 剩余 < 2GB: 禁止大文件上传
//   - 剩余 < 1GB: 暂停所有上传
func (c *Collector) CheckDiskWarnings() []DiskWarning {
	var warnings []DiskWarning

	usage, err := disk.Usage(c.dataDir)
	if err != nil {
		return warnings
	}

	freeGB := float64(usage.Free) / (1024 * 1024 * 1024)

	// 剩余空间 < 1GB: 严重警告，暂停所有上传
	if freeGB < 1.0 {
		warnings = append(warnings, DiskWarning{
			Level:   "critical",
			Message: "磁盘剩余空间不足 1GB，已暂停所有上传操作",
			Action:  "pause_all_upload",
		})
	} else if freeGB < 2.0 {
		// 剩余空间 < 2GB: 高级警告，禁止大文件上传
		warnings = append(warnings, DiskWarning{
			Level:   "high",
			Message: "磁盘剩余空间不足 2GB，已禁止大文件上传",
			Action:  "block_large_upload",
		})
	} else if freeGB < 5.0 {
		// 剩余空间 < 5GB: 黄色警告
		warnings = append(warnings, DiskWarning{
			Level:   "warn",
			Message: "磁盘剩余空间不足 5GB，建议尽快清理",
			Action:  "suggest_cleanup",
		})
	}

	return warnings
}

// dirSize 递归计算目录大小（字节）
// 遍历目录下所有文件，累加每个文件的实际大小
func dirSize(path string) uint64 {
	var size uint64

	entries, err := os.ReadDir(path)
	if err != nil {
		// 目录不存在或无法读取，返回 0
		return 0
	}

	for _, entry := range entries {
		fullPath := filepath.Join(path, entry.Name())
		if entry.IsDir() {
			size += dirSize(fullPath)
		} else {
			info, err := entry.Info()
			if err == nil {
				size += uint64(info.Size())
			}
		}
	}

	return size
}

// fileSize 获取单个文件大小（字节）
// 如果文件不存在或无法访问，返回 0
func fileSize(path string) uint64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return uint64(info.Size())
}

// formatDuration 将时长格式化为可读字符串
// 例如: "2h30m15s", "5m0s", "30s"
func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	if h > 0 {
		return fmt.Sprintf("%dh%dm%ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm%ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}
