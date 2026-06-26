package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"runtime"

	"github.com/aync/syncbox/internal/auth"
	"github.com/aync/syncbox/internal/cleanup"
	"github.com/aync/syncbox/internal/db"
	"github.com/aync/syncbox/internal/monitor"
	"github.com/aync/syncbox/internal/ws"
	"github.com/gin-gonic/gin"
)

// API 服务端?API 澶勭悊鍣?
type API struct {
	db      *db.DB
	hub     *ws.Hub
	monitor *monitor.Collector
	cleaner *cleanup.Cleaner
	dataDir string
}

// NewAPI 创建 API 瀹炰緥
func NewAPI(database *db.DB, hub *ws.Hub, dataDir string) *API {
	return &API{
		db:      database,
		hub:     hub,
		monitor: monitor.NewCollector(dataDir),
		cleaner: cleanup.NewCleaner(dataDir, database),
		dataDir: dataDir,
	}
}

// broadcastFileChange 骞挎挱文件鍙樻洿娑堟伅
func (a *API) broadcastFileChange(eventType, relPath string, size int64, hash string, version int) {
	a.hub.BroadcastJSON(ws.Message{
		Type: eventType,
		Data: gin.H{"rel_path": relPath, "size": size, "hash": hash, "version": version},
	})
}

// broadcastToOthers 骞挎挱缁欓櫎鍙戦€佽€呭鐨勫鎴风
func (a *API) broadcastToOthers(senderDeviceID, eventType, relPath string, size int64, hash string, version int) {
	a.hub.BroadcastToOthers(senderDeviceID, ws.Message{
		Type: eventType,
		Data: gin.H{"rel_path": relPath, "size": size, "hash": hash, "version": version},
	})
}

// SetupRoutes 设置鎵€鏈夎矾鐢?
func (a *API) SetupRoutes(r *gin.Engine) {
	r.GET("/health", func(c *gin.Context) { c.JSON(200, gin.H{"status": "up"}) })
	r.GET("/favicon.ico", func(c *gin.Context) { c.Status(204) })
	r.POST("/api/client/register", a.handleClientRegister)
	r.POST("/api/admin/login", a.handleLogin)

	admin := r.Group("/api/admin")
	admin.Use(auth.AuthMiddleware(a.db))
	{
		admin.GET("/overview", a.handleOverview)
		admin.GET("/metrics", a.handleMetrics)
		admin.GET("/metrics/stream", a.handleMetricsStream)
		admin.GET("/files", a.handleListFiles)
		admin.GET("/files/list", a.handleFileList)
		admin.POST("/files/upload", a.handleFileUpload)
		admin.GET("/files/download", a.handleFileDownload)
		admin.POST("/files/delete", a.handleFileDelete)
		admin.POST("/files/rename", a.handleFileRename)
		admin.GET("/devices", a.handleListDevices)
		admin.POST("/devices", a.handleCreateDevice)
		admin.POST("/devices/revoke", a.handleRevokeDevice)
		admin.POST("/devices/delete", a.handleDeleteDevice)
		admin.GET("/conflicts", a.handleListConflicts)
		admin.POST("/conflicts/resolve", a.handleResolveConflict)
		admin.GET("/cleanup/scan", a.handleCleanupScan)
		admin.POST("/cleanup/safe", a.handleSafeCleanup)
		admin.POST("/cleanup/deep", a.handleDeepCleanup)
		admin.GET("/logs", a.handleListLogs)
		admin.GET("/settings", a.handleGetSettings)
		admin.POST("/settings", a.handleSetSettings)
		admin.POST("/change-password", a.handleChangePassword)
		admin.POST("/change-username", a.handleChangeUsername)
		admin.GET("/tokens", a.handleListTokens)
		admin.POST("/tokens/create", a.handleCreateToken)
		admin.POST("/tokens/delete", a.handleDeleteToken)
		admin.POST("/logs/delete", a.handleDeleteLog)
		admin.POST("/logs/clear", a.handleClearLogs)
		admin.POST("/files/move", a.handleFileMove)
		admin.POST("/cleanup/system-scan", a.handleSystemScan)
		admin.POST("/cleanup/system-clean", a.handleSystemClean)
		admin.GET("/trash", a.handleListTrash)
		admin.POST("/trash/restore", a.handleRestoreTrash)
		admin.POST("/trash/delete", a.handleDeleteTrash)
		admin.POST("/trash/restore-all", a.handleRestoreAllTrash)
		admin.POST("/trash/clear", a.handleClearTrash)
		admin.POST("/files/mkdir", a.handleMkdir)
		admin.POST("/metrics/gc", a.handleGC)
		admin.POST("/avatar", a.handleSetAvatar)
		admin.GET("/avatar", a.handleGetAvatar)
	}

	client := r.Group("/api/client")
	client.Use(auth.DeviceAuthMiddleware(a.db))
	{
		client.POST("/hello", a.handleClientHello)
		client.GET("/events", a.handleClientEvents)
		client.PUT("/upload", a.handleClientUpload)
		client.GET("/download", a.handleClientDownload)
		client.POST("/delete", a.handleClientDelete)
		client.POST("/rename", a.handleClientRename)
		client.GET("/sync_dirs", a.handleClientSyncDirs)
		client.GET("/files", a.handleClientFileList)
	}
	r.GET("/api/client/ws", a.handleClientWS)
}

func (a *API) handleLogin(c *gin.Context) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "请求格式错误"})
		return
	}
	if !a.db.LoginAdmin(req.Username, req.Password) {
		c.JSON(401, gin.H{"error": "用户名或密码错误"})
		return
	}
	token, err := auth.GenerateJWT(a.db, req.Username)
	if err != nil {
		c.JSON(500, gin.H{"error": "生成令牌失败"})
		return
	}
	c.JSON(200, gin.H{"token": token})
}

func (a *API) handleOverview(c *gin.Context) {
	stats, _ := a.db.GetStats()
	m, _ := a.monitor.Collect()
	devices, _ := a.db.ListDevices()
	onlineCount := 0
	for _, d := range devices {
		if !d.Revoked && time.Now().UnixMilli()-d.LastSeen < 300000 {
			onlineCount++
		}
	}
	conflicts, _ := a.db.ListConflicts(true)
	pendingConflicts := 0
	for _, cf := range conflicts {
		if !cf.Resolved {
			pendingConflicts++
		}
	}
	var diskFree uint64
	var diskPercent float64
	if m != nil {
		diskFree = m.Disk.Free
		diskPercent = m.Disk.UsedPercent
	}
	warnings := []string{}
	if diskFree < 5*1024*1024*1024 {
		warnings = append(warnings, "磁盘剩余空间不足 5GB")
	}
	if diskFree < 2*1024*1024*1024 {
		warnings = append(warnings, "磁盘空间涓ラ噸涓嶈冻锛屽ぇ文件上传宸茶绂佹")
	}
	if diskFree < 1*1024*1024*1024 {
		warnings = append(warnings, "磁盘空间极低，所有上传已暂停")
	}
	var totalFiles, deletedFiles, todayEvents int64
	if stats != nil {
		totalFiles = stats.TotalFiles
		deletedFiles = stats.DeletedFiles
		todayEvents = stats.TodayEvents
	}
	// 计算同步目录占用空间
	syncDirSize := int64(0)
	syncDir := filepath.Join(a.dataDir, "sync")
	filepath.Walk(syncDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			syncDirSize += info.Size()
		}
		return nil
	})

	c.JSON(200, gin.H{
		"online_devices": onlineCount, "total_devices": len(devices),
		"total_files": totalFiles, "deleted_files": deletedFiles,
		"today_events": todayEvents, "pending_conflicts": pendingConflicts,
		"disk_free": diskFree, "disk_percent": diskPercent, "warnings": warnings,
		"sync_dir_size": syncDirSize,
	})
}

func (a *API) handleMetrics(c *gin.Context) {
	m, err := a.monitor.Collect()
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, m)
}

func (a *API) handleMetricsStream(c *gin.Context) {
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(500, gin.H{"error": "streaming unsupported"})
		return
	}
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.Request.Context().Done():
			return
		case <-ticker.C:
			m, err := a.monitor.Collect()
			if err != nil {
				continue
			}
			data, _ := json.Marshal(m)
			c.Writer.Write([]byte("data: " + string(data) + "\n\n"))
			flusher.Flush()
		}
	}
}

func (a *API) handleListFiles(c *gin.Context) {
	files, _ := a.db.ListFiles(false)
	c.JSON(200, files)
}

func (a *API) handleFileUpload(c *gin.Context) {
	m, _ := a.monitor.Collect()
	if m != nil && m.Disk.Free < 1*1024*1024*1024 {
		c.JSON(503, gin.H{"error": "磁盘空间不足，上传已暂停"})
		return
	}
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(400, gin.H{"error": "获取上传文件失败: " + err.Error()})
		return
	}
	defer file.Close()
	relPath := c.PostForm("path")
	if relPath == "" || relPath == "/" {
		relPath = header.Filename
	} else {
		// 如果 path 是目录路径，则拼接文件名
		if strings.HasSuffix(relPath, "/") || relPath == "." {
			relPath = filepath.Join(relPath, header.Filename)
		}
	}
	// 清理路径
	relPath = strings.TrimPrefix(relPath, "/")
	tmpPath := filepath.Join(a.dataDir, "temp", fmt.Sprintf("%d_%s", time.Now().UnixNano(), filepath.Base(relPath)))
	os.MkdirAll(filepath.Dir(tmpPath), 0755)
	dst, _ := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	hasher := sha256.New()
	writer := io.MultiWriter(dst, hasher)
	written, _ := io.Copy(writer, file)
	dst.Close()
	hash := hex.EncodeToString(hasher.Sum(nil))
	syncPath := filepath.Join(a.dataDir, "sync", relPath)
	os.MkdirAll(filepath.Dir(syncPath), 0755)
	os.Rename(tmpPath, syncPath)
	pathKey := strings.ToLower(filepath.ToSlash(relPath))
	f := &db.File{
		ID: db.GenerateDeviceID(), RelPath: relPath, PathKey: pathKey, Type: "file",
		Size: written, Hash: hash, MtimeNs: time.Now().UnixNano(),
	}
	a.db.UpsertFile(f)
	a.db.AddEvent("update", relPath, "", 1, "admin")
	a.db.AddActivityLog("admin", "upload", relPath, fmt.Sprintf("上传文件 %s (%d bytes)", relPath, written))
	a.broadcastFileChange("file_change", relPath, written, hash, 0)
	c.JSON(200, gin.H{"ok": true, "path": relPath, "size": written, "hash": hash})
}

func (a *API) handleFileDownload(c *gin.Context) {
	relPath := c.Query("path")
	if relPath == "" {
		c.JSON(400, gin.H{"error": "缺少 path 参数"})
		return
	}
	filePath := filepath.Join(a.dataDir, "sync", relPath)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		c.JSON(404, gin.H{"error": "文件不存在"})
		return
	}
	c.File(filePath)
}

func (a *API) handleFileDelete(c *gin.Context) {
	var req struct {
		Path string `json:"path"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "请求格式错误"})
		return
	}
	syncDir := filepath.Join(a.dataDir, "sync")
	srcPath := filepath.Join(syncDir, req.Path)
	trashDir := filepath.Join(a.dataDir, "trash")
	trashName := fmt.Sprintf("%d_%s", time.Now().UnixNano(), filepath.Base(req.Path))
	trashPath := filepath.Join(trashDir, trashName)

	info, err := os.Stat(srcPath)
	if err != nil {
		c.JSON(404, gin.H{"error": "文件不存在"})
		return
	}

	if info.IsDir() {
		// 目录：递归复制到回收站，然后删除源
		filepath.Walk(srcPath, func(walkPath string, fi os.FileInfo, walkErr error) error {
			if walkErr != nil {
				return nil
			}
			relPath, _ := filepath.Rel(srcPath, walkPath)
			dest := filepath.Join(trashPath, relPath)
			if fi.IsDir() {
				os.MkdirAll(dest, 0755)
				return nil
			}
			os.MkdirAll(filepath.Dir(dest), 0755)
			os.Rename(walkPath, dest)
			return nil
		})
		os.RemoveAll(srcPath)
		a.db.DeleteFilesByPrefix(strings.ToLower(filepath.ToSlash(req.Path)))
	} else {
		// 单文件：移动到回收站
		os.MkdirAll(filepath.Dir(trashPath), 0755)
		os.Rename(srcPath, trashPath)
		pathKey := strings.ToLower(filepath.ToSlash(req.Path))
		a.db.DeleteFile(pathKey)
	}

	a.db.AddEvent("delete", req.Path, "", 0, "admin")
	a.db.AddActivityLog("admin", "delete", req.Path, fmt.Sprintf("删除: %s", req.Path))
	a.broadcastFileChange("file_delete", req.Path, 0, "", 0)
	c.JSON(200, gin.H{"ok": true})
}

func (a *API) handleFileRename(c *gin.Context) {
	var req struct {
		OldPath string `json:"old_path"`
		NewPath string `json:"new_path"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "请求格式错误"})
		return
	}
	oldFullPath := filepath.Join(a.dataDir, "sync", req.OldPath)
	newFullPath := filepath.Join(a.dataDir, "sync", req.NewPath)
	os.MkdirAll(filepath.Dir(newFullPath), 0755)
	os.Rename(oldFullPath, newFullPath)
	oldKey := strings.ToLower(filepath.ToSlash(req.OldPath))
	newKey := strings.ToLower(filepath.ToSlash(req.NewPath))
	a.db.RenameFile(oldKey, req.NewPath, newKey)
	a.db.AddEvent("rename", req.NewPath, req.OldPath, 0, "admin")
	a.db.AddActivityLog("admin", "rename", req.OldPath, fmt.Sprintf("閲嶅懡鍚?%s -> %s", req.OldPath, req.NewPath))
	a.broadcastFileChange("file_rename", req.NewPath, 0, "", 0)
	c.JSON(200, gin.H{"ok": true})
}

func (a *API) handleListDevices(c *gin.Context) {
	devices, err := a.db.ListDevices()
	if err != nil {
		fmt.Printf("[设备] ListDevices错误: %v\n", err)
	}
	if devices == nil {
		devices = []*db.Device{}
	}
	result := make([]gin.H, 0)
	now := time.Now().UnixMilli()
	for _, d := range devices {
		online := !d.Revoked && now-d.LastSeen < 300000 /* 5分钟内有心跳算在线 */
		result = append(result, gin.H{
			"id": d.ID, "name": d.Name, "platform": d.Platform, "ip": d.IP,
			"last_seen": d.LastSeen, "online": online, "revoked": d.Revoked, "created_at": d.CreatedAt,
		})
	}
	c.JSON(200, gin.H{"devices": result})
}

func (a *API) handleCreateDevice(c *gin.Context) {
	var req struct {
		Name     string `json:"name"`
		Platform string `json:"platform"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "请求格式错误"})
		return
	}
	token := db.GenerateToken()
	d, err := a.db.CreateDevice(req.Name, token, req.Platform)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	a.db.AddActivityLog("admin", "register", "", fmt.Sprintf("注册设备: %s", req.Name))
	c.JSON(200, gin.H{"id": d.ID, "token": token, "name": d.Name})
}

func (a *API) handleRevokeDevice(c *gin.Context) {
	var req struct {
		ID string `json:"id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "请求格式错误"})
		return
	}
	a.db.RevokeDevice(req.ID)
	a.db.AddActivityLog("admin", "revoke", "", fmt.Sprintf("撤销设备: %s", req.ID))
	c.JSON(200, gin.H{"ok": true})
}

// handleDeleteDevice 从数据库彻底删除设备
func (a *API) handleDeleteDevice(c *gin.Context) {
	var req struct {
		ID string `json:"id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "请求格式错误"})
		return
	}
	if err := a.db.DeleteDevice(req.ID); err != nil {
		c.JSON(500, gin.H{"error": "删除失败: " + err.Error()})
		return
	}
	a.db.AddActivityLog("admin", "delete_device", req.ID, "删除设备")
	c.JSON(200, gin.H{"ok": true})
}

func (a *API) handleListConflicts(c *gin.Context) {
	conflicts, _ := a.db.ListConflicts(true)
	if conflicts == nil {
		conflicts = make([]*db.Conflict, 0)
	}
	c.JSON(200, gin.H{"conflicts": conflicts})
}

func (a *API) handleResolveConflict(c *gin.Context) {
	var req struct {
		ID     int64  `json:"id"`
		Action string `json:"action"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "请求格式错误"})
		return
	}
	a.db.ResolveConflict(req.ID)
	a.db.AddActivityLog("admin", "resolve_conflict", "", fmt.Sprintf("解决冲突 #%d: %s", req.ID, req.Action))
	c.JSON(200, gin.H{"ok": true})
}

func (a *API) handleCleanupScan(c *gin.Context) {
	result, err := a.cleaner.Scan()
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, result)
}

func (a *API) handleSafeCleanup(c *gin.Context) {
	freed, err := a.cleaner.SafeClean()
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	a.db.AddActivityLog("admin", "safe_cleanup", "", fmt.Sprintf("瀹夊叏娓呯悊锛岄噴鏀?%d 瀛楄妭", freed))
	c.JSON(200, gin.H{"freed": freed})
}

func (a *API) handleDeepCleanup(c *gin.Context) {
	var req struct {
		Targets []string `json:"targets"`
		Confirm string   `json:"confirm"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "请求格式错误"})
		return
	}
	if req.Confirm != "DELETE" {
		c.JSON(400, gin.H{"error": "深度清理需要佺‘璁わ細璇峰湪 confirm 字段输入 DELETE"})
		return
	}
	freed, err := a.cleaner.DeepClean(req.Targets)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	a.db.AddActivityLog("admin", "deep_cleanup", "", fmt.Sprintf("深度清理锛岄噴鏀?%d 瀛楄妭", freed))
	c.JSON(200, gin.H{"freed": freed})
}

func (a *API) handleListLogs(c *gin.Context) {
	limit := 200
	if l := c.Query("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil {
			limit = v
		}
	}
	logs, _ := a.db.ListActivityLogs(limit)
	c.JSON(200, logs)
}

func (a *API) handleGetSettings(c *gin.Context) {
	keys := []string{"sync_root", "scan_interval_seconds", "keep_deleted_days", "keep_conflict_days", "keep_log_days", "max_upload_concurrency"}
	settings := make(map[string]string)
	for _, k := range keys {
		v, _ := a.db.GetSetting(k)
		settings[k] = v
	}
	c.JSON(200, settings)
}

func (a *API) handleSetSettings(c *gin.Context) {
	var req map[string]string
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "请求格式错误"})
		return
	}
	for k, v := range req {
		a.db.SetSetting(k, v)
	}
	c.JSON(200, gin.H{"ok": true})
}

func (a *API) handleClientHello(c *gin.Context) {
	deviceID := c.GetString("device_id")
	ip := c.ClientIP()
	a.db.UpdateDeviceLastSeen(deviceID, 0)
	a.db.UpdateDeviceIP(deviceID, ip)
	c.JSON(200, gin.H{"ok": true, "device_id": deviceID})
}

func (a *API) handleClientEvents(c *gin.Context) {
	afterStr := c.DefaultQuery("after_seq", "0")
	afterSeq, _ := strconv.ParseInt(afterStr, 10, 64)
	events, _ := a.db.GetEventsAfterSeq(afterSeq)
	c.JSON(200, events)
}

func (a *API) handleClientUpload(c *gin.Context) {
	// ========== Seafile风格同步逻辑 ==========
	// 服务端是权威存储后端，始终接受上传
	// 冲突仅在真正的并发修改时才产生（通过WebSocket通知机制避免）
	deviceID := c.GetString("device_id")
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(400, gin.H{"error": "获取文件失败"})
		return
	}
	defer file.Close()

	// 解析文件路径
	relPath := c.PostForm("path")
	if relPath == "" || relPath == "/" {
		relPath = header.Filename
	} else {
		if strings.HasSuffix(relPath, "/") || relPath == "." {
			relPath = filepath.Join(relPath, header.Filename)
		}
	}
	relPath = strings.TrimPrefix(relPath, "/")

	// 写入临时文件并计算哈希
	tmpPath := filepath.Join(a.dataDir, "temp", fmt.Sprintf("%d_%s", time.Now().UnixNano(), filepath.Base(relPath)))
	os.MkdirAll(filepath.Dir(tmpPath), 0755)
	dst, _ := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	hasher := sha256.New()
	writer := io.MultiWriter(dst, hasher)
	written, _ := io.Copy(writer, file)
	dst.Close()
	hash := hex.EncodeToString(hasher.Sum(nil))

	pathKey := strings.ToLower(filepath.ToSlash(relPath))
	existing, _ := a.db.GetFile(pathKey)

	// 步骤1: 去重 — 如果哈希相同，说明内容没变，直接返回成功
	if existing != nil && existing.Hash == hash {
		os.Remove(tmpPath)
		c.JSON(200, gin.H{"ok": true, "version": existing.Version, "hash": existing.Hash, "unchanged": true})
		return
	}

	// 步骤2: 接受上传 — 服务端始终接受，递增版本号
	// 这是Seafile的核心理念：服务端是权威存储，不做版本冲突拦截
	syncPath := filepath.Join(a.dataDir, "sync", relPath)
	os.MkdirAll(filepath.Dir(syncPath), 0755)
	os.Rename(tmpPath, syncPath)

	var newVersion int64 = 1
	if existing != nil {
		newVersion = existing.Version + 1
	}

	f := &db.File{
		RelPath: relPath, PathKey: pathKey, Type: "file",
		Version: newVersion, Size: written, Hash: hash, MtimeNs: time.Now().UnixNano(),
	}
	a.db.UpsertFile(f)
	a.db.AddEvent("update", relPath, "", newVersion, deviceID)
	a.db.AddActivityLog(deviceID, "upload", relPath, fmt.Sprintf("设备 %s 上传 %s (v%d)", deviceID, relPath, newVersion))

	// 步骤3: 通知其他客户端下载新版本
	a.broadcastToOthers(deviceID, "file_change", relPath, written, hash, int(newVersion))
	c.JSON(200, gin.H{"ok": true, "version": newVersion, "hash": hash})
}

func (a *API) handleClientDownload(c *gin.Context) {
	relPath := c.Query("path")
	if relPath == "" {
		c.JSON(400, gin.H{"error": "缺少 path 参数"})
		return
	}
	filePath := filepath.Join(a.dataDir, "sync", relPath)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		c.JSON(404, gin.H{"error": "文件不存在"})
		return
	}
	// 在响应头中附带文件的 hash 和 version，方便客户端验证和更新状态
	pathKey := strings.ToLower(filepath.ToSlash(relPath))
	dbFile, _ := a.db.GetFile(pathKey)
	if dbFile != nil {
		c.Header("X-File-Hash", dbFile.Hash)
		c.Header("X-File-Version", fmt.Sprintf("%d", dbFile.Version))
	}
	c.File(filePath)
}

func (a *API) handleClientDelete(c *gin.Context) {
	deviceID := c.GetString("device_id")
	var req struct {
		Path string `json:"path"`
	}
	c.ShouldBindJSON(&req)
	srcPath := filepath.Join(a.dataDir, "sync", req.Path)

	info, err := os.Stat(srcPath)
	if err != nil {
		c.JSON(404, gin.H{"error": "文件不存在"})
		return
	}

	if info.IsDir() {
		// 目录删除：递归移动到回收站，删除数据库中所有子文件记录
		trashDir := filepath.Join(a.dataDir, "trash")
		trashName := fmt.Sprintf("%d_%s", time.Now().UnixNano(), filepath.Base(req.Path))
		trashPath := filepath.Join(trashDir, trashName)
		filepath.Walk(srcPath, func(walkPath string, fi os.FileInfo, walkErr error) error {
			if walkErr != nil { return nil }
			relSub, _ := filepath.Rel(srcPath, walkPath)
			dest := filepath.Join(trashPath, relSub)
			if fi.IsDir() { os.MkdirAll(dest, 0755); return nil }
			os.MkdirAll(filepath.Dir(dest), 0755)
			os.Rename(walkPath, dest)
			return nil
		})
		os.RemoveAll(srcPath)
		a.db.DeleteFilesByPrefix(strings.ToLower(filepath.ToSlash(req.Path)))
	} else {
		// 单文件删除
		trashPath := filepath.Join(a.dataDir, "trash", fmt.Sprintf("%d_%s", time.Now().UnixNano(), filepath.Base(req.Path)))
		os.MkdirAll(filepath.Dir(trashPath), 0755)
		os.Rename(srcPath, trashPath)
		a.db.DeleteFile(strings.ToLower(filepath.ToSlash(req.Path)))
	}

	a.db.AddEvent("delete", req.Path, "", 0, deviceID)
	a.db.AddActivityLog(deviceID, "delete", req.Path, fmt.Sprintf("设备 %s 删除 %s", deviceID, req.Path))
	a.broadcastToOthers(deviceID, "file_delete", req.Path, 0, "", 0)
	c.JSON(200, gin.H{"ok": true})
}

func (a *API) handleClientRename(c *gin.Context) {
	deviceID := c.GetString("device_id")
	var req struct {
		OldPath string `json:"old_path"`
		NewPath string `json:"new_path"`
	}
	c.ShouldBindJSON(&req)
	oldFullPath := filepath.Join(a.dataDir, "sync", req.OldPath)
	newFullPath := filepath.Join(a.dataDir, "sync", req.NewPath)
	os.MkdirAll(filepath.Dir(newFullPath), 0755)
	os.Rename(oldFullPath, newFullPath)
	oldKey := strings.ToLower(filepath.ToSlash(req.OldPath))
	newKey := strings.ToLower(filepath.ToSlash(req.NewPath))
	a.db.RenameFile(oldKey, req.NewPath, newKey)
	a.db.AddEvent("rename", req.NewPath, req.OldPath, 0, deviceID)
	a.db.AddActivityLog(deviceID, "rename", req.OldPath, fmt.Sprintf("设备 %s 閲嶅懡鍚?%s -> %s", deviceID, req.OldPath, req.NewPath))
	a.broadcastToOthers(deviceID, "file_rename", req.NewPath, 0, "", 0)
	c.JSON(200, gin.H{"ok": true})
}

func (a *API) handleClientWS(c *gin.Context) {
	token := c.Query("token")
	if token == "" {
		token = strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer ")
	}
	if token == "" {
		c.JSON(401, gin.H{"error": "缺少认证token"})
		return
	}
	var deviceID string
	// 检查是否是 DeviceID:Token 格式
	if parts := strings.SplitN(token, ":", 2); len(parts) == 2 {
		if a.db.VerifyDeviceToken(parts[0], parts[1]) {
			deviceID = parts[0]
		}
	}
	// 如果上面没匹配，尝试用原始token遍历设备
	if deviceID == "" {
		devices, _ := a.db.ListDevices()
		tokenHash := hashToken(token)
		for _, d := range devices {
			if d.TokenHash == tokenHash && !d.Revoked {
				deviceID = d.ID
				break
			}
		}
	}
	if deviceID == "" {
		c.JSON(401, gin.H{"error": "无效的token"})
		return
	}
	ip := c.ClientIP()
	a.db.UpdateDeviceLastSeen(deviceID, 0)
	a.db.UpdateDeviceIP(deviceID, ip)
	a.hub.HandleWebSocket(c, deviceID)
}


// handleClientFileList 返回同步目录中的所有文件列表（供客户端全量扫描使用）
func (a *API) handleClientFileList(c *gin.Context) {
	// Seafile风格：从数据库获取文件列表（秒级响应，不再遍历磁盘算hash）
	dbFiles, err := a.db.ListFiles(false)
	if err != nil {
		c.JSON(500, gin.H{"error": "获取文件列表失败"})
		return
	}
	var files []map[string]interface{}
	for _, f := range dbFiles {
		if f.Deleted {
			continue
		}
		files = append(files, map[string]interface{}{
			"rel_path": f.RelPath,
			"hash":     f.Hash,
			"size":     f.Size,
			"version":  f.Version,
		})
	}
	if files == nil {
		files = []map[string]interface{}{}
	}
	c.JSON(200, files)
}

// computeFileHash 计算文件SHA256哈希
func computeFileHash(path string) (string, error) {
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

// handleFileList 处理文件列表请求，支持目录浏览
func (a *API) handleFileList(c *gin.Context) {
	reqPath := c.DefaultQuery("path", "/")
	// 标准化路径
	reqPath = strings.TrimPrefix(reqPath, "/")
	reqPath = strings.TrimSuffix(reqPath, "/")

	syncDir := filepath.Join(a.dataDir, "sync")
	targetDir := filepath.Join(syncDir, reqPath)

	// 安全检查：防止路径穿越
	absSync, _ := filepath.Abs(syncDir)
	absTarget, _ := filepath.Abs(targetDir)
	if !strings.HasPrefix(absTarget, absSync) {
		c.JSON(403, gin.H{"error": "禁止访问"})
		return
	}

	entries, err := os.ReadDir(targetDir)
	if err != nil {
		// 目录不存在返回空列表
		c.JSON(200, gin.H{"files": []interface{}{}, "path": "/" + reqPath})
		return
	}

	type fileEntry struct {
		Name     string `json:"name"`
		Size     int64  `json:"size"`
		IsDir    bool   `json:"is_dir"`
		Modified string `json:"modified"`
		Version  int    `json:"version"`
	}

	var files []fileEntry
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		fe := fileEntry{
			Name:     entry.Name(),
			IsDir:    entry.IsDir(),
			Modified: info.ModTime().Format("2006-01-02 15:04:05"),
		}
		if !entry.IsDir() {
			fe.Size = info.Size()
			// 从数据库获取版本号
			relPath := reqPath + "/" + entry.Name()
			if reqPath == "" {
				relPath = entry.Name()
			}
			pathKey := strings.ToLower(filepath.ToSlash(relPath))
			dbFile, _ := a.db.GetFile(pathKey)
			if dbFile != nil {
				fe.Version = int(dbFile.Version)
			}
		}
		files = append(files, fe)
	}

	c.JSON(200, gin.H{"files": files, "path": "/" + reqPath})
}
func hashToken(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}

// handleClientRegister 客户端嚜鍔ㄦ敞鍐?// 客户端繛鎺ユ椂鑷姩注册锛屽鏋滆澶囧悕宸插瓨鍦ㄤ笖鏈挙閿€鍒欒繑鍥炲凡鏈夎澶囦俊鎭?
// handleClientRegister 客户端注册接口
// 客户端需要提供管理员创建的 device_token 进行验证
// 验证通过后自动绑定设备信息到该token
func (a *API) handleClientRegister(c *gin.Context) {
	var req struct {
		Name        string `json:"name"`
		Platform    string `json:"platform"`
		DeviceToken string `json:"device_token"` // 管理员在web端创建的接入令牌
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "请求格式错误"})
		return
	}
	if req.Name == "" {
		c.JSON(400, gin.H{"error": "设备名称不能为空"})
		return
	}
	if req.DeviceToken == "" {
		c.JSON(400, gin.H{"error": "缺少访问令牌，请在服务端Web管理界面创建Token后填入"})
		return
	}
	// 验证管理员创建的设备Token是否有效
	valid, tokenID := a.db.VerifyDeviceTokenByToken(req.DeviceToken)
	if !valid {
		c.JSON(401, gin.H{"error": "访问令牌无效或已被使用，请检查令牌是否正确"})
		return
	}
	// 查询设备名是否已存在
	existing, _ := a.db.GetDeviceByName(req.Name)
	if existing != nil && !existing.Revoked {
		// 设备已存在，更新token哌員并绑定
		a.db.UpdateDeviceToken(existing.ID, req.DeviceToken)
		a.db.BindDeviceToken(tokenID, existing.ID)
		c.JSON(200, gin.H{"id": existing.ID, "name": existing.Name, "device_token": req.DeviceToken, "existed": true})
		return
	}
	// 注册新设备，使用管理员创建的token作为设备token
	d, err := a.db.CreateDevice(req.Name, req.DeviceToken, req.Platform)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	// 绑定Token到设备ID
	a.db.BindDeviceToken(tokenID, d.ID)
	a.db.AddActivityLog(d.ID, "register", "", fmt.Sprintf("设备注册: %s (%s)", req.Name, req.Platform))
	c.JSON(200, gin.H{"id": d.ID, "device_token": req.DeviceToken, "name": d.Name, "existed": false})
}

// handleChangePassword 淇敼管理员樺瘑鐮?
func (a *API) handleChangePassword(c *gin.Context) {
	var req struct {
		OldPassword string `json:"old_password"`
		NewPassword string `json:"new_password"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "请求格式错误"})
		return
	}
	if req.OldPassword == "" || req.NewPassword == "" {
		c.JSON(400, gin.H{"error": "旧密码和新密码不能为空"})
		return
	}
	if len(req.NewPassword) < 6 {
		c.JSON(400, gin.H{"error": "新密码长度不能少于6位"})
		return
	}
	// 获取褰撳墠管理员樼敤鎴峰悕
	username, _ := a.db.GetSetting("admin_username")
	if username == "" {
		username = "admin"
	}
	// 验证鏃у瘑鐮?
	if !a.db.LoginAdmin(username, req.OldPassword) {
		c.JSON(401, gin.H{"error": "旧密码错误"})
		return
	}
	// 淇敼密码
	if err := a.db.ChangeAdminPassword(req.NewPassword); err != nil {
		c.JSON(500, gin.H{"error": "淇敼密码失败: " + err.Error()})
		return
	}
	a.db.AddActivityLog("admin", "change_password", "", "管理员樺瘑鐮佸凡淇敼")
	c.JSON(200, gin.H{"ok": true, "message": "密码淇敼成功"})
}

// handleChangeUsername 淇敼管理员樼敤鎴峰悕
func (a *API) handleChangeUsername(c *gin.Context) {
	var req struct {
		NewUsername string `json:"new_username"`
		Password   string `json:"password"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "请求格式错误"})
		return
	}
	if req.NewUsername == "" || req.Password == "" {
		c.JSON(400, gin.H{"error": "新用户名和密码不能为空"})
		return
	}
	// 获取褰撳墠鐢ㄦ埛鍚?
	oldUsername, _ := a.db.GetSetting("admin_username")
	if oldUsername == "" {
		oldUsername = "admin"
	}
	// 验证密码
	if !a.db.LoginAdmin(oldUsername, req.Password) {
		c.JSON(401, gin.H{"error": "密码错误"})
		return
	}
	// 淇敼鐢ㄦ埛鍚?
	if err := a.db.ChangeAdminUsername(req.NewUsername); err != nil {
		c.JSON(500, gin.H{"error": "淇敼鐢ㄦ埛鍚嶅け璐? " + err.Error()})
		return
	}
	a.db.SetSetting("admin_username", req.NewUsername)
	a.db.AddActivityLog("admin", "change_username", "", fmt.Sprintf("管理员用户名已修改? %s -> %s", oldUsername, req.NewUsername))
	c.JSON(200, gin.H{"ok": true, "message": "用户名修改成功，请使用新用户名重新登录"})
}

// handleListTokens 返回所有设备Token列表
func (a *API) handleListTokens(c *gin.Context) {
	tokens, _ := a.db.ListDeviceTokens()
	if tokens == nil {
		tokens = []map[string]interface{}{}
	}
	c.JSON(200, gin.H{"tokens": tokens})
}

// handleCreateToken 创建新的设备Token
func (a *API) handleCreateToken(c *gin.Context) {
	token, id, err := a.db.CreateDeviceToken()
	if err != nil {
		c.JSON(500, gin.H{"error": "创建Token失败"})
		return
	}
	a.db.AddActivityLog("admin", "create_token", "", "创建设备Token")
	c.JSON(200, gin.H{"token": token, "id": id})
}

// handleDeleteToken 删除指定设备Token
func (a *API) handleDeleteToken(c *gin.Context) {
	var req struct {
		ID int64 `json:"id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "请求格式错误"})
		return
	}
	a.db.DeleteDeviceToken(req.ID)
	c.JSON(200, gin.H{"ok": true})
}

// handleDeleteLog 删除单条活动日志
func (a *API) handleDeleteLog(c *gin.Context) {
	var req struct {
		ID int64 `json:"id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "请求格式错误"})
		return
	}
	a.db.DeleteActivityLog(req.ID)
	c.JSON(200, gin.H{"ok": true})
}

// handleClearLogs 清空所有活动日志
func (a *API) handleClearLogs(c *gin.Context) {
	a.db.ClearActivityLogs()
	c.JSON(200, gin.H{"ok": true})
}

// handleClientSyncDirs 返回同步目录列表
func (a *API) handleClientSyncDirs(c *gin.Context) {
	syncRoot, _ := a.db.GetSetting("sync_root")
	if syncRoot == "" {
		syncRoot = filepath.Join(a.dataDir, "sync")
	}
	// 返回实际的子目录列表
	var dirs []gin.H
	// 始终包含根同步目录
	dirs = append(dirs, gin.H{"id": "default", "name": "/", "path": syncRoot})
	entries, err := os.ReadDir(syncRoot)
	if err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				dirs = append(dirs, gin.H{
					"id":   entry.Name(),
					"name": entry.Name(),
					"path": filepath.Join(syncRoot, entry.Name()),
				})
			}
		}
	}
	c.JSON(200, dirs)
}

// handleFileMove 移动文件到指定目录
func (a *API) handleFileMove(c *gin.Context) {
	var req struct {
		Paths []string `json:"paths"`
		Dest  string   `json:"dest"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "请求格式错误"})
		return
	}
	syncDir := filepath.Join(a.dataDir, "sync")
	for _, p := range req.Paths {
		srcPath := filepath.Join(syncDir, p)
		fileName := filepath.Base(p)
		destPath := filepath.Join(syncDir, req.Dest, fileName)
		os.MkdirAll(filepath.Dir(destPath), 0755)
		if err := os.Rename(srcPath, destPath); err != nil {
			c.JSON(500, gin.H{"error": fmt.Sprintf("移动失败: %v", err)})
			return
		}
		oldKey := strings.ToLower(filepath.ToSlash(p))
		newRelPath := filepath.ToSlash(filepath.Join(req.Dest, fileName))
		newKey := strings.ToLower(newRelPath)
		a.db.RenameFile(oldKey, newRelPath, newKey)
		a.db.AddActivityLog("admin", "move", p, fmt.Sprintf("移动 %s -> %s", p, newRelPath))
	}
	c.JSON(200, gin.H{"ok": true})
}

// handleGC 触发 Go GC
func (a *API) handleGC(c *gin.Context) {
	runtime.GC()
	c.JSON(200, gin.H{"ok": true})
}

// handleSystemScan 扫描系统垃圾（Windows临时文件、回收站等）
func (a *API) handleSystemScan(c *gin.Context) {
	type scanItem struct {
		Path string `json:"path"`
		Size int64  `json:"size"`
		Desc string `json:"desc"`
	}
	var items []scanItem
	var totalSize int64

	tempDir := os.TempDir()
	filepath.Walk(tempDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		age := time.Since(info.ModTime())
		if age > 7*24*time.Hour {
			items = append(items, scanItem{Path: path, Size: info.Size(), Desc: "临时文件（超过7天）"})
			totalSize += info.Size()
		}
		return nil
	})

	recyclePaths := []string{}
	if userProfile := os.Getenv("USERPROFILE"); userProfile != "" {
		recyclePaths = append(recyclePaths, filepath.Join(userProfile, "$Recycle.Bin"))
	}
	for _, rp := range recyclePaths {
		filepath.Walk(rp, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			items = append(items, scanItem{Path: path, Size: info.Size(), Desc: "回收站文件"})
			totalSize += info.Size()
			return nil
		})
	}

	if items == nil {
		items = []scanItem{}
	}
	c.JSON(200, gin.H{"items": items, "total_size": totalSize, "count": len(items)})
}

// handleSystemClean 清理指定的系统垃圾路径
func (a *API) handleSystemClean(c *gin.Context) {
	var req struct {
		Paths []string `json:"paths"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "请求格式错误"})
		return
	}
	var freed int64
	var cleaned int
	for _, p := range req.Paths {
		info, err := os.Stat(p)
		if err != nil {
			continue
		}
		size := info.Size()
		if err := os.Remove(p); err != nil {
			continue
		}
		freed += size
		cleaned++
	}
	a.db.AddActivityLog("admin", "system_clean", "", fmt.Sprintf("系统清理: 删除 %d 个文件，释放 %d 字节", cleaned, freed))
	c.JSON(200, gin.H{"ok": true, "freed": freed, "cleaned": cleaned})
}

// handleListTrash 返回回收站文件列表
func (a *API) handleListTrash(c *gin.Context) {
	trashDir := filepath.Join(a.dataDir, "trash")
	var files []map[string]interface{}
	filepath.Walk(trashDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		relPath, _ := filepath.Rel(trashDir, path)
		files = append(files, map[string]interface{}{
			"name": info.Name(),
			"path": relPath,
			"size": info.Size(),
		})
		return nil
	})
	if files == nil {
		files = []map[string]interface{}{}
	}
	c.JSON(200, gin.H{"files": files})
}

// handleRestoreTrash 恢复回收站文件到同步目录
func (a *API) handleRestoreTrash(c *gin.Context) {
	var req struct {
		Path string `json:"path"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "请求格式错误"})
		return
	}
	srcPath := filepath.Join(a.dataDir, "trash", req.Path)
	fileName := filepath.Base(req.Path)
	parts := strings.SplitN(fileName, "_", 2)
	if len(parts) == 2 {
		fileName = parts[1]
	}
	destPath := filepath.Join(a.dataDir, "sync", fileName)
	os.MkdirAll(filepath.Dir(destPath), 0755)
	if err := os.Rename(srcPath, destPath); err != nil {
		c.JSON(500, gin.H{"error": "恢复失败: " + err.Error()})
		return
	}
	c.JSON(200, gin.H{"ok": true})
}

// handleDeleteTrash 彻底删除回收站文件
func (a *API) handleDeleteTrash(c *gin.Context) {
	var req struct {
		Path string `json:"path"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "请求格式错误"})
		return
	}
	filePath := filepath.Join(a.dataDir, "trash", req.Path)
	if err := os.Remove(filePath); err != nil {
		c.JSON(500, gin.H{"error": "删除失败: " + err.Error()})
		return
	}
	c.JSON(200, gin.H{"ok": true})
}

// handleRestoreAllTrash 恢复回收站所有文件
func (a *API) handleRestoreAllTrash(c *gin.Context) {
	trashDir := filepath.Join(a.dataDir, "trash")
	restored := 0
	filepath.Walk(trashDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		fileName := filepath.Base(path)
		parts := strings.SplitN(fileName, "_", 2)
		if len(parts) == 2 {
			fileName = parts[1]
		}
		destPath := filepath.Join(a.dataDir, "sync", fileName)
		os.MkdirAll(filepath.Dir(destPath), 0755)
		if err := os.Rename(path, destPath); err == nil {
			restored++
		}
		return nil
	})
	c.JSON(200, gin.H{"restored": restored})
}

// handleClearTrash 清空回收站
func (a *API) handleClearTrash(c *gin.Context) {
	trashDir := filepath.Join(a.dataDir, "trash")
	deleted := 0
	filepath.Walk(trashDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if err := os.Remove(path); err == nil {
			deleted++
		}
		return nil
	})
	c.JSON(200, gin.H{"deleted": deleted})
}

// handleMkdir 创建新文件夹
func (a *API) handleMkdir(c *gin.Context) {
	var req struct {
		Path string `json:"path"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "请求格式错误"})
		return
	}
	dirPath := filepath.Join(a.dataDir, "sync", strings.TrimPrefix(req.Path, "/"))
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		c.JSON(500, gin.H{"error": "创建文件夹失败: " + err.Error()})
		return
	}
	a.db.AddActivityLog("admin", "mkdir", req.Path, "创建文件夹 "+req.Path)
	c.JSON(200, gin.H{"ok": true})
}



func (a *API) handleSetAvatar(c *gin.Context) {
	file, _, err := c.Request.FormFile("avatar")
	if err != nil {
		c.JSON(400, gin.H{"error": "请上传头像文件"})
		return
	}
	defer file.Close()

	outPath := filepath.Join(a.dataDir, "avatar.png")
	out, err := os.Create(outPath)
	if err != nil {
		c.JSON(500, gin.H{"error": "保存头像失败"})
		return
	}
	defer out.Close()

	if _, err := io.Copy(out, file); err != nil {
		c.JSON(500, gin.H{"error": "写入头像失败"})
		return
	}

	c.JSON(200, gin.H{"ok": true})
}

func (a *API) handleGetAvatar(c *gin.Context) {
	path := filepath.Join(a.dataDir, "avatar.png")
	if _, err := os.Stat(path); err != nil {
		c.Status(404)
		return
	}
	c.File(path)
}
