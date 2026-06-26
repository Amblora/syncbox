package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"net"
	"strings"

	"github.com/aync/syncbox/internal/api"
	"github.com/aync/syncbox/internal/db"
	"github.com/aync/syncbox/internal/syncengine"
	"github.com/aync/syncbox/internal/ws"
	"github.com/gin-gonic/gin"
)

// setSettingIfEmpty 仅在设置项为空时写入默认值
func setSettingIfEmpty(database *db.DB, key, value string) {
	existing, _ := database.GetSetting(key)
	if existing == "" {
		database.SetSetting(key, value)
	}
}

// getHostname 获取主机名，失败时返回 "unknown"
func getHostname() string {
	name, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return name
}

// getOSName 获取操作系统名称
func getOSName() string {
	switch runtime.GOOS {
	case "windows":
		return "Windows"
	case "linux":
		return "Linux"
	case "darwin":
		return "macOS"
	default:
		return runtime.GOOS
	}
}

// generateServerName 生成服务端设备名称: Server-hostname-OS
func generateServerName() string {
	return fmt.Sprintf("Server-%s-%s", getHostname(), getOSName())
}

func main() {
	addr := flag.String("addr", ":8080", "监听地址（默认 0.0.0.0:8080）")
	dataDir := flag.String("data", "./data", "数据目录")
	adminUser := flag.String("admin-user", "admin", "管理员用户名")
	adminPass := flag.String("admin-pass", "admin123", "管理员密码")
	scanInterval := flag.Int("scan", 300, "兜底扫描间隔（秒）")
	flag.Parse()

	// 创建数据目录结构
	dirs := []string{
		filepath.Join(*dataDir, "sync"),
		filepath.Join(*dataDir, "db"),
		filepath.Join(*dataDir, "temp"),
		filepath.Join(*dataDir, "conflicts"),
		filepath.Join(*dataDir, "trash"),
		filepath.Join(*dataDir, "logs"),
		filepath.Join(*dataDir, "backups"),
	}
	for _, d := range dirs {
		os.MkdirAll(d, 0755)
	}

	// 初始化 SQLite 数据库
	dbPath := filepath.Join(*dataDir, "db", "syncbox.db")
	database, err := db.NewDB(dbPath)
	if err != nil {
		log.Fatalf("打开数据库失败: %v", err)
	}
	defer database.Close()

	// 初始化管理员账户
	if err := database.InitAdmin(*adminUser, *adminPass); err != nil {
		log.Fatalf("初始化管理员失败: %v", err)
	}

	// 设置默认配置
	setSettingIfEmpty(database, "sync_root", filepath.Join(*dataDir, "sync"))
	setSettingIfEmpty(database, "scan_interval_seconds", "300")
	setSettingIfEmpty(database, "keep_deleted_days", "30")
	setSettingIfEmpty(database, "keep_conflict_days", "30")
	setSettingIfEmpty(database, "keep_log_days", "30")
	setSettingIfEmpty(database, "max_upload_concurrency", "2")
	setSettingIfEmpty(database, "listen_addr", *addr)

	// 自动注册服务端设备（如果尚未注册）
	serverName := generateServerName()
	serverDevice, _ := database.GetDeviceByName(serverName)
	if serverDevice == nil {
		token := db.GenerateToken()
		d, err := database.CreateDevice(serverName, token, "server")
		if err == nil {
			log.Printf("[设备] 服务端自动注册: %s (ID: %s)", serverName, d.ID)
		}
	}

	// 创建 WebSocket Hub 并启动
	hub := ws.NewHub()
	go hub.Run()

	// 创建并启动文件监控器
	watcher := syncengine.NewWatcher(&syncengine.DBAdapter{DB: database}, *dataDir)
	watcher.SetOnChange(func(ev syncengine.SyncEvent) {
		hub.BroadcastJSON(ws.Message{
			Type: ev.Type,
			Data: gin.H{"rel_path": ev.RelPath, "size": ev.Size, "hash": ev.Hash, "version": ev.Version},
		})
	})
	if err := watcher.Start(); err != nil {
		log.Printf("文件监控启动失败: %v（不影响服务运行）", err)
	}
	defer watcher.Stop()

	// 首次全量扫描
	go func() {
		if err := watcher.ScanDir(); err != nil {
			log.Printf("首次扫描失败: %v", err)
		}
	}()

	// 创建 API 处理器
	a := api.NewAPI(database, hub, *dataDir)

	// 创建 Gin 路由
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(corsMiddleware())

	// 静态文件服务（Web 管理界面）
	webDir := "./web"
	if fi, err := os.Stat("web"); err == nil && fi.IsDir() {
		webDir = "web"
	}
	r.StaticFile("/", filepath.Join(webDir, "index.html"))
	r.StaticFile("/index.html", filepath.Join(webDir, "index.html"))
	r.Static("/static", filepath.Join(webDir, "static"))

	// 注册 API 路由
	a.SetupRoutes(r)

	// 启动服务器 — 打印完整启动信息（含用户名密码）
	separator := strings.Repeat("=", 50)
	log.Println(separator)
	log.Println("  SyncBox 同步服务器启动")
	log.Println(separator)
	log.Printf("  监听地址:   %s", *addr)
	log.Printf("  数据目录:   %s", *dataDir)
	log.Printf("  服务端名称: %s", serverName)
	log.Printf("  管理员用户: %s", *adminUser)
	log.Printf("  管理员密码: %s", *adminPass)
	log.Printf("  扫描间隔:   %d 秒", *scanInterval)
	log.Println(separator)
	// 尝试获取本机 IP
	if ip := getLocalIP(); ip != "" {
		log.Printf("  局域网访问: http://%s:8080", ip)
	}
	log.Printf("  本地访问:   http://localhost:8080")
	log.Println(separator)
	log.Println("  ⚠ 首次登录后请立即修改默认密码！")
	log.Println("  ⚠ 用户名密码可在 Web 界面「设置」页面修改")
	log.Println(separator)

	srv := &http.Server{Addr: *addr, Handler: r}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("服务器错误: %v", err)
	}
}

// getLocalIP 获取本机局域网 IP
func getLocalIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return ""
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}

func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, PATCH")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	}
}