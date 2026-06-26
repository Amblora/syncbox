package main

import (
	"encoding/json"
	"fmt"
	"runtime/debug"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"github.com/aync/syncbox/internal/client"
)

// AppConfig 应用配置结构体
type AppConfig struct {
	ServerURL  string `json:"server_url"`
	LocalDir   string `json:"local_dir"`
	DeviceID   string `json:"device_id"`
	Token      string `json:"token"`
	DeviceName string `json:"device_name"`
	HideOnStart bool `json:"hide_on_start"`
	ServerDirID string `json:"server_dir_id"`
	LaunchOnLogin bool `json:"launch_on_login"`
	RemoteDirName  string `json:"remote_dir_name"`
}

func generateClientName() string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("Client-%s-%s", hostname, runtime.GOOS)
}

func getConfigPath() string {
	cwd, err := os.Getwd()
	if err == nil {
		p := filepath.Join(cwd, "syncbox-client.json")
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	exePath, err := os.Executable()
	if err == nil {
		return filepath.Join(filepath.Dir(exePath), "syncbox-client.json")
	}
	return "syncbox-client.json"
}

func loadConfig() AppConfig {
	cfgPath := getConfigPath()
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		return AppConfig{}
	}
	var cfg AppConfig
	json.Unmarshal(data, &cfg)
	cfg.ServerURL = strings.TrimRight(cfg.ServerURL, "/")
	return cfg
}

func saveConfig(cfg AppConfig) error {
	cfg.ServerURL = strings.TrimRight(cfg.ServerURL, "/")
	cwd, _ := os.Getwd()
	cfgPath := filepath.Join(cwd, "syncbox-client.json")
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(cfgPath, data, 0666)
}

type App struct {
	cfg        AppConfig
	fyneApp    fyne.App
	mainWindow fyne.Window
	syncClient *client.SyncClient
	mu         sync.Mutex
	paused     bool
	connected  bool
	connecting bool

	eventLog []string
	eventMu  sync.Mutex

	connStatusLabel *widget.Label
	syncStatusLabel *widget.Label
	serverLabel     *widget.Label
	dirLabel        *widget.Label
	lastSyncLabel   *widget.Label
	remoteDirLabel  *widget.Label
	logList         *widget.List
	serverEntry     *widget.Entry
	dirEntry        *widget.Entry
	tokenEntry      *widget.Entry
	deviceNameLabel *widget.Label
	deviceIDLabel   *widget.Label

	hideOnStartCheck *widget.Check
	speedLabel       *widget.Label
	serverDirSelect  *widget.Select
	suppressSelectCB bool // 临时禁止选择回调
	autostartCheck   *widget.Check
}

func main() {
	debug.SetGCPercent(40)
	debug.SetMemoryLimit(200 * 1024 * 1024)

	if !ensureSingleInstance() {
		return
	}

	fyneApp := app.NewWithID("com.syncbox.client")
	fyneApp.Settings().SetTheme(theme.LightTheme())
	// 设置应用图标（从嵌入资源加载）
	iconRes := getIconResource()
	fyneApp.SetIcon(iconRes)
	mainWindow := fyneApp.NewWindow("SyncBox 文件同步客户端")
	mainWindow.SetIcon(iconRes)
	mainWindow.Resize(fyne.NewSize(300, 500))

	a := &App{
		cfg:        loadConfig(),
		fyneApp:    fyneApp,
		mainWindow: mainWindow,
	}
	a.buildUI()

	if a.cfg.DeviceName == "" {
		a.cfg.DeviceName = generateClientName()
	}
	if a.cfg.ServerURL != "" && a.cfg.Token != "" && a.cfg.DeviceID != "" && a.cfg.LocalDir != "" && a.cfg.RemoteDirName != "" {
		if a.remoteDirLabel != nil {
			a.remoteDirLabel.SetText("远端目录: " + a.cfg.RemoteDirName)
		}
		go func() {
			time.Sleep(2 * time.Second)
			a.startSyncWithRetry(5, 5*time.Second)
		}()
	}

	go a.startSpeedPoller()
	a.setupSystemTray()
	mainWindow.SetCloseIntercept(func() { mainWindow.Hide() })

	if a.cfg.HideOnStart {
		mainWindow.Hide()
	} else {
		mainWindow.Show()
	}

	fyneApp.Run()
}

func (a *App) buildUI() {
	tabs := container.NewAppTabs(
		container.NewTabItem("同步概览", a.buildOverviewTab()),
		container.NewTabItem("连接向导", a.buildWizardTab()),
		container.NewTabItem("传输日志", a.buildLogTab()),
		container.NewTabItem("设置", a.buildSettingsTab()),
	)
	a.mainWindow.SetContent(tabs)
}

func (a *App) buildOverviewTab() fyne.CanvasObject {
	a.connStatusLabel = widget.NewLabel("连接状态: 未连接")
	a.connStatusLabel.TextStyle = fyne.TextStyle{Bold: true}
	a.syncStatusLabel = widget.NewLabel("同步状态: 空闲")
	a.serverLabel = widget.NewLabel("远端地址: " + a.cfg.ServerURL)
	a.dirLabel = widget.NewLabel("本地目录: " + a.cfg.LocalDir)
	a.remoteDirLabel = widget.NewLabel("远端目录: " + a.cfg.RemoteDirName)
	a.lastSyncLabel = widget.NewLabel("上次同步: 无")

	testBtn := widget.NewButton("测试连接", func() { a.testConnection() })
	pauseBtn := widget.NewButton("暂停同步", func() { a.pauseSync() })
	resumeBtn := widget.NewButton("恢复同步", func() { a.resumeSync() })
	syncBtn := widget.NewButton("立即同步", func() { a.scanNow() })
	openDirBtn := widget.NewButton("打开目录", func() { a.openSyncDir() })

	a.speedLabel = widget.NewLabel("同步速度: ↑0 B/S ↓0 B/S")

	// 远端目录选择器
	a.serverDirSelect = widget.NewSelect([]string{}, func(value string) {
		// 如果是程序自动设置的，不弹确认
		if a.suppressSelectCB {
			a.suppressSelectCB = false
			return
		}
		// 弹出确认对话框
		prevValue := a.cfg.RemoteDirName
		dialog.ShowConfirm("确认远端目录",
			"将同步远端目录设置为:\n\n  "+value+"\n\n确认后将立即开始同步",
			func(ok bool) {
				if !ok {
					// 取消，恢复上次的选择
					if prevValue != "" {
						a.serverDirSelect.SetSelected(prevValue)
					} else {
						a.serverDirSelect.ClearSelected()
					}
					return
				}
				// 确认
				log.Printf("[概览] 选择远端目录: %s", value)
				a.cfg.RemoteDirName = value
				if a.syncClient != nil {
					a.syncClient.SyncDirName = value
				}
				if a.remoteDirLabel != nil {
					a.remoteDirLabel.SetText("远端目录: " + value)
				}
				saveConfig(a.cfg)
				// 启动同步
				if a.cfg.DeviceID != "" && a.syncClient == nil && !a.connecting {
					a.startSync()
				}
			}, a.mainWindow)
	})
	if a.cfg.RemoteDirName != "" {
		a.suppressSelectCB = true
		a.serverDirSelect.SetSelected(a.cfg.RemoteDirName)
	}
	refreshDirBtn := widget.NewButton("刷新远端目录", func() { a.refreshServerDirs() })
	remoteDirRow := container.NewBorder(nil, nil, nil, refreshDirBtn, a.serverDirSelect)

	buttons := container.NewGridWithColumns(4, pauseBtn, resumeBtn, syncBtn, openDirBtn)
	testRow := container.NewHBox(a.connStatusLabel, testBtn)

	card := widget.NewCard("同步概览", "", container.NewVBox(testRow, a.syncStatusLabel, a.serverLabel, a.dirLabel, a.remoteDirLabel, remoteDirRow, a.lastSyncLabel, a.speedLabel, layout.NewSpacer(), buttons))
	return container.NewPadded(card)
}

func (a *App) testConnection() {
	a.mu.Lock()
	serverURL := a.cfg.ServerURL
	deviceID := a.cfg.DeviceID
	token := a.cfg.Token
	a.mu.Unlock()

	if serverURL == "" {
		dialog.ShowError(fmt.Errorf("请先在连接向导中配置服务端地址"), a.mainWindow)
		return
	}

	go func() {
		hc := &http.Client{Timeout: 5 * time.Second}
		url := strings.TrimRight(serverURL, "/") + "/health"
		log.Printf("[测试] 请求: %s", url)
		resp, err := hc.Get(url)
		if err != nil {
			// UI操作回到主线程
			a.mainWindow.Canvas().Refresh(a.connStatusLabel)
			log.Printf("[测试] 连接失败: %v", err)
			dialog.ShowError(fmt.Errorf("连接失败: %v", err), a.mainWindow)
			return
		}
		resp.Body.Close()

		if resp.StatusCode != 200 {
			dialog.ShowError(fmt.Errorf("服务端返回错误: %d", resp.StatusCode), a.mainWindow)
			return
		}

		if deviceID != "" && token != "" {
			req, _ := http.NewRequest("POST", strings.TrimRight(serverURL, "/")+"/api/client/hello", nil)
			req.Header.Set("Authorization", "Bearer "+deviceID+":"+token)
			resp2, err := hc.Do(req)
			if err != nil {
				dialog.ShowInformation("连接测试", "服务端可达，但设备认证失败: "+err.Error(), a.mainWindow)
				return
			}
			resp2.Body.Close()
			if resp2.StatusCode == 200 {
				dialog.ShowInformation("连接测试", "连接成功！服务端正常，设备已认证。", a.mainWindow)
			} else {
				dialog.ShowInformation("连接测试", fmt.Sprintf("服务端可达，但设备认证失败(%d)，请检查Token。", resp2.StatusCode), a.mainWindow)
			}
		} else {
			dialog.ShowInformation("连接测试", "连接成功！服务端正常响应。请在连接向导中配置Token。", a.mainWindow)
		}
	}()
}

func (a *App) buildWizardTab() fyne.CanvasObject {
	a.serverEntry = widget.NewEntry()
	a.serverEntry.SetPlaceHolder("服务器地址，例如: http://localhost:8080")
	a.serverEntry.SetText(a.cfg.ServerURL)

	a.dirEntry = widget.NewEntry()
	a.dirEntry.SetPlaceHolder("本地同步目录路径")
	a.dirEntry.SetText(a.cfg.LocalDir)

	browseBtn := widget.NewButton("浏览...", func() {
		dialog.ShowFolderOpen(func(lu fyne.ListableURI, err error) {
			if err == nil && lu != nil {
				a.dirEntry.SetText(lu.Path())
			}
		}, a.mainWindow)
	})
	dirRow := container.NewBorder(nil, nil, nil, browseBtn, a.dirEntry)

	a.tokenEntry = widget.NewEntry()
	a.tokenEntry.SetPlaceHolder("访问令牌（无令牌将无法连接服务端）")
	a.tokenEntry.SetText(a.cfg.Token)

	saveBtn := widget.NewButton("保存并连接", func() { a.saveAndConnect() })

	return container.NewVBox(
		widget.NewLabel("服务端地址"), a.serverEntry,
		widget.NewLabel("本地同步目录"), dirRow,
		widget.NewLabel("访问令牌"), a.tokenEntry,
		layout.NewSpacer(), saveBtn,
	)
}

func (a *App) refreshServerDirs() {
	a.mu.Lock()
	serverURL := a.cfg.ServerURL
	token := a.cfg.Token
	deviceID := a.cfg.DeviceID
	a.mu.Unlock()

	if serverURL == "" || token == "" {
		log.Printf("[远端目录] 服务端地址或令牌未配置")
		return
	}
	if deviceID == "" {
		log.Printf("[远端目录] 设备未注册，请先连接服务端")
		return
	}

	go func() {
		hc := &http.Client{Timeout: 10 * time.Second}
		req, err := http.NewRequest("GET", strings.TrimRight(serverURL, "/")+"/api/client/sync_dirs", nil)
		if err != nil {
			log.Printf("[远端目录] 创建请求失败: %v", err)
			return
		}
		req.Header.Set("Authorization", "Bearer "+deviceID+":"+token)

		resp, err := hc.Do(req)
		if err != nil {
			log.Printf("[远端目录] 请求失败: %v", err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			bodyBytes, _ := io.ReadAll(resp.Body)
			log.Printf("[远端目录] 服务端返回 %d: %s", resp.StatusCode, string(bodyBytes))
			return
		}

		var dirs []struct {
			ID   string `json:"id"` 
			Name string `json:"name"` 
		}
		if err := json.NewDecoder(resp.Body).Decode(&dirs); err != nil {
			log.Printf("[远端目录] 解析失败: %v", err)
			return
		}

		log.Printf("[远端目录] 获取到 %d 个目录", len(dirs))
		var options []string
		for _, d := range dirs {
			if d.Name != "" {
				options = append(options, d.Name)
			}
		}

		if len(options) == 0 {
			log.Printf("[远端目录] 服务端无可用目录")
			return
		}

		// Fyne UI 更新必须在主线程
		fyne.CurrentApp().Driver().CanvasForObject(a.serverDirSelect).Refresh(a.serverDirSelect)
		a.serverDirSelect.Options = options

		// 如果有已保存的远端目录名，选中它
		found := false
		if a.cfg.RemoteDirName != "" {
			for _, opt := range options {
				if opt == a.cfg.RemoteDirName {
					a.suppressSelectCB = true
					a.serverDirSelect.SetSelected(opt)
					found = true
					break
				}
			}
		}
		if !found {
			// 不自动选择，留空让用户手动选择
			log.Printf("[远端目录] 未找到已保存的目录，请手动选择")
		}
		if a.remoteDirLabel != nil {
			a.remoteDirLabel.SetText("远端目录: " + a.cfg.RemoteDirName)
		}
		a.mainWindow.Canvas().Refresh(a.serverDirSelect)
	}()
}
func (a *App) buildLogTab() fyne.CanvasObject {
	a.logList = widget.NewList(
		func() int { a.eventMu.Lock(); defer a.eventMu.Unlock(); return len(a.eventLog) },
		func() fyne.CanvasObject { return widget.NewLabel("") },
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			a.eventMu.Lock(); defer a.eventMu.Unlock()
			if id < len(a.eventLog) { obj.(*widget.Label).SetText(a.eventLog[id]) }
		},
	)
	return a.logList
}

func (a *App) buildSettingsTab() fyne.CanvasObject {
	a.deviceNameLabel = widget.NewLabel("设备名称: " + a.cfg.DeviceName)
	a.deviceIDLabel = widget.NewLabel("设备ID: " + maskID(a.cfg.DeviceID))
	syncDirLabel := widget.NewLabel("本地同步目录: " + a.cfg.LocalDir)
	changeDirBtn := widget.NewButton("修改本地同步目录", func() {
		fd := dialog.NewFolderOpen(func(uri fyne.ListableURI, err error) {
			if err != nil || uri == nil {
				return
			}
			a.cfg.LocalDir = uri.Path()
			syncDirLabel.SetText("本地同步目录: " + a.cfg.LocalDir)
			a.dirLabel.SetText("本地目录: " + a.cfg.LocalDir)
			if err := saveConfig(a.cfg); err != nil {
				dialog.ShowError(err, a.mainWindow)
			}
		}, a.mainWindow)
		fd.Show()
	})

	a.autostartCheck = widget.NewCheck("开机自启动", func(checked bool) {
		a.cfg.LaunchOnLogin = checked
		applyLaunchOnLogin(checked)
		if err := saveConfig(a.cfg); err != nil {
			dialog.ShowError(err, a.mainWindow)
		}
	})
	a.autostartCheck.SetChecked(a.cfg.LaunchOnLogin)

	a.hideOnStartCheck = widget.NewCheck("启动时隐藏主界面（仅托盘运行）", func(checked bool) {
		a.cfg.HideOnStart = checked
		if err := saveConfig(a.cfg); err != nil {
			dialog.ShowError(err, a.mainWindow)
		}
	})
	a.hideOnStartCheck.SetChecked(a.cfg.HideOnStart)

	content := container.NewVBox(a.deviceNameLabel, a.deviceIDLabel, syncDirLabel, changeDirBtn, a.autostartCheck, a.hideOnStartCheck)
	card := widget.NewCard("设置", "", content)
	return container.NewPadded(card)
}

func maskID(id string) string {
	if len(id) <= 8 { return id }
	return id[:4] + "****" + id[len(id)-4:]
}

func (a *App) saveAndConnect() {
	oldToken := a.cfg.Token
	a.cfg.ServerURL = strings.TrimRight(a.serverEntry.Text, "/")
	a.cfg.LocalDir = a.dirEntry.Text
	a.cfg.Token = a.tokenEntry.Text

	if a.cfg.ServerURL == "" || a.cfg.LocalDir == "" || a.cfg.Token == "" {
		dialog.ShowError(fmt.Errorf("服务端地址、本地目录和访问令牌不能为空"), a.mainWindow)
		return
	}
	if a.cfg.DeviceName == "" {
		a.cfg.DeviceName = generateClientName()
	}

	// 如果Token发生了变化，清除DeviceID强制重新注册
	if oldToken != "" && oldToken != a.cfg.Token {
		log.Printf("[注册] Token已变化，清除旧DeviceID重新注册")
		a.cfg.DeviceID = ""
	}

	if err := saveConfig(a.cfg); err != nil {
		dialog.ShowError(fmt.Errorf("保存配置失败: %v", err), a.mainWindow)
		return
	}
	a.serverLabel.SetText("远端地址: " + a.cfg.ServerURL)
	a.dirLabel.SetText("本地目录: " + a.cfg.LocalDir)

	if a.syncClient != nil {
		a.syncClient.Stop()
		a.syncClient = nil
	}
	a.mu.Lock()
	a.connected = false
	a.connecting = true
	a.mu.Unlock()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[注册] panic: %v", r)
			}
		}()

		// 每次连接都调用Register，服务端有名称去重逻辑，不会重复创建
		sc := client.NewSyncClient(a.cfg.ServerURL, a.cfg.Token, a.cfg.LocalDir)
		sc.SyncDirName = a.cfg.RemoteDirName
		if err := sc.Register(a.cfg.DeviceName); err != nil {
			log.Printf("[注册] 失败: %v", err)
			a.mu.Lock()
			a.connecting = false
			a.mu.Unlock()
			dialog.ShowError(fmt.Errorf("注册失败: %v", err), a.mainWindow)
			return
		}
		a.mu.Lock()
		a.cfg.DeviceID = sc.DeviceID
		a.mu.Unlock()
		saveConfig(a.cfg)
		log.Printf("[注册] 成功 DeviceID=%s", sc.DeviceID)

		a.mu.Lock()
		a.connecting = false
		a.mu.Unlock()
		if a.cfg.RemoteDirName != "" {
			a.startSync()
		} else {
			a.updateConnStatus("已连接，请选择远端目录")
		}
		go a.refreshServerDirs()
		dialog.ShowInformation("连接成功", "已成功连接到服务端，同步已启动。", a.mainWindow)
	}()
}

func (a *App) startSync() {
	a.mu.Lock()
	if (a.connected && a.syncClient != nil) || a.connecting {
		a.mu.Unlock()
		return
	}
	a.connecting = true
	serverURL := a.cfg.ServerURL
	token := a.cfg.Token
	deviceID := a.cfg.DeviceID
	localDir := a.cfg.LocalDir
	a.mu.Unlock()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[同步] panic恢复: %v", r)
			}
		}()

		sc := client.NewSyncClient(serverURL, token, localDir)
		sc.DeviceID = deviceID
		sc.SyncDirName = a.cfg.RemoteDirName

		if err := sc.Connect(); err != nil {
			log.Printf("[同步] 连接失败: %v", err)
			a.mu.Lock(); a.connecting = false; a.connected = false; a.mu.Unlock()
			a.updateConnStatus("未连接")
			return
		}
		if err := sc.Start(); err != nil {
			log.Printf("[同步] 启动失败: %v", err)
			a.mu.Lock(); a.connecting = false; a.connected = false; a.mu.Unlock()
			a.updateConnStatus("未连接")
			return
		}

		a.mu.Lock()
		a.syncClient = sc
		a.connected = true
		a.connecting = false
		a.paused = false
		a.mu.Unlock()
		a.updateConnStatus("已连接")
		a.updateSyncStatus("同步中")
		log.Printf("[同步] 服务已启动，设备ID: %s", deviceID)

		go func() {
			for event := range sc.Events() {
				a.addEvent(fmt.Sprintf("[%s] %s", event.Type, event.Message))
				// 自动刷新上次同步时间
				if event.Type == "upload" || event.Type == "download" || event.Type == "info" {
					a.updateLastSyncTime()
				}
			}
		}()

		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			failCount := 0
			for {
				select {
				case <-sc.StopCh():
					return
				case <-ticker.C:
					if err := sc.Hello(); err != nil {
						failCount++
						log.Printf("[心跳] 失败 (%d/5): %v", failCount, err)
						a.updateConnStatus(fmt.Sprintf("网络异常 重试%d/5", failCount))
						if failCount >= 5 {
							log.Printf("[心跳] 连续5次失败，停止同步并重连")
							sc.Stop()
							a.mu.Lock()
							a.syncClient = nil
							a.connected = false
							a.mu.Unlock()
							a.updateConnStatus("正在重连...")
							go a.reconnectLoop()
							return
						}
					} else {
						if failCount > 0 {
							log.Printf("[心跳] 网络恢复")
						}
						failCount = 0
						a.updateConnStatus("已连接")
					}
				}
			}
		}()
	}()
}


// reconnectLoop 断线后自动重连循环
func (a *App) reconnectLoop() {
	log.Printf("[重连] 开始自动重连...")
	for i := 0; i < 60; i++ {
		time.Sleep(10 * time.Second)
		// 检查是否已经手动重连
		a.mu.Lock()
		if a.connected || a.syncClient != nil {
			a.mu.Unlock()
			log.Printf("[重连] 已连接，停止重连")
			return
		}
		a.mu.Unlock()

		log.Printf("[重连] 尝试重连 (%d/60)...", i+1)
		a.startSync()

		// 等待连接结果
		time.Sleep(5 * time.Second)
		a.mu.Lock()
		ok := a.connected
		a.mu.Unlock()
		if ok {
			log.Printf("[重连] 重连成功")
			return
		}
	}
	log.Printf("[重连] 重连失败，请检查网络和服务端")
	a.updateConnStatus("重连失败")
}

// startSyncWithRetry 带重试的同步启动（开机自启场景用）
func (a *App) startSyncWithRetry(maxRetries int, interval time.Duration) {
	for i := 0; i < maxRetries; i++ {
		if a.cfg.DeviceID == "" || a.cfg.RemoteDirName == "" {
			log.Printf("[同步] 设备未就绪，等待...")
			return
		}
		a.startSync()
		// 等待片刻检查是否连接成功
		time.Sleep(3 * time.Second)
		a.mu.Lock()
		ok := a.connected
		a.mu.Unlock()
		if ok {
			log.Printf("[同步] 自动启动同步成功")
			return
		}
		log.Printf("[同步] 连接失败，%v后重试 (%d/%d)", interval, i+1, maxRetries)
		time.Sleep(interval)
	}
	log.Printf("[同步] 自动启动失败，已达最大重试次数")
}

func (a *App) updateConnStatus(status string) {
	if a.connStatusLabel != nil {
		fyne.CurrentApp().Driver().DoFromGoroutine(func() {
			a.connStatusLabel.SetText("连接状态: " + status)
		}, false)
	}
}

func (a *App) updateSyncStatus(status string) {
	if a.syncStatusLabel != nil {
		fyne.CurrentApp().Driver().DoFromGoroutine(func() {
			a.syncStatusLabel.SetText("同步状态: " + status)
		}, false)
	}
}

func (a *App) updateLastSyncTime() {
	if a.lastSyncLabel != nil {
		fyne.CurrentApp().Driver().DoFromGoroutine(func() {
			a.lastSyncLabel.SetText("上次同步: " + time.Now().Format("2006-01-02 15:04:05"))
		}, false)
	}
}

func (a *App) pauseSync() {
	a.mu.Lock(); defer a.mu.Unlock()
	if a.syncClient != nil && a.connected {
		a.syncClient.Pause()
		a.paused = true
		a.updateSyncStatus("已暂停")
		a.addEvent("同步已暂停")
	}
}

func (a *App) resumeSync() {
	a.mu.Lock(); defer a.mu.Unlock()
	if a.syncClient != nil && a.connected && a.paused {
		a.syncClient.Resume()
		a.paused = false
		a.updateSyncStatus("同步中")
		a.addEvent("同步已恢复")
	}
}

func (a *App) scanNow() {
	if a.cfg.RemoteDirName == "" {
		dialog.ShowInformation("提示", "请先在概览页选择远端目录", a.mainWindow)
		return
	}
	a.mu.Lock(); defer a.mu.Unlock()
	if a.syncClient != nil && a.connected {
		a.updateSyncStatus("同步中")
		go func() {
			a.syncClient.ScanAndSync()
			a.updateLastSyncTime()
			a.updateSyncStatus("等待同步")
		}()
		a.addEvent("手动同步已触发")
	}
}

func (a *App) openSyncDir() {
	dir := a.cfg.LocalDir
	if dir == "" {
		dialog.ShowError(fmt.Errorf("请先在连接向导中选择同步目录"), a.mainWindow)
		return
	}
	dir = filepath.FromSlash(dir)
	if err := os.MkdirAll(dir, 0777); err != nil {
		dialog.ShowError(fmt.Errorf("创建目录失败: %v", err), a.mainWindow)
		return
	}
	openPath(dir)
}


func (a *App) updateSpeedLabel(uploadBps, downloadBps int64) {
	if a.speedLabel == nil {
		return
	}
	fyne.CurrentApp().Driver().DoFromGoroutine(func() {
			a.speedLabel.SetText(fmt.Sprintf("同步速度: ↑%s/S ↓%s/S", humanBytes(uploadBps), humanBytes(downloadBps)))
		}, false)
}
func (a *App) setupSystemTray() {
	if a.fyneApp.Driver().Device().IsMobile() {
		return
	}
	// 设置系统托盘图标
	iconRes := getIconResource()
	a.fyneApp.SetIcon(iconRes)
	menu := fyne.NewMenu("SyncBox",
		fyne.NewMenuItem("打开 SyncBox", func() { a.mainWindow.Show() }),
		fyne.NewMenuItemSeparator(),
		fyne.NewMenuItem("暂停同步", func() { a.pauseSync() }),
		fyne.NewMenuItem("恢复同步", func() { a.resumeSync() }),
		fyne.NewMenuItem("立即扫描", func() { a.scanNow() }),
		fyne.NewMenuItem("打开同步目录", func() { a.openSyncDir() }),
		fyne.NewMenuItemSeparator(),
		fyne.NewMenuItem("退出", func() {
			if a.syncClient != nil { a.syncClient.Stop() }
			a.fyneApp.Quit()
		}),
	)
	if d, ok := a.fyneApp.Driver().(interface{ SetSystemTrayMenu(*fyne.Menu) }); ok {
		d.SetSystemTrayMenu(menu)
	}
}

func (a *App) addEvent(msg string) {
	a.eventMu.Lock()
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	a.eventLog = append(a.eventLog, fmt.Sprintf("[%s] %s", timestamp, msg))
	if len(a.eventLog) > 500 {
		a.eventLog = a.eventLog[len(a.eventLog)-500:]
	}
	newIdx := len(a.eventLog) - 1
	a.eventMu.Unlock()
	log.Println(msg)
	if a.logList != nil {
		fyne.CurrentApp().Driver().DoFromGoroutine(func() {
			a.logList.Refresh()
			a.logList.ScrollTo(widget.ListItemID(newIdx))
		}, false)
	}
}

func openPath(path string) {
	path = filepath.FromSlash(path)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		log.Printf("路径不存在: %s", path)
		return
	}
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("explorer.exe", path)
	case "darwin":
		cmd = exec.Command("open", path)
	default:
		cmd = exec.Command("xdg-open", path)
	}
	cmd.Start()
}

func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.0f %c", float64(b)/float64(div), "KMGTPE"[exp])
}


func ensureSingleInstance() bool {
	host := "127.0.0.1"
	port := 59212
	addr := fmt.Sprintf("%s:%d", host, port)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		http.Get(fmt.Sprintf("http://%s/open", addr))
		return false
	}

	go func() {
		http.HandleFunc("/open", func(w http.ResponseWriter, r *http.Request) {
			log.Println("[实例] 收到打开窗口请求")
		})
		http.Serve(ln, nil)
	}()

	return true
}

func (a *App) startSpeedPoller() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		a.mu.Lock()
		sc := a.syncClient
		a.mu.Unlock()
		if sc == nil {
			a.updateSpeedLabel(0, 0)
			continue
		}
		up, down := sc.GetSpeedStats()
		a.updateSpeedLabel(up, down)
	}
}


// applyLaunchOnLogin 已移到平台文件 (hidewindow_windows.go / autostart_other.go)
