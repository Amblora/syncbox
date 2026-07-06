//go:build darwin

package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
)

const launchAgentLabel = "com.syncbox.client"

func getLaunchAgentPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "Library", "LaunchAgents", launchAgentLabel+".plist")
}

func getLaunchAgentPlist(exePath string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>%s</string>
	<key>ProgramArguments</key>
	<array>
		<string>%s</string>
	</array>
	<key>RunAtLoad</key>
	<true/>
	<key>KeepAlive</key>
	<false/>
</dict>
</plist>`, launchAgentLabel, exePath)
}

func applyLaunchOnLogin(enabled bool) {
	plistPath := getLaunchAgentPath()

	if !enabled {
		exec.Command("launchctl", "unload", "-w", plistPath).Run()
		os.Remove(plistPath)
		log.Printf("[自启动] macOS LaunchAgent 已禁用")
		return
	}

	exePath, err := os.Executable()
	if err != nil {
		log.Printf("[自启动] 获取可执行文件路径失败: %v", err)
		return
	}
	exePath, _ = filepath.Abs(exePath)

	launchDir := filepath.Dir(plistPath)
	os.MkdirAll(launchDir, 0755)

	plistContent := getLaunchAgentPlist(exePath)
	if err := os.WriteFile(plistPath, []byte(plistContent), 0644); err != nil {
		log.Printf("[自启动] 写入 LaunchAgent plist 失败: %v", err)
		return
	}

	if err := exec.Command("launchctl", "load", "-w", plistPath).Run(); err != nil {
		log.Printf("[自启动] launchctl load 失败: %v (可能已加载)", err)
	}

	log.Printf("[自启动] macOS LaunchAgent 已启用: %s", exePath)
}
