//go:build windows

package main

import (
	"log"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows/registry"
)

const regRunKey = `Software\Microsoft\Windows\CurrentVersion\Run`
const regValueName = "SyncBoxClient"

// applyLaunchOnLogin 设置或取消开机自启动（Windows 注册表 Run 键）
func applyLaunchOnLogin(enabled bool) {
	exePath, err := os.Executable()
	if err != nil {
		log.Printf("[自启动] 获取可执行文件路径失败: %v", err)
		return
	}
	exePath, _ = filepath.Abs(exePath)

	k, err := registry.OpenKey(registry.CURRENT_USER, regRunKey, registry.SET_VALUE)
	if err != nil {
		log.Printf("[自启动] 打开注册表失败: %v", err)
		return
	}
	defer k.Close()

	if enabled {
		// 路径含空格时需要加引号
		val := "\"" + exePath + "\""
		if err := k.SetStringValue(regValueName, val); err != nil {
			log.Printf("[自启动] 写入注册表失败: %v", err)
			return
		}
		log.Printf("[自启动] 已启用开机自启动: %s", exePath)
	} else {
		if err := k.DeleteValue(regValueName); err != nil {
			// 值不存在也算成功
			log.Printf("[自启动] 删除注册表值: %v (可能本来就不存在)", err)
		} else {
			log.Printf("[自启动] 已禁用开机自启动")
		}
	}
}
