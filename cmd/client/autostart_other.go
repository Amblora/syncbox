//go:build !windows

package main

import "log"

// applyLaunchOnLogin 非 Windows 平台的桩实现
func applyLaunchOnLogin(enabled bool) {
	log.Printf("[自启动] 当前平台暂不支持开机自启动设置: %v", enabled)
}
