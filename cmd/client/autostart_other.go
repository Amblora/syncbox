//go:build !windows && !darwin

package main

import "log"

func applyLaunchOnLogin(enabled bool) {
	log.Printf("[自启动] 当前平台暂不支持开机自启动设置: %v", enabled)
}
