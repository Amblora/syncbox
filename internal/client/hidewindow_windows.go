//go:build windows

package client

import "syscall"

// getHideWindowAttr 返回Windows下隐藏子进程窗口的系统属性
func getHideWindowAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{HideWindow: true}
}
