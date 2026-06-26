//go:build !windows

package client

import "syscall"

// getHideWindowAttr 非Windows平台返回nil
func getHideWindowAttr() *syscall.SysProcAttr {
	return nil
}
