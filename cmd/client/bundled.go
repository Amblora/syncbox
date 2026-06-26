package main

import (
	_ "embed"
	"fyne.io/fyne/v2"
)

//go:embed resources/icon.png
var clientIconData []byte

// getIconData 返回客户端图标数据
func getIconData() []byte {
	return clientIconData
}

// getIconResource 返回 Fyne 图标资源
func getIconResource() *fyne.StaticResource {
	return &fyne.StaticResource{
		StaticName:    "syncbox-icon.png",
		StaticContent: clientIconData,
	}
}
