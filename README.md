# SyncBox 文件同步系统

SyncBox 是一套自建的轻量级文件同步解决方案，采用类似 Seafile 的架构设计。由同步服务端（Linux/Windows）和桌面客户端（Windows/macOS/Linux）组成，支持多设备实时文件同步、Web 管理控制台、系统托盘常驻等功能。

---

## 项目结构

```
syncbox/
├── cmd/                          # 程序入口
│   ├── client/                   # 客户端入口
│   │   ├── main.go               # 客户端主程序（Fyne GUI）
│   │   ├── bundled.go            # 嵌入资源（图标）
│   │   └── resources/            # 客户端资源文件
│   │       └── icon.png          # 应用图标
│   └── server/                   # 服务端入口
│       └── main.go               # 服务端主程序
├── internal/                     # 核心业务模块
│   ├── api/                      # HTTP API 路由与处理器
│   │   └── api.go                # 所有 REST API 端点
│   ├── auth/                     # 认证与授权
│   │   └── auth.go               # JWT 令牌、设备认证中间件
│   ├── cleanup/                  # 磁盘清理引擎
│   │   └── cleanup.go            # 临时文件、旧日志、回收站清理
│   ├── client/                   # 客户端同步引擎
│   │   ├── client.go             # 文件同步核心逻辑
│   │   ├── hidewindow_windows.go # Windows 隐藏控制台窗口
│   │   └── hidewindow_other.go   # 非 Windows 平台兼容
│   ├── db/                       # 数据库层
│   │   └── db.go                 # SQLite 数据库操作（modernc.org/sqlite）
│   ├── monitor/                  # 系统资源监控
│   │   └── metrics.go            # CPU、内存、磁盘指标采集
│   ├── syncengine/               # 服务端文件监控
│   │   ├── watcher.go            # fsnotify 目录监控器
│   │   └── adapter.go            # 数据库接口适配器
│   └── ws/                       # WebSocket 通信
│       └── hub.go                # WebSocket 连接管理与消息广播
├── web/                          # Web 控制台前端
│   └── index.html                # 单文件 SPA（纯 HTML/CSS/JS）
├── assets/                       # 图标资源
│   ├── client.ico / client-icon.png / client-logo.svg
│   └── server.ico / server-icon.png / server-logo.svg
├── deploy/                       # 部署配置
│   ├── DEPLOY.md                 # 部署指南
│   └── linux/
│       ├── install.sh            # Linux 一键安装脚本
│       └── syncbox-server.service # systemd 服务文件
├── scripts/                      # 构建与辅助脚本
│   ├── build.py                  # Python 统一构建脚本
│   ├── build.ps1                 # PowerShell 构建脚本
│   └── build.sh                  # Bash 构建脚本
├── dist/                         # 构建产物
│   ├── server/
│   │   ├── linux-amd64/          # Linux 服务端
│   │   └── windows-amd64/        # Windows 服务端
│   └── client/
│       └── windows-amd64/        # Windows 客户端
├── data/                         # 运行时数据目录
│   ├── sync/                     # 同步文件存储
│   ├── db/                       # SQLite 数据库
│   ├── temp/                     # 临时文件
│   ├── conflicts/                # 冲突文件
│   ├── trash/                    # 回收站
│   ├── logs/                     # 日志文件
│   └── backups/                  # 备份
├── go.mod                        # Go 模块定义
├── go.sum                        # 依赖校验
└── README.md                     # 本文档
```

---

## 技术栈

| 组件 | 技术 |
|------|------|
| 服务端语言 | Go 1.24+ |
| 客户端 GUI | Fyne v2.7.4 |
| Web 框架 | Gin v1.12.0 |
| 数据库 | SQLite（modernc.org/sqlite，纯 Go 无 CGO） |
| 实时通信 | gorilla/websocket |
| 文件监控 | fsnotify v1.10.1 |
| 系统监控 | shirou/gopsutil/v3 |
| 认证 | golang-jwt/jwt/v5 |
| 前端 | 原生 HTML/CSS/JavaScript（单文件 SPA） |

---

## 功能特性

### 服务端
- **Web 管理控制台**：Fluent 风格深色主题，包含总览、文件管理、设备管理、Token 管理、冲突中心、资源监控、磁盘清理、回收站、设置等页面
- **文件同步 API**：RESTful 接口，支持上传、下载、删除、重命名、移动
- **设备管理**：接入令牌创建、设备注册、在线状态监控、吊销/删除
- **冲突处理**：多设备同时修改同一文件时自动检测并提供解决界面
- **磁盘清理**：临时文件清理、旧日志清理、回收站清理、深度扫描系统垃圾
- **用户头像**：控制台设置页支持上传自定义头像
- **系统监控**：CPU、内存、Swap、磁盘使用率实时图表
- **WebSocket 推送**：文件变更实时通知所有在线客户端

### 客户端
- **单实例保护**：同一台机器只允许运行一个客户端实例，重复启动自动打开已有窗口
- **连接向导**：引导式配置服务端地址、本地同步目录、接入令牌
- **服务端目录选择**：连接成功后可选择服务端的同步目录（类似 Seafile 库选择）
- **本地同步目录设置**：设置页可随时修改本地同步目录
- **开机自启动**：设置页一键开启/关闭开机自启
- **隐藏主界面启动**：配合开机自启，可选择启动时直接最小化到系统托盘
- **传输日志**：实时显示同步事件，自动滚动到最新日志
- **同步速度显示**：概览页实时显示上传/下载速率（↑xxxKB/S ↓xxxMB/S）
- **概览按钮自适应**：四个操作按钮根据窗口宽度自动调整布局
- **Fluent 风格界面**：卡片式布局、浅色主题、现代化 UI
- **系统托盘**：最小化到托盘，托盘菜单支持暂停/恢复/扫描/打开目录/退出

---

## 快速开始

### 1. 部署服务端

#### 方法一：使用安装脚本（推荐）

```bash
# 解压构建产物到服务器
# 运行安装脚本
sudo bash deploy/linux/install.sh
```

安装脚本会自动完成：
- 创建 `syncbox` 系统用户
- 安装到 `/opt/syncbox`
- 数据目录 `/var/lib/syncbox/data`
- 注册并启动 systemd 服务

#### 方法二：手动部署

```bash
# 创建系统用户
sudo useradd --system --no-create-home --shell /usr/sbin/nologin syncbox

# 创建目录
sudo mkdir -p /opt/syncbox
sudo mkdir -p /var/lib/syncbox/data/{sync,db,temp,conflicts,trash,logs,backups}

# 复制文件
sudo cp dist/server/linux-amd64/syncbox-server /opt/syncbox/
sudo cp -r web /opt/syncbox/

# 设置权限
sudo chown -R syncbox:syncbox /opt/syncbox
sudo chown -R syncbox:syncbox /var/lib/syncbox/data

# 安装 systemd 服务
sudo cp deploy/linux/syncbox-server.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable syncbox-server
sudo systemctl start syncbox-server
```

#### 方法三：直接运行（开发/测试）

```bash
cd dist/server/linux-amd64
./syncbox-server -addr :8080 -data ./data
```

### 2. 访问控制台

浏览器打开 `http://服务器IP:8080`，默认账号密码：
- 用户名：`admin`
- 密码：`admin123`

首次登录后建议立即修改密码。

### 3. 创建同步目录和接入令牌

1. 在控制台「Token管理」页面创建一个新的接入令牌
2. 记录令牌字符串，后续配置客户端需要使用

### 4. 安装客户端

#### Windows

双击 `dist/client/windows-amd64/syncbox-client.exe` 启动，或使用 `SyncBox-Client.vbs` 无控制台窗口启动。

#### Linux

```bash
# 安装 Fyne 依赖
sudo apt install libgl1-mesa-dev xorg-dev libxkbcommon-dev

# 运行
./dist/client/linux-amd64/syncbox-client
```

#### macOS

```bash
./dist/client/darwin-amd64/syncbox-client
```

### 5. 配置客户端连接

1. 启动客户端后进入「连接向导」页
2. 填写服务端地址（如 `http://192.168.1.100:8080`）
3. 选择本地同步目录
4. 填写接入令牌
5. 点击「保存并连接」
6. 连接成功后，可选择服务端的同步目录
7. 开始自动同步

---

## 系统管理

### systemd 管理命令

```bash
sudo systemctl start syncbox-server    # 启动
sudo systemctl stop syncbox-server     # 停止
sudo systemctl restart syncbox-server  # 重启
sudo systemctl status syncbox-server   # 查看状态
sudo journalctl -u syncbox-server -f   # 实时日志
sudo journalctl -u syncbox-server --since today  # 今日日志
```

### 服务端命令行参数

```
-addr :8080          监听地址（默认 :8080）
-data ./data         数据目录（默认 ./data）
-admin admin         管理员用户名（默认 admin）
-pass admin123       管理员密码（默认 admin123）
```

### 客户端配置文件

客户端配置保存在 `syncbox-client.json`（与可执行文件同目录）：

```json
{
  "server_url": "http://192.168.1.100:8080",
  "local_dir": "C:\Users\user\SyncBox",
  "device_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "token": "接入令牌",
  "device_name": "Client-DESKTOP-windows",
  "hide_on_start": false,
  "server_dir_id": "",
  "launch_on_login": false
}
```

---

## 构建

### 环境要求

- Go 1.24+
- Python 3.x（运行构建脚本）
- GCC/MinGW（客户端 CGO 编译，Windows 推荐 TDM-GCC）
- go-winres（Windows 资源嵌入）

### 使用 Python 构建脚本

```bash
# 构建全部（服务端全平台 + 当前平台客户端）
python scripts/build.py

# 仅构建服务端
python scripts/build.py server

# 仅构建客户端
python scripts/build.py client
```

### 使用 PowerShell 构建（Windows）

```powershell
.\scriptsuild.ps1 -Target all
.\scriptsuild.ps1 -Target server
.\scriptsuild.ps1 -Target client
```

### 使用 Bash 构建（Linux/macOS）

```bash
bash scripts/build.sh all
bash scripts/build.sh server
bash scripts/build.sh client
```

### 构建产物

```
dist/
├── server/
│   ├── linux-amd64/
│   │   ├── syncbox-server      # Linux 服务端
│   │   └── web/index.html      # Web 控制台
│   └── windows-amd64/
│       ├── syncbox-server.exe  # Windows 服务端
│       └── web/index.html
└── client/
    ├── windows-amd64/
    │   ├── syncbox-client.exe  # Windows 客户端
    │   └── SyncBox-Client.vbs  # 无控制台启动脚本
    ├── linux-amd64/
    │   └── syncbox-client      # Linux 客户端
    └── darwin-amd64/
        └── syncbox-client      # macOS 客户端
```

> **注意**：客户端 GUI 依赖 Fyne 框架，需要 CGO 支持。跨平台编译需要对应平台的 C 编译工具链。

---

## API 接口

### 客户端接口（需要设备认证）

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/client/register` | 设备注册 |
| POST | `/api/client/hello` | 心跳保活 |
| GET | `/api/client/events` | SSE 事件流 |
| PUT | `/api/client/upload` | 上传文件 |
| GET | `/api/client/download` | 下载文件 |
| POST | `/api/client/delete` | 删除文件 |
| POST | `/api/client/rename` | 重命名文件 |
| GET | `/api/client/sync_dirs` | 获取同步目录列表 |
| GET | `/api/client/files` | 获取文件列表 |
| GET | `/api/client/ws` | WebSocket 连接 |

### 管理接口（需要 JWT 认证）

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/admin/login` | 管理员登录 |
| GET | `/api/admin/overview` | 系统概览 |
| GET | `/api/admin/metrics` | 系统指标 |
| GET | `/api/admin/files` | 文件管理 |
| POST | `/api/admin/files/upload` | 上传文件 |
| GET | `/api/admin/files/download` | 下载文件 |
| POST | `/api/admin/files/delete` | 删除文件 |
| POST | `/api/admin/files/rename` | 重命名 |
| POST | `/api/admin/files/move` | 移动文件 |
| POST | `/api/admin/files/mkdir` | 创建目录 |
| GET | `/api/admin/devices` | 设备列表 |
| POST | `/api/admin/devices` | 创建设备 |
| POST | `/api/admin/devices/revoke` | 吊销设备 |
| POST | `/api/admin/devices/delete` | 删除设备 |
| GET | `/api/admin/tokens` | Token 列表 |
| POST | `/api/admin/tokens/create` | 创建 Token |
| POST | `/api/admin/tokens/delete` | 删除 Token |
| GET | `/api/admin/conflicts` | 冲突列表 |
| POST | `/api/admin/conflicts/resolve` | 解决冲突 |
| GET | `/api/admin/cleanup/scan` | 扫描可清理项 |
| POST | `/api/admin/cleanup/safe` | 安全清理 |
| POST | `/api/admin/cleanup/deep` | 深度清理 |
| GET | `/api/admin/logs` | 日志列表 |
| POST | `/api/admin/logs/delete` | 删除日志 |
| POST | `/api/admin/logs/clear` | 清空日志 |
| GET | `/api/admin/trash` | 回收站 |
| POST | `/api/admin/trash/restore` | 恢复文件 |
| POST | `/api/admin/trash/clear` | 清空回收站 |
| GET | `/api/admin/settings` | 获取设置 |
| POST | `/api/admin/settings` | 保存设置 |
| POST | `/api/admin/change-password` | 修改密码 |
| POST | `/api/admin/change-username` | 修改用户名 |
| POST | `/api/admin/avatar` | 上传头像 |
| GET | `/api/admin/avatar` | 获取头像 |

---

## 数据存储

所有数据存储在 `data/` 目录下：

| 目录 | 说明 |
|------|------|
| `data/sync/` | 同步文件实际存储 |
| `data/db/` | SQLite 数据库（文件元数据、事件日志、设备信息、设置） |
| `data/temp/` | 上传临时文件 |
| `data/conflicts/` | 冲突文件副本 |
| `data/trash/` | 已删除文件（回收站） |
| `data/logs/` | 运行日志 |
| `data/backups/` | 数据库备份 |

---

## 安全说明

- 管理控制台使用 JWT 令牌认证
- 客户端使用设备 ID + Token 双因子认证
- WebSocket 连接需要设备认证
- systemd 服务以最小权限运行（NoNewPrivileges、ProtectSystem）
- 建议在生产环境配合 Nginx 反向代理并启用 HTTPS
