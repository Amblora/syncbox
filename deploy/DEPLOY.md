# SyncBox 部署指南

## 服务端部署 (Linux)

### 方法一: 使用安装脚本
```bash
# 1. 解压构建产物到目标机器
# 2. 运行安装脚本
sudo bash deploy/linux/install.sh

amblora/amblora@sync.com
```

### 方法二: 手动部署
```bash
# 1. 创建系统用户
sudo useradd --system --no-create-home --shell /usr/sbin/nologin syncbox

# 2. 创建目录
sudo mkdir -p /opt/syncbox
sudo mkdir -p /var/lib/syncbox/data/{sync,db,temp,conflicts,trash,logs,backups}

# 3. 复制文件
sudo cp dist/server/linux-amd64/syncbox-server /opt/syncbox/
sudo cp -r web /opt/syncbox/

# 4. 安装 systemd 服务
sudo cp deploy/linux/syncbox-server.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable syncbox-server
sudo systemctl start syncbox-server

# 5. 查看状态
sudo systemctl status syncbox-server
sudo journalctl -u syncbox-server -f
```

### systemd 管理命令
```bash
sudo systemctl start syncbox-server    # 启动
sudo systemctl stop syncbox-server     # 停止
sudo systemctl restart syncbox-server  # 重启
sudo systemctl status syncbox-server   # 查看状态
sudo journalctl -u syncbox-server -f   # 实时日志
sudo journalctl -u syncbox-server --since today  # 今日日志
```

## 客户端部署

### Windows
1. 双击 `SyncBox-Client.vbs` 启动（无控制台窗口）
2. 或直接运行 `syncbox-client.exe`（会有控制台窗口）
3. 客户端启动后自动最小化到系统托盘

### Linux
```bash
# 安装 Fyne 依赖
sudo apt install libgl1-mesa-dev xorg-dev libxkbcommon-dev

# 运行
./dist/client/linux-amd64/syncbox-client
```

### macOS
```bash
./dist/client/darwin-amd64/syncbox-client
```

## 构建

### Windows (PowerShell)
```powershell
.\scripts\build.ps1 -Target all    # 构建全部
.\scripts\build.ps1 -Target server # 仅构建服务端
.\scripts\build.ps1 -Target client # 仅构建客户端
```

### Linux/macOS (Bash)
```bash
bash scripts/build.sh all
bash scripts/build.sh server
bash scripts/build.sh client
```
