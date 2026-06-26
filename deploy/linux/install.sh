#!/bin/bash
# SyncBox Server Linux 安装脚本
# 用法: sudo bash install.sh

set -e

echo ""
echo "========================================"
echo "  SyncBox Server 安装程序"
echo "========================================"
echo ""

# 检查 root 权限
if [ "$(id -u)" -ne 0 ]; then
    echo "错误: 请使用 sudo 运行此脚本"
    exit 1
fi

INSTALL_DIR="/opt/syncbox"
DATA_DIR="/var/lib/syncbox/data"
SERVICE_FILE="/etc/systemd/system/syncbox-server.service"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# 创建用户
if ! id "syncbox" &>/dev/null; then
    echo "[1/5] 创建系统用户 syncbox ..."
    useradd --system --no-create-home --shell /usr/sbin/nologin syncbox
else
    echo "[1/5] 用户 syncbox 已存在"
fi

# 创建目录
echo "[2/5] 创建安装目录 ..."
mkdir -p "$INSTALL_DIR"
mkdir -p "$DATA_DIR"/{sync,db,temp,conflicts,trash,logs,backups}

# 复制文件
echo "[3/5] 复制服务端文件 ..."
cp "$SCRIPT_DIR/../server/linux-amd64/syncbox-server" "$INSTALL_DIR/"
cp "$SCRIPT_DIR/../web/index.html" "$INSTALL_DIR/web/" 2>/dev/null || true
cp -r "$SCRIPT_DIR/../web" "$INSTALL_DIR/" 2>/dev/null || true

# 设置权限
chown -R syncbox:syncbox "$INSTALL_DIR"
chown -R syncbox:syncbox "$DATA_DIR"
chmod 755 "$INSTALL_DIR/syncbox-server"

# 安装 systemd 服务
echo "[4/5] 安装 systemd 服务 ..."
cp "$SCRIPT_DIR/syncbox-server.service" "$SERVICE_FILE"
systemctl daemon-reload

# 启动服务
echo "[5/5] 启动服务 ..."
systemctl enable syncbox-server
systemctl start syncbox-server

echo ""
echo "========================================"
echo "  安装完成!"
echo "========================================"
echo ""
echo "  管理命令:"
echo "    systemctl start syncbox-server    # 启动"
echo "    systemctl stop syncbox-server     # 停止"
echo "    systemctl restart syncbox-server  # 重启"
echo "    systemctl status syncbox-server   # 状态"
echo "    journalctl -u syncbox-server -f   # 查看日志"
echo ""
echo "  安装位置: $INSTALL_DIR"
echo "  数据目录: $DATA_DIR"
echo "  服务端口: 8080"
echo ""
