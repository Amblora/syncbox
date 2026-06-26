package ws

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

const (
	// 写超时时间
	writeWait = 10 * time.Second

	// 读超时时间（等待 pong 消息）
	pongWait = 60 * time.Second

	// ping 发送间隔（必须小于 pongWait）
	pingPeriod = 30 * time.Second

	// 最大消息大小
	maxMessageSize = 1024 * 1024 // 1MB
)

// 事件类型常量
const (
	EventFileChange    = "file_change"   // 文件变更（上传/修改）
	EventFileDelete    = "file_delete"   // 文件删除
	EventFileRename    = "file_rename"   // 文件重命名
	EventConflict      = "conflict"      // 新冲突
	EventDeviceOnline  = "device_online" // 设备上线
	EventDeviceOffline = "device_offline" // 设备离线
)

// Message 表示 WebSocket 消息格式
type Message struct {
	Type string      `json:"type"`           // 事件类型
	Data interface{} `json:"data,omitempty"` // 事件数据
}

// Hub 管理所有 WebSocket 客户端连接
type Hub struct {
	clients    map[*Client]bool // 已连接的客户端
	broadcast  chan []byte       // 广播消息通道
	register   chan *Client      // 注册新客户端
	unregister chan *Client      // 注销客户端
	mu         sync.RWMutex     // 保护 clients 的并发访问
}

// Client 代表一个 WebSocket 客户端连接
type Client struct {
	hub      *Hub
	conn     *websocket.Conn
	send     chan []byte
	deviceID string
}

// upgrader 配置 HTTP 到 WebSocket 的升级器
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// 允许所有来源（生产环境应该配置允许的域名）
		return true
	},
}

// NewHub 创建 Hub 实例
func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

// Run 是 Hub 的主循环，处理 register/unregister/broadcast 事件
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			// 注册新客户端
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			log.Printf("[WebSocket Hub] 设备 %s 已连接，当前连接数: %d", client.deviceID, len(h.clients))

			// 广播设备上线事件
			h.BroadcastJSON(Message{
				Type: EventDeviceOnline,
				Data: map[string]string{"device_id": client.deviceID},
			})

		case client := <-h.unregister:
			// 注销客户端
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			h.mu.Unlock()
			log.Printf("[WebSocket Hub] 设备 %s 已断开，当前连接数: %d", client.deviceID, len(h.clients))

			// 广播设备离线事件
			h.BroadcastJSON(Message{
				Type: EventDeviceOffline,
				Data: map[string]string{"device_id": client.deviceID},
			})

		case message := <-h.broadcast:
			// 广播消息给所有客户端
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- message:
					// 消息已发送到客户端的发送缓冲区
				default:
					// 客户端发送缓冲区已满，关闭连接
					h.mu.RUnlock()
					h.mu.Lock()
					close(client.send)
					delete(h.clients, client)
					h.mu.Unlock()
					h.mu.RLock()
				}
			}
			h.mu.RUnlock()
		}
	}
}

// BroadcastJSON 序列化消息并广播给所有客户端
func (h *Hub) BroadcastJSON(v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		log.Printf("[WebSocket Hub] 消息序列化失败: %v", err)
		return
	}
	h.broadcast <- data
}

// BroadcastToOthers 广播消息给除发送者外的所有客户端
func (h *Hub) BroadcastToOthers(senderDeviceID string, v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		log.Printf("[WebSocket Hub] 消息序列化失败: %v", err)
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	for client := range h.clients {
		// 跳过发送者
		if client.deviceID == senderDeviceID {
			continue
		}
		select {
		case client.send <- data:
			// 消息已发送到客户端的发送缓冲区
		default:
			// 客户端发送缓冲区已满，关闭连接
			h.mu.RUnlock()
			h.mu.Lock()
			close(client.send)
			delete(h.clients, client)
			h.mu.Unlock()
			h.mu.RLock()
		}
	}
}

// HandleWebSocket 升级 HTTP 连接为 WebSocket，注册客户端到 Hub
func (h *Hub) HandleWebSocket(c *gin.Context, deviceID string) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("[WebSocket Hub] WebSocket 升级失败: %v", err)
		return
	}

	// 创建客户端
	client := &Client{
		hub:      h,
		conn:     conn,
		send:     make(chan []byte, 256),
		deviceID: deviceID,
	}

	// 注册客户端到 Hub
	h.register <- client

	// 启动读写协程
	go client.writePump()
	go client.readPump()
}

// readPump 处理客户端发送的消息，包括心跳 pong
func (c *Client) readPump() {
	defer func() {
		// 读取结束后注销客户端
		c.hub.unregister <- c
		c.conn.Close()
	}()

	// 设置读取消息的最大大小
	c.conn.SetReadLimit(maxMessageSize)

	// 设置读取超时
	c.conn.SetReadDeadline(time.Now().Add(pongWait))

	// 设置 pong 处理器（收到 pong 时重置读取超时）
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	// 设置关闭处理器
	c.conn.SetCloseHandler(func(code int, text string) error {
		log.Printf("[WebSocket] 设备 %s 关闭连接: %d %s", c.deviceID, code, text)
		return nil
	})

	for {
		// 读取消息
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("[WebSocket] 设备 %s 读取错误: %v", c.deviceID, err)
			}
			break
		}

		// 处理客户端消息（目前只支持心跳 ping）
		var msg Message
		if err := json.Unmarshal(message, &msg); err != nil {
			log.Printf("[WebSocket] 设备 %s 消息解析失败: %v", c.deviceID, err)
			continue
		}

		// 如果是 ping 消息，回复 pong
		if msg.Type == "ping" {
			pong := Message{Type: "pong"}
			pongData, _ := json.Marshal(pong)
			select {
			case c.send <- pongData:
			default:
			}
		}

		// 可以在这里扩展处理其他客户端消息类型
		log.Printf("[WebSocket] 收到设备 %s 的消息: %s", c.deviceID, msg.Type)
	}
}

// writePump 向客户端发送消息，并维护心跳 ping
func (c *Client) writePump() {
	// 创建定时器，定期发送 ping
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			// 设置写入超时
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))

			if !ok {
				// Hub 已关闭该客户端的发送通道，发送关闭消息
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			// 获取下一个消息类型
			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}

			// 写入消息
			w.Write(message)

			// 批量发送缓冲区中的消息
			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write([]byte("\n"))
				w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			// 定期发送 ping 心跳
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// GetOnlineDevices 返回当前在线设备列表
func (h *Hub) GetOnlineDevices() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	devices := make([]string, 0, len(h.clients))
	for client := range h.clients {
		devices = append(devices, client.deviceID)
	}
	return devices
}

// GetClientCount 返回当前连接的客户端数量
func (h *Hub) GetClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// BroadcastTo 广播消息给指定设备
func (h *Hub) BroadcastTo(deviceID string, v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		log.Printf("[WebSocket Hub] 消息序列化失败: %v", err)
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	for client := range h.clients {
		if client.deviceID == deviceID {
			select {
			case client.send <- data:
			default:
				// 客户端发送缓冲区已满
				log.Printf("[WebSocket Hub] 设备 %s 发送缓冲区已满", deviceID)
			}
			return
		}
	}

	log.Printf("[WebSocket Hub] 设备 %s 未找到", deviceID)
}
