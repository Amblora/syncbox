package db

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
	"golang.org/x/crypto/bcrypt"
)

// DB 灏佽 SQLite 数据库撹繛鎺ュ拰鎿嶄綔鏂规硶
type DB struct {
	conn *sql.DB
}
// Device 设备信息
type Device struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	TokenHash string `json:"-"`
	Platform  string `json:"platform"`
	LastSeen  int64  `json:"last_seen"`
	IP       string `json:"ip"`
	LastSeq   int64  `json:"last_seq"`
	Revoked   bool   `json:"revoked"`
	CreatedAt int64  `json:"created_at"`
}
// File 文件鍏冩暟鎹?
type File struct {
	ID        string `json:"id"`
	RelPath   string `json:"rel_path"`
	PathKey   string `json:"path_key"`
	Type      string `json:"type"`
	Version   int64  `json:"version"`
	Seq       int64  `json:"seq"`
	Size      int64  `json:"size"`
	Hash      string `json:"hash"`
	MtimeNs   int64  `json:"mtime_ns"`
	Deleted   bool   `json:"deleted"`
	UpdatedAt int64  `json:"updated_at"`
}
// SyncEvent 鍚屾浜嬩欢
type SyncEvent struct {
	Seq       int64  `json:"seq"`
	EventType string `json:"event_type"`
	RelPath   string `json:"rel_path"`
	OldPath   string `json:"old_path"`
	Version   int64  `json:"version"`
	DeviceID  string `json:"device_id"`
	CreatedAt int64  `json:"created_at"`
}
// Conflict 鍐茬獊璁板綍
type Conflict struct {
	ID           int64  `json:"id"`
	RelPath      string `json:"rel_path"`
	ServerPath   string `json:"server_path"`
	ConflictPath string `json:"conflict_path"`
	DeviceID     string `json:"device_id"`
	Reason       string `json:"reason"`
	Resolved     bool   `json:"resolved"`
	CreatedAt    int64  `json:"created_at"`
}
// ActivityLog 娲诲姩鏃ュ織
type ActivityLog struct {
	ID        int64  `json:"id"`
	Timestamp int64  `json:"timestamp"`
	DeviceID  string `json:"device_id"`
	Action    string `json:"action"`
	RelPath   string `json:"rel_path"`
	Detail    string `json:"detail"`
}
// Stats 绯荤粺统计信息
type Stats struct {
	TotalFiles          int64 `json:"total_files"`
	DeletedFiles        int64 `json:"deleted_files"`
	TodayEvents         int64 `json:"today_events"`
	TotalDevices        int64 `json:"total_devices"`
	ActiveDevices       int64 `json:"active_devices"`
	TotalConflicts      int64 `json:"total_conflicts"`
	UnresolvedConflicts int64 `json:"unresolved_conflicts"`
}
// NewDB 打开并初始化 SQLite 数据库?
func NewDB(path string) (*DB, error) {
	conn, err := sql.Open("sqlite", path+"?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000&_foreign_keys=ON&_temp_store=MEMORY")
	if err != nil {
		return nil, fmt.Errorf("打开数据库失败? %w", err)
	}
// 设置杩炴帴姹犲弬鏁?	conn.SetMaxOpenConns(1) // SQLite 鍗曞啓
	conn.SetMaxIdleConns(1)
	conn.SetConnMaxLifetime(0)

	// 鎵ц PRAGMA 设置
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA foreign_keys=ON",
		"PRAGMA temp_store=MEMORY",
	}
	for _, p := range pragmas {
		if _, err := conn.Exec(p); err != nil {
			conn.Close()
			return nil, fmt.Errorf("鎵ц PRAGMA 失败: %w", err)
		}
	}

	db := &DB{conn: conn}
	if err := db.createTables(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("创建表失败? %w", err)
	}

	return db, nil
}
// createTables 创建鎵€鏈夊繀瑕佺殑鏁版嵁琛?
func (db *DB) createTables() error {
	queries := []string{
		// 管理员樿〃
		`CREATE TABLE IF NOT EXISTS admin_user (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			username TEXT NOT NULL UNIQUE,
			password_hash TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		)`,
		// 设备琛?
`CREATE TABLE IF NOT EXISTS devices (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			token_hash TEXT NOT NULL,
			platform TEXT NOT NULL DEFAULT '',
			last_seen INTEGER NOT NULL DEFAULT 0,
		ip TEXT NOT NULL DEFAULT "",
			last_seq INTEGER NOT NULL DEFAULT 0,
			revoked INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL
		)`,
		// 文件鍏冩暟鎹〃
		`CREATE TABLE IF NOT EXISTS files (
			id TEXT PRIMARY KEY,
			rel_path TEXT NOT NULL,
			path_key TEXT NOT NULL UNIQUE,
			type TEXT NOT NULL DEFAULT '',
			version INTEGER NOT NULL DEFAULT 1,
			seq INTEGER NOT NULL DEFAULT 0,
			size INTEGER NOT NULL DEFAULT 0,
			hash TEXT NOT NULL DEFAULT '',
			mtime_ns INTEGER NOT NULL DEFAULT 0,
			deleted INTEGER NOT NULL DEFAULT 0,
			updated_at INTEGER NOT NULL
		)`,
		// 鍚屾浜嬩欢琛?
`CREATE TABLE IF NOT EXISTS sync_events (
			seq INTEGER PRIMARY KEY AUTOINCREMENT,
			event_type TEXT NOT NULL,
			rel_path TEXT NOT NULL,
			old_path TEXT NOT NULL DEFAULT '',
			version INTEGER NOT NULL DEFAULT 0,
			device_id TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL
		)`,
		// 鍐茬獊琛?
`CREATE TABLE IF NOT EXISTS conflicts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			rel_path TEXT NOT NULL,
			server_path TEXT NOT NULL DEFAULT '',
			conflict_path TEXT NOT NULL DEFAULT '',
			device_id TEXT NOT NULL DEFAULT '',
			reason TEXT NOT NULL DEFAULT '',
			resolved INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL
		)`,
		// 娲诲姩鏃ュ織琛?
`CREATE TABLE IF NOT EXISTS activity_logs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp INTEGER NOT NULL,
			device_id TEXT NOT NULL DEFAULT '',
			action TEXT NOT NULL,
			rel_path TEXT NOT NULL DEFAULT '',
			detail TEXT NOT NULL DEFAULT ''
		)`,
		// 设置琛?
`CREATE TABLE IF NOT EXISTS settings (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL DEFAULT ''
		)`,
		// 绱㈠紩
		// 设备Token表
		`CREATE TABLE IF NOT EXISTS device_tokens (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			token TEXT NOT NULL UNIQUE,
			device_id TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL,
			revoked INTEGER NOT NULL DEFAULT 0
		)`,
		// 索引
		`CREATE INDEX IF NOT EXISTS idx_files_path_key ON files(path_key)`,
		`CREATE INDEX IF NOT EXISTS idx_files_deleted ON files(deleted)`,
		`CREATE INDEX IF NOT EXISTS idx_sync_events_created_at ON sync_events(created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_activity_logs_timestamp ON activity_logs(timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_devices_revoked ON devices(revoked)`,
	}

	for _, q := range queries {
		if _, err := db.conn.Exec(q); err != nil {
			return fmt.Errorf("鎵ц寤鸿〃璇彞失败: %w\nSQL: %s", err, q)
		}
	}
	return nil
}
// Close 关闭数据库连接?
func (db *DB) Close() error {
	return db.conn.Close()
}
// Checkpoint 鎵ц WAL 妫€鏌ョ偣锛屾竻鐞?WAL 文件
func (db *DB) Checkpoint() error {
	_, err := db.conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	return err
}
// Optimize 浼樺寲数据库?
func (db *DB) Optimize() error {
	_, err := db.conn.Exec("PRAGMA optimize")
	return err
}
// ========== 管理员樻搷浣?==========

// InitAdmin 鍒濆鍖栫鐞嗗憳璐︽埛锛屼粎鍦?admin_user 琛ㄤ负绌烘椂插入
// InitAdmin 初始化管理员账户
// 如果管理员不存在则创建，如果已存在则更新密码（确保每次启动都同步最新密码）
func (db *DB) InitAdmin(username, password string) error {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("生成密码哈希失败: %w", err)
	}

	var count int
	err = db.conn.QueryRow("SELECT COUNT(*) FROM admin_user").Scan(&count)
	if err != nil {
		return fmt.Errorf("查询管理员数量失败: %w", err)
	}

	now := time.Now().UnixMilli()
	if count == 0 {
		// 首次启动，创建管理员
		_, err = db.conn.Exec(
			"INSERT INTO admin_user (id, username, password_hash, created_at, updated_at) VALUES (1, ?, ?, ?, ?)",
			username, string(hash), now, now,
		)
		if err != nil {
			return fmt.Errorf("插入管理员失败: %w", err)
		}
	} else {
		// 管理员已存在，更新用户名和密码（确保命令行参数生效）
		_, err = db.conn.Exec(
			"UPDATE admin_user SET username = ?, password_hash = ?, updated_at = ? WHERE id = 1",
			username, string(hash), now,
		)
		if err != nil {
			return fmt.Errorf("更新管理员失败: %w", err)
		}
	}
	return nil
}
// LoginAdmin 验证管理员樼敤鎴峰悕鍜屽瘑鐮?
func (db *DB) LoginAdmin(username, password string) bool {
	var hash string
	err := db.conn.QueryRow(
		"SELECT password_hash FROM admin_user WHERE username = ?", username,
	).Scan(&hash)
	if err != nil {
		return false
	}
	compareErr := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	if compareErr != nil {
	}
	return compareErr == nil
}
// ========== 设备鎿嶄綔 ==========

// CreateDevice 创建鏂拌澶囷紝杩斿洖设备信息
func (db *DB) CreateDevice(name, token, platform string) (*Device, error) {
	id := GenerateDeviceID()
	tokenHash := hashToken(token)
	now := time.Now().UnixMilli()

	_, err := db.conn.Exec(
		`INSERT INTO devices (id, name, token_hash, platform, last_seen, last_seq, revoked, created_at)
		 VALUES (?, ?, ?, ?, ?, 0, 0, ?)`,
		id, name, tokenHash, platform, now, now,
	)
	if err != nil {
		return nil, fmt.Errorf("创建设备失败: %w", err)
	}

	return &Device{
		ID:        id,
		Name:      name,
		TokenHash: tokenHash,
		Platform:  platform,
		LastSeen:  now,
		LastSeq:   0,
		Revoked:   false,
		CreatedAt: now,
	}, nil
}
// GetDevice 鏍规嵁 ID 获取设备信息
func (db *DB) GetDevice(id string) (*Device, error) {
	d := &Device{}
	var revoked int
	err := db.conn.QueryRow(
		`SELECT id, name, token_hash, platform, last_seen, last_seq, revoked, created_at, COALESCE(ip, "") as ip
		 FROM devices WHERE id = ?`, id,
	).Scan(&d.ID, &d.Name, &d.TokenHash, &d.Platform, &d.LastSeen, &d.LastSeq, &revoked, &d.CreatedAt, &d.IP)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("获取设备失败: %w", err)
	}
	d.Revoked = revoked != 0
	return d, nil
}
// ListDevices 鍒楀嚭鎵€鏈夎澶?
func (db *DB) ListDevices() ([]*Device, error) {
	rows, err := db.conn.Query(
		`SELECT id, name, token_hash, platform, last_seen, last_seq, revoked, created_at, COALESCE(ip, "") as ip
		 FROM devices ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, fmt.Errorf("查询设备鍒楄〃失败: %w", err)
	}
	defer rows.Close()

	var devices []*Device
	for rows.Next() {
		d := &Device{}
		var revoked int
		if err := rows.Scan(&d.ID, &d.Name, &d.TokenHash, &d.Platform, &d.LastSeen, &d.LastSeq, &revoked, &d.CreatedAt, &d.IP); err != nil {
			return nil, fmt.Errorf("鎵弿设备琛屽け璐? %w", err)
		}
		d.Revoked = revoked != 0
		devices = append(devices, d)
	}
	return devices, nil
}
// UpdateDeviceLastSeen 更新设备最后在线挎椂闂村拰搴忓垪鍙?
func (db *DB) UpdateDeviceLastSeen(id string, seq int64) error {
	now := time.Now().UnixMilli()
	_, err := db.conn.Exec(
		"UPDATE devices SET last_seen = ?, last_seq = ? WHERE id = ?",
		now, seq, id,
	)
	return err
}
// RevokeDevice 撤销设备锛堟爣璁颁负已撤销锛?
// UpdateDeviceIP 更新设备IP地址
func (db *DB) UpdateDeviceIP(id, ip string) error {
	_, err := db.conn.Exec("UPDATE devices SET ip = ? WHERE id = ?", ip, id)
	return err
}

func (db *DB) RevokeDevice(id string) error {
	_, err := db.conn.Exec("UPDATE devices SET revoked = 1 WHERE id = ?", id)
	return err
}

// DeleteDevice 从数据库彻底删除设备
func (db *DB) DeleteDevice(id string) error {
	_, err := db.conn.Exec("DELETE FROM devices WHERE id = ?", id)
	return err
}
// ========== 文件鎿嶄綔 ==========

// UpsertFile 插入鎴栨洿鏂版枃浠跺厓鏁版嵁
func (db *DB) UpsertFile(f *File) error {
	now := time.Now().UnixMilli()
	f.UpdatedAt = now

	_, err := db.conn.Exec(
		`INSERT INTO files (id, rel_path, path_key, type, version, seq, size, hash, mtime_ns, deleted, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(path_key) DO UPDATE SET
			id = excluded.id,
			rel_path = excluded.rel_path,
			type = excluded.type,
			version = excluded.version,
			seq = excluded.seq,
			size = excluded.size,
			hash = excluded.hash,
			mtime_ns = excluded.mtime_ns,
			deleted = excluded.deleted,
			updated_at = excluded.updated_at`,
		f.ID, f.RelPath, f.PathKey, f.Type, f.Version, f.Seq, f.Size, f.Hash, f.MtimeNs, boolToInt(f.Deleted), f.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("写入文件鍏冩暟鎹け璐? %w", err)
	}
	return nil
}
// GetFile 鏍规嵁璺緞閿幏鍙栨枃浠朵俊鎭?
func (db *DB) GetFile(pathKey string) (*File, error) {
	f := &File{}
	var deleted int
	err := db.conn.QueryRow(
		`SELECT id, rel_path, path_key, type, version, seq, size, hash, mtime_ns, deleted, updated_at
		 FROM files WHERE path_key = ?`, pathKey,
	).Scan(&f.ID, &f.RelPath, &f.PathKey, &f.Type, &f.Version, &f.Seq, &f.Size, &f.Hash, &f.MtimeNs, &deleted, &f.UpdatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("获取文件失败: %w", err)
	}
	f.Deleted = deleted != 0
	return f, nil
}
// ListFiles 鍒楀嚭鎵€鏈夋枃浠讹紙鍙€夋槸鍚﹀寘鍚凡删除鐨勶級
func (db *DB) ListFiles(includeDeleted bool) ([]*File, error) {
	query := `SELECT id, rel_path, path_key, type, version, seq, size, hash, mtime_ns, deleted, updated_at FROM files`
	if !includeDeleted {
		query += " WHERE deleted = 0"
	}
	query += " ORDER BY rel_path"

	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("查询文件鍒楄〃失败: %w", err)
	}
	defer rows.Close()

	var files []*File
	for rows.Next() {
		f := &File{}
		var deleted int
		if err := rows.Scan(&f.ID, &f.RelPath, &f.PathKey, &f.Type, &f.Version, &f.Seq, &f.Size, &f.Hash, &f.MtimeNs, &deleted, &f.UpdatedAt); err != nil {
			return nil, fmt.Errorf("鎵弿文件琛屽け璐? %w", err)
		}
		f.Deleted = deleted != 0
		files = append(files, f)
	}
	return files, nil
}
// DeleteFile 鏍囪文件涓哄凡删除锛坱ombstone 妯″紡锛?
// DeleteFilesByPrefix 批量标记以指定前缀开头的文件为已删除
func (db *DB) DeleteFilesByPrefix(prefix string) error {
	_, err := db.conn.Exec("UPDATE files SET deleted = 1, updated_at = ? WHERE path_key LIKE ?", time.Now().UnixMilli(), prefix+"%")
	return err
}

func (db *DB) DeleteFile(pathKey string) error {
	now := time.Now().UnixMilli()
	result, err := db.conn.Exec(
		"UPDATE files SET deleted = 1, updated_at = ? WHERE path_key = ?",
		now, pathKey,
	)
	if err != nil {
		return fmt.Errorf("删除文件失败: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("文件涓嶅瓨鍦? %s", pathKey)
	}
	return nil
}
// RenameFile 閲嶅懡鍚嶆枃浠讹紙更新 rel_path 鍜?path_key锛?
func (db *DB) RenameFile(oldPathKey, newRelPath, newPathKey string) error {
	now := time.Now().UnixMilli()
	result, err := db.conn.Exec(
		"UPDATE files SET rel_path = ?, path_key = ?, updated_at = ? WHERE path_key = ?",
		newRelPath, newPathKey, now, oldPathKey,
	)
	if err != nil {
		return fmt.Errorf("閲嶅懡鍚嶆枃浠跺け璐? %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("文件涓嶅瓨鍦? %s", oldPathKey)
	}
	return nil
}
// ========== 鍚屾浜嬩欢鎿嶄綔 ==========

// AddEvent 娣诲姞鍚屾浜嬩欢锛岃繑鍥炶嚜澧炲簭鍒楀彿
func (db *DB) AddEvent(eventType, relPath, oldPath string, version int64, deviceID string) (int64, error) {
	now := time.Now().UnixMilli()
	result, err := db.conn.Exec(
		`INSERT INTO sync_events (event_type, rel_path, old_path, version, device_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		eventType, relPath, oldPath, version, deviceID, now,
	)
	if err != nil {
		return 0, fmt.Errorf("娣诲姞鍚屾浜嬩欢失败: %w", err)
	}
	return result.LastInsertId()
}
// GetEventsAfterSeq 获取鎸囧畾搴忓垪鍙蜂箣鍚庣殑鎵€鏈夊悓姝ヤ簨浠?
func (db *DB) GetEventsAfterSeq(afterSeq int64) ([]SyncEvent, error) {
	rows, err := db.conn.Query(
		`SELECT seq, event_type, rel_path, old_path, version, device_id, created_at
		 FROM sync_events WHERE seq > ? ORDER BY seq ASC`, afterSeq,
	)
	if err != nil {
		return nil, fmt.Errorf("查询鍚屾浜嬩欢失败: %w", err)
	}
	defer rows.Close()

	var events []SyncEvent
	for rows.Next() {
		var e SyncEvent
		if err := rows.Scan(&e.Seq, &e.EventType, &e.RelPath, &e.OldPath, &e.Version, &e.DeviceID, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("鎵弿鍚屾浜嬩欢琛屽け璐? %w", err)
		}
		events = append(events, e)
	}
	return events, nil
}
// GetLastSeq 获取鏈€鏂扮殑鍚屾浜嬩欢搴忓垪鍙?
func (db *DB) GetLastSeq() int64 {
	var seq sql.NullInt64
	err := db.conn.QueryRow("SELECT MAX(seq) FROM sync_events").Scan(&seq)
	if err != nil || !seq.Valid {
		return 0
	}
	return seq.Int64
}
// ========== 鍐茬獊鎿嶄綔 ==========

// CreateConflict 创建鍐茬獊璁板綍
func (db *DB) CreateConflict(relPath, serverPath, conflictPath, deviceID, reason string) (int64, error) {
	now := time.Now().UnixMilli()
	result, err := db.conn.Exec(
		`INSERT INTO conflicts (rel_path, server_path, conflict_path, device_id, reason, resolved, created_at)
		 VALUES (?, ?, ?, ?, ?, 0, ?)`,
		relPath, serverPath, conflictPath, deviceID, reason, now,
	)
	if err != nil {
		return 0, fmt.Errorf("创建鍐茬獊璁板綍失败: %w", err)
	}
	return result.LastInsertId()
}
// ListConflicts 鍒楀嚭鎵€鏈夊啿绐佽褰曪紝鍙€変粎杩斿洖鏈В鍐崇殑
func (db *DB) ListConflicts(unresolvedOnly bool) ([]*Conflict, error) {
	query := `SELECT id, rel_path, server_path, conflict_path, device_id, reason, resolved, created_at FROM conflicts`
	if unresolvedOnly {
		query += " WHERE resolved = 0"
	}
	query += " ORDER BY created_at DESC"

	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("查询鍐茬獊璁板綍失败: %w", err)
	}
	defer rows.Close()

	var conflicts []*Conflict
	for rows.Next() {
		c := &Conflict{}
		var resolved int
		if err := rows.Scan(&c.ID, &c.RelPath, &c.ServerPath, &c.ConflictPath, &c.DeviceID, &c.Reason, &resolved, &c.CreatedAt); err != nil {
			return nil, fmt.Errorf("鎵弿鍐茬獊璁板綍琛屽け璐? %w", err)
		}
		c.Resolved = resolved != 0
		conflicts = append(conflicts, c)
	}
	return conflicts, nil
}
// ResolveConflict 鏍囪鍐茬獊涓哄凡瑙ｅ喅
func (db *DB) ResolveConflict(id int64) error {
	_, err := db.conn.Exec("UPDATE conflicts SET resolved = 1 WHERE id = ?", id)
	return err
}
// ========== 娲诲姩鏃ュ織鎿嶄綔 ==========

// AddActivityLog 娣诲姞娲诲姩鏃ュ織
func (db *DB) AddActivityLog(deviceID, action, relPath, detail string) error {
	now := time.Now().UnixMilli()
	_, err := db.conn.Exec(
		`INSERT INTO activity_logs (timestamp, device_id, action, rel_path, detail)
		 VALUES (?, ?, ?, ?, ?)`,
		now, deviceID, action, relPath, detail,
	)
	return err
}
// ListActivityLogs 鍒楀嚭鏈€杩戠殑娲诲姩鏃ュ織
func (db *DB) ListActivityLogs(limit int) ([]*ActivityLog, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := db.conn.Query(
		`SELECT id, timestamp, device_id, action, rel_path, detail
		 FROM activity_logs ORDER BY id DESC LIMIT ?`, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("查询娲诲姩鏃ュ織失败: %w", err)
	}
	defer rows.Close()

	var logs []*ActivityLog
	for rows.Next() {
		l := &ActivityLog{}
		if err := rows.Scan(&l.ID, &l.Timestamp, &l.DeviceID, &l.Action, &l.RelPath, &l.Detail); err != nil {
			return nil, fmt.Errorf("鎵弿娲诲姩鏃ュ織琛屽け璐? %w", err)
		}
		logs = append(logs, l)
	}
	return logs, nil
}
// ========== 设置鎿嶄綔 ==========

// autoMigrate 自动迁移数据库结构，为旧数据库添加缺失的字段
func (db *DB) autoMigrate() {
	// 检查 devices 表是否有 ip 字段，没有则添加
	var hasIP bool
	rows, _ := db.conn.Query("PRAGMA table_info(devices)")
	if rows != nil {
		defer rows.Close()
		for rows.Next() {
			var cid int
			var name, ctype string
			var dflt interface{}
			var notnull int
			var pk int
			rows.Scan(&cid, &name, &ctype, &dflt, &notnull, &pk)
			if name == "ip" {
				hasIP = true
				break
			}
		}
	}
	if !hasIP {
		db.conn.Exec("ALTER TABLE devices ADD COLUMN ip TEXT NOT NULL DEFAULT ''")
	}
}

// GetSetting 获取设置鍊?
func (db *DB) GetSetting(key string) (string, error) {
	var value string
	err := db.conn.QueryRow("SELECT value FROM settings WHERE key = ?", key).Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", fmt.Errorf("获取设置失败: %w", err)
	}
	return value, nil
}
// SetSetting 设置閿€煎锛堝瓨鍦ㄥ垯更新锛屼笉瀛樺湪鍒欐彃鍏ワ級
func (db *DB) SetSetting(key, value string) error {
	_, err := db.conn.Exec(
		`INSERT INTO settings (key, value) VALUES (?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		key, value,
	)
	return err
}
// ========== 统计 ==========

// GetStats 获取绯荤粺统计信息
func (db *DB) GetStats() (*Stats, error) {
	s := &Stats{}
// 文件总数（不含已删除）
	_ = db.conn.QueryRow("SELECT COUNT(*) FROM files WHERE deleted = 0").Scan(&s.TotalFiles)
	// 宸插垹闄ゆ枃浠舵暟
	_ = db.conn.QueryRow("SELECT COUNT(*) FROM files WHERE deleted = 1").Scan(&s.DeletedFiles)

	// 今日事件数
	todayStart := time.Now().Truncate(24 * time.Hour).UnixMilli()
	_ = db.conn.QueryRow("SELECT COUNT(*) FROM sync_events WHERE created_at >= ?", todayStart).Scan(&s.TodayEvents)

	// 设备统计
	_ = db.conn.QueryRow("SELECT COUNT(*) FROM devices").Scan(&s.TotalDevices)
	_ = db.conn.QueryRow("SELECT COUNT(*) FROM devices WHERE revoked = 0").Scan(&s.ActiveDevices)

	// 鍐茬獊统计
	_ = db.conn.QueryRow("SELECT COUNT(*) FROM conflicts").Scan(&s.TotalConflicts)
	_ = db.conn.QueryRow("SELECT COUNT(*) FROM conflicts WHERE resolved = 0").Scan(&s.UnresolvedConflicts)

	return s, nil
}
// ========== 宸ュ叿鍑芥暟 ==========

// GenerateDeviceID 鐢熸垚闅忔満设备 ID锛?6 瀛楄妭 hex锛?
func GenerateDeviceID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
// GenerateToken 鐢熸垚闅忔満设备 Token锛?2 瀛楄妭 hex锛?
func GenerateToken() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
// hashToken 浣跨敤 SHA256 瀵?token 杩涜鍝堝笇
func hashToken(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}
// VerifyDeviceToken 验证设备 token 鏄惁鍖归厤涓旇澶囨湭琚挙閿€
func (db *DB) VerifyDeviceToken(id, token string) bool {
	var tokenHash string
	var revoked int
	err := db.conn.QueryRow(
		"SELECT token_hash, revoked FROM devices WHERE id = ?", id,
	).Scan(&tokenHash, &revoked)
	if err != nil {
		return false
	}
	if revoked != 0 {
		return false
	}
	return hashToken(token) == tokenHash
}
// boolToInt 灏?bool 杞负 int锛圫QLite 涓嶅師鐢熸敮鎸?bool锛?
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
// GetResolvedConflicts 杩斿洖宸茶В鍐充笖鍦ㄦ寚瀹氭椂闂翠箣鍓嶇殑鍐茬獊文件璺緞
func (db *DB) GetResolvedConflicts(before time.Time) ([]string, error) {
	rows, err := db.conn.Query("SELECT conflict_path FROM conflicts WHERE resolved = 1 AND created_at < ?", before.Unix())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var paths []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err == nil {
			paths = append(paths, p)
		}
	}
	return paths, nil
}
// GetOrphanFileKeys 杩斿洖数据库撲腑宸叉爣璁板垹闄ょ殑文件閿垪琛?
func (db *DB) GetOrphanFileKeys() ([]string, error) {
	rows, err := db.conn.Query("SELECT path_key FROM files WHERE deleted = 1")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err == nil {
			keys = append(keys, k)
		}
	}
	return keys, nil
}
// GetDeviceByName 鏍规嵁设备鍚嶇О鏌ユ壘设备
func (db *DB) GetDeviceByName(name string) (*Device, error) {
	row := db.conn.QueryRow("SELECT id, name, token_hash, platform, last_seen, last_seq, revoked, created_at, COALESCE(ip, \"\"\") as ip FROM devices WHERE name = ?", name)
	d := &Device{}
	var revoked int
	err := row.Scan(&d.ID, &d.Name, &d.TokenHash, &d.Platform, &d.LastSeen, &d.LastSeq, &revoked, &d.CreatedAt, &d.IP)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return nil, nil
		}
		return nil, err
	}
	d.Revoked = revoked == 1
	return d, nil
}
// ChangeAdminPassword 淇敼管理员樺瘑鐮?
func (db *DB) ChangeAdminPassword(newPassword string) error {
	hash, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	_, err = db.conn.Exec("UPDATE admin_user SET password_hash = ?, updated_at = ? WHERE id = 1", string(hash), time.Now().Unix())
	return err
}
// ChangeAdminUsername 淇敼管理员樼敤鎴峰悕
func (db *DB) ChangeAdminUsername(newUsername string) error {
	_, err := db.conn.Exec("UPDATE admin_user SET username = ?, updated_at = ? WHERE id = 1", newUsername, time.Now().Unix())
	return err
}

// ========== 设备Token管理 ==========

// CreateDeviceToken 创建新的设备Token，返回Token字符串和ID
func (db *DB) CreateDeviceToken() (string, int64, error) {
	token := GenerateToken()
	now := time.Now().UnixMilli()
	result, err := db.conn.Exec("INSERT INTO device_tokens (token, device_id, created_at, revoked) VALUES (?, '', ?, 0)", token, now)
	if err != nil {
		return "", 0, err
	}
	id, _ := result.LastInsertId()
	return token, id, nil
}

// ListDeviceTokens 返回所有设备Token列表
func (db *DB) ListDeviceTokens() ([]map[string]interface{}, error) {
	rows, err := db.conn.Query("SELECT id, token, device_id, created_at, revoked FROM device_tokens ORDER BY created_at DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []map[string]interface{}
	for rows.Next() {
		var id int64
		var token, deviceID string
		var createdAt int64
		var revoked int
		rows.Scan(&id, &token, &deviceID, &createdAt, &revoked)
		result = append(result, map[string]interface{}{
			"id": id, "token": token, "device_id": deviceID,
			"created_at": createdAt, "revoked": revoked == 1,
		})
	}
	return result, nil
}

// DeleteDeviceToken 删除指定ID的Token（同时解绑设备）
func (db *DB) DeleteDeviceToken(id int64) error {
	_, err := db.conn.Exec("DELETE FROM device_tokens WHERE id = ?", id)
	return err
}

// VerifyDeviceTokenByToken 通过原始Token字符串验证Token是否有效
// 已绑定设备的Token也允许通过（支持设备重连）
func (db *DB) VerifyDeviceTokenByToken(rawToken string) (bool, int64) {
	var id int64
	var deviceID string
	var revoked int
	err := db.conn.QueryRow("SELECT id, device_id, revoked FROM device_tokens WHERE token = ?", rawToken).Scan(&id, &deviceID, &revoked)
	if err != nil {
		return false, 0
	}
	if revoked == 1 {
		return false, 0
	}
	// token有效，无论是否已绑定都返回true
	return true, id
}

// BindDeviceToken 将Token绑定到设备
func (db *DB) BindDeviceToken(tokenID int64, deviceID string) error {
	_, err := db.conn.Exec("UPDATE device_tokens SET device_id = ? WHERE id = ?", deviceID, tokenID)
	return err
}

// UpdateDeviceToken 更新设备的token_hash（设备重新注册时调用）
func (db *DB) UpdateDeviceToken(deviceID string, newToken string) error {
	tokenHash := hashToken(newToken)
	_, err := db.conn.Exec("UPDATE devices SET token_hash = ? WHERE id = ?", tokenHash, deviceID)
	return err
}

// ========== 日志管理 ==========

// DeleteActivityLog 删除单条活动日志
func (db *DB) DeleteActivityLog(id int64) error {
	_, err := db.conn.Exec("DELETE FROM activity_logs WHERE id = ?", id)
	return err
}

// ClearActivityLogs 清空所有活动日志
func (db *DB) ClearActivityLogs() error {
	_, err := db.conn.Exec("DELETE FROM activity_logs")
	return err
}


// ========== 数据库清理 ==========

// PurgeDeletedFiles 删除数据库中标记为已删除的文件记录
// 返回删除的记录数
func (db *DB) PurgeDeletedFiles() (int, error) {
	result, err := db.conn.Exec("DELETE FROM files WHERE deleted = 1")
	if err != nil {
		return 0, err
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

// PurgeOldSyncEvents 删除超过指定天数的同步事件记录
// 返回删除的记录数
func (db *DB) PurgeOldSyncEvents(days int) (int, error) {
	cutoff := time.Now().AddDate(0, 0, -days).UnixMilli()
	result, err := db.conn.Exec("DELETE FROM sync_events WHERE created_at < ?", cutoff)
	if err != nil {
		return 0, err
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

// PurgeOldActivityLogs 删除超过指定天数的活动日志
// 返回删除的记录数
func (db *DB) PurgeOldActivityLogs(days int) (int, error) {
	cutoff := time.Now().AddDate(0, 0, -days).UnixMilli()
	result, err := db.conn.Exec("DELETE FROM activity_logs WHERE created_at < ?", cutoff)
	if err != nil {
		return 0, err
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

// PurgeResolvedConflicts 删除已解决的冲突记录
// 返回删除的记录数
func (db *DB) PurgeResolvedConflicts() (int, error) {
	result, err := db.conn.Exec("DELETE FROM conflicts WHERE resolved = 1")
	if err != nil {
		return 0, err
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

// VacuumDB 执行 VACUUM 压缩数据库文件，回收已删除数据占用的空间
func (db *DB) VacuumDB() error {
	_, err := db.conn.Exec("VACUUM")
	return err
}

// GetDBSize 返回数据库文件大小（字节）
// 注意：需要从外部获取数据库文件路径，这里通过 PRAGMA 查询页面数估算
func (db *DB) GetDBStats() (pageCount int64, pageSize int64, err error) {
	err = db.conn.QueryRow("PRAGMA page_count").Scan(&pageCount)
	if err != nil {
		return 0, 0, err
	}
	err = db.conn.QueryRow("PRAGMA page_size").Scan(&pageSize)
	return
}