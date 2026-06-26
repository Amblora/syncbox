package syncengine

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"github.com/aync/syncbox/internal/db"
)

// DBAdapter 适配 db.DB 到 syncengine.DBInterface
type DBAdapter struct {
	DB *db.DB
}

// generateFileID 根据文件路径生成确定性ID，避免UNIQUE constraint冲突
func generateFileID(path string) string {
	h := sha256.Sum256([]byte(strings.ToLower(path)))
	return hex.EncodeToString(h[:16])
}

func (a *DBAdapter) UpsertFile(path string, size int64, hash string, version int, deviceID string) error {
	f := &db.File{
		ID:      generateFileID(path),
		RelPath: path,
		PathKey: strings.ToLower(path),
		Type:    "file",
		Size:    size,
		Hash:    hash,
		Version: int64(version),
		MtimeNs: time.Now().UnixNano(),
	}
	return a.DB.UpsertFile(f)
}

func (a *DBAdapter) GetFile(path string) (*FileRecord, error) {
	pathKey := strings.ToLower(path)
	f, err := a.DB.GetFile(pathKey)
	if err != nil {
		return nil, err
	}
	if f == nil {
		return nil, nil
	}
	return &FileRecord{
		Path:    f.RelPath,
		Size:    f.Size,
		Hash:    f.Hash,
		Version: int(f.Version),
	}, nil
}

func (a *DBAdapter) DeleteFile(path string, deviceID string) error {
	return a.DB.DeleteFile(strings.ToLower(path))
}

func (a *DBAdapter) RenameFile(oldPath, newPath string, deviceID string) error {
	return a.DB.RenameFile(strings.ToLower(oldPath), newPath, strings.ToLower(newPath))
}

func (a *DBAdapter) AddEvent(event SyncEvent) error {
	_, err := a.DB.AddEvent(event.Type, event.RelPath, event.OldPath, int64(event.Version), event.DeviceID)
	return err
}

func (a *DBAdapter) GetLastSeq() (int64, error) {
	return a.DB.GetLastSeq(), nil
}

func (a *DBAdapter) ListFiles() ([]*FileRecord, error) {
	files, err := a.DB.ListFiles(false)
	if err != nil {
		return nil, err
	}
	result := make([]*FileRecord, 0, len(files))
	for _, f := range files {
		result = append(result, &FileRecord{
			Path:    f.RelPath,
			Size:    f.Size,
			Hash:    f.Hash,
			Version: int(f.Version),
		})
	}
	return result, nil
}