package manager

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ManagerState 管理器状态（持久化）
type ManagerState struct {
	CurrentUserID string                    `json:"current_user_id"`
	Statuses      map[string]*PersistStatus `json:"statuses"`
	UpdatedAt     time.Time                 `json:"updated_at"`
}

// PersistStatus 持久化的账号状态
type PersistStatus struct {
	Status      string    `json:"status"`
	ExpireTime  time.Time `json:"expire_time"`
	RemainSec   int       `json:"remain_sec"`
	FrozenUntil time.Time `json:"frozen_until,omitempty"`
}

var (
	statePath = "data/manager_state.json" // 默认值，可通过 SetStatePath 覆盖
	stateMu   sync.RWMutex
)

// SetStatePath 设置状态文件路径
func SetStatePath(path string) {
	stateMu.Lock()
	defer stateMu.Unlock()
	statePath = path
}

// SaveManagerState 保存管理器状态
func SaveManagerState(currentUserID string, statuses map[string]*AccountStatus) {
	stateMu.Lock()
	defer stateMu.Unlock()

	state := ManagerState{
		CurrentUserID: currentUserID,
		Statuses:      make(map[string]*PersistStatus),
		UpdatedAt:     time.Now(),
	}

	for uid, s := range statuses {
		state.Statuses[uid] = &PersistStatus{
			Status:      s.Status,
			ExpireTime:  s.ExpireTime,
			RemainSec:   s.RemainSec,
			FrozenUntil: s.FrozenUntil,
		}
	}

	os.MkdirAll(filepath.Dir(statePath), 0755)
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		slog.Error("序列化状态失败", "error", err)
		return
	}

	if err := os.WriteFile(statePath, data, 0644); err != nil {
		slog.Error("保存状态失败", "error", err)
	}
}

// LoadManagerState 加载管理器状态
func LoadManagerState() *ManagerState {
	stateMu.RLock()
	defer stateMu.RUnlock()

	data, err := os.ReadFile(statePath)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("加载状态失败", "error", err)
		}
		return nil
	}

	var state ManagerState
	if err := json.Unmarshal(data, &state); err != nil {
		slog.Warn("解析状态失败", "error", err)
		return nil
	}

	slog.Info("加载管理器状态", "current_user", state.CurrentUserID, "statuses", len(state.Statuses))
	return &state
}
