package manager

import (
	"container/ring"
	"fmt"
	"sync"
	"time"
)

// AccountLog 账号日志
type AccountLog struct {
	Time    time.Time `json:"time"`
	UserID  string    `json:"userId"`
	Level   string    `json:"level"`
	Message string    `json:"message"`
}

// AccountLogStore 账号日志存储
type AccountLogStore struct {
	logs   *ring.Ring
	mu     sync.RWMutex
	maxLen int
}

func NewAccountLogStore(maxLen int) *AccountLogStore {
	return &AccountLogStore{
		logs:   ring.New(maxLen),
		maxLen: maxLen,
	}
}

func (s *AccountLogStore) Add(userID, level, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logs.Value = AccountLog{
		Time:    time.Now(),
		UserID:  userID,
		Level:   level,
		Message: message,
	}
	s.logs = s.logs.Next()
}

func (s *AccountLogStore) GetByUser(userID string, limit int) []AccountLog {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []AccountLog
	s.logs.Do(func(v interface{}) {
		if v == nil {
			return
		}
		log := v.(AccountLog)
		if userID == "" || log.UserID == userID {
			result = append(result, log)
		}
	})

	// 按时间倒序
	for i := 0; i < len(result)/2; i++ {
		j := len(result) - 1 - i
		result[i], result[j] = result[j], result[i]
	}

	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	return result
}

func (s *AccountLogStore) GetAll(limit int) []AccountLog {
	return s.GetByUser("", limit)
}

// 全局日志实例
var AccountLogs = NewAccountLogStore(500)

// LogAccountEvent 记录账号事件
func LogAccountEvent(userID, level, message string) {
	AccountLogs.Add(userID, level, message)
}

// LogAccountInfo 记录信息
func LogAccountInfo(userID, format string, args ...any) {
	AccountLogs.Add(userID, "INFO", fmt.Sprintf(format, args...))
}

// LogAccountWarn 记录警告
func LogAccountWarn(userID, format string, args ...any) {
	AccountLogs.Add(userID, "WARN", fmt.Sprintf(format, args...))
}

// LogAccountError 记录错误
func LogAccountError(userID, format string, args ...any) {
	AccountLogs.Add(userID, "ERROR", fmt.Sprintf(format, args...))
}
