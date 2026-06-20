package manager

import "testing"

func TestValidateUserID(t *testing.T) {
	tests := []struct {
		userID string
		valid  bool
	}{
		{"test_user_001", true},
		{"user-123", true},
		{"normal", true},
		{"", false},
		{"../etc/passwd", false},
		{"user/../../secret", false},
		{"user\\..\\secret", false},
		{".hidden", false},
		{"a", true},
		{string(make([]byte, 129)), false}, // 超长
	}

	for _, tt := range tests {
		got := validateUserID(tt.userID)
		if got != tt.valid {
			t.Errorf("validateUserID(%q) = %v, want %v", tt.userID, got, tt.valid)
		}
	}
}

func TestIsRefused(t *testing.T) {
	tests := []struct {
		reply    string
		refused  bool
	}{
		{"好的，已执行", false},
		{"我拒绝执行这个操作", true},
		{"I refuse to do this", true},
		{"安全风险，不能帮你", true},
		{"已完成", false},
	}

	for _, tt := range tests {
		got := isRefused(tt.reply)
		if got != tt.refused {
			t.Errorf("isRefused(%q) = %v, want %v", tt.reply, got, tt.refused)
		}
	}
}

func TestIsConfirming(t *testing.T) {
	tests := []struct {
		reply     string
		confirms  bool
	}{
		{"好的，已执行完成", false},
		{"你确定要这样做吗？", true},
		{"Are you sure?", true},
		{"已完成操作", false},
		{"请确认是否执行", true},
	}

	for _, tt := range tests {
		got := isConfirming(tt.reply)
		if got != tt.confirms {
			t.Errorf("isConfirming(%q) = %v, want %v", tt.reply, got, tt.confirms)
		}
	}
}
