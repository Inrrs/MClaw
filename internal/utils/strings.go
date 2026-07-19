package utils

// Truncate 截断字符串到指定最大长度，超出部分用 "..." 代替
func Truncate(s string, maxLen int) string {
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen]) + "..."
}
