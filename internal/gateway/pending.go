package gateway

// CleanupPending 清理待处理请求
func (p *NodePool) CleanupPending(reqID string) {
	if v, ok := p.pendingRequests.LoadAndDelete(reqID); ok {
		v.(*PendingRequest).MarkDone()
	}
}

// PendingRequestsCount 返回待处理请求数量
func (p *NodePool) PendingRequestsCount() int {
	count := 0
	p.pendingRequests.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}
