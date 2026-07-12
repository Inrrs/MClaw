package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"unicode/utf8"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"mclaw/internal/gateway"
	"mclaw/internal/metrics"
)

func AuthMiddleware(apiKey string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var token string
			// 先检查 Authorization: Bearer
			auth := r.Header.Get("Authorization")
			if strings.HasPrefix(auth, "Bearer ") {
				token = strings.TrimPrefix(auth, "Bearer ")
			}
			// 兼容 Anthropic SDK 的 x-api-key
			if token == "" {
				token = r.Header.Get("x-api-key")
			}
			if token == "" {
				writeError(w, http.StatusUnauthorized, "Unauthorized")
				return
			}
			if subtle.ConstantTimeCompare([]byte(token), []byte(apiKey)) != 1 {
				writeError(w, http.StatusUnauthorized, "Unauthorized")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

var (
	modelMapping       map[string]string
	modelMappingMu     sync.RWMutex
	modelMappingLoaded bool
)

func LoadModelMapping(path string) {
	modelMappingMu.Lock()
	defer modelMappingMu.Unlock()

	data, err := os.ReadFile(path)
	if err != nil {
		modelMapping = defaultModelMapping()
		if d, e := json.MarshalIndent(modelMapping, "", "  "); e == nil {
			os.MkdirAll("data", 0755)
			os.WriteFile(path, d, 0644)
		}
		modelMappingLoaded = true
		return
	}

	modelMapping = make(map[string]string)
	if err := json.Unmarshal(data, &modelMapping); err != nil {
		slog.Error("解析模型映射失败，使用默认映射", "error", err)
		modelMapping = defaultModelMapping()
	}
	modelMappingLoaded = true
	slog.Info("加载模型映射", "count", len(modelMapping))
}

func ApplyModelMapping(model string) string {
	modelMappingMu.RLock()
	defer modelMappingMu.RUnlock()
	if mapped, ok := modelMapping[model]; ok {
		return mapped
	}
	return model
}

func SaveModelMapping(path string, mapping map[string]string) error {
	modelMappingMu.Lock()
	modelMapping = mapping
	modelMappingMu.Unlock()
	data, err := json.MarshalIndent(mapping, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func defaultModelMapping() map[string]string {
	return map[string]string{
		"gpt-5":              "mimo-v2.5-pro",
		"gpt-5-mini":         "mimo-v2.5",
		"gpt-4.1":            "mimo-v2.5-pro",
		"gpt-4.1-mini":       "mimo-v2.5",
		"gpt-4.1-nano":       "mimo-v2.5",
		"gpt-4o":             "mimo-v2.5-pro",
		"gpt-4o-mini":        "mimo-v2.5",
		"o3":                 "mimo-v2.5-pro",
		"o3-mini":            "mimo-v2.5",
		"o4-mini":            "mimo-v2.5",
		"claude-fable-5":     "mimo-v2.5-pro",
		"claude-opus-4-8":    "mimo-v2.5-pro",
		"claude-sonnet-4-6":  "mimo-v2.5-pro",
		"claude-haiku-4-5":   "mimo-v2.5",
		"gemini-3.5-pro":     "mimo-v2.5-pro",
		"gemini-3.5-flash":   "mimo-v2-flash",
		"gemini-2.5-pro":     "mimo-v2.5-pro",
		"gemini-2.5-flash":   "mimo-v2-flash",
		"gemini-2.0-flash":   "mimo-v2-flash",
	}
}

func containsImage(body []byte) bool {
	s := string(body)
	return strings.Contains(s, `"type":"image_url"`) ||
		strings.Contains(s, `"type": "image_url"`) ||
		strings.Contains(s, `"type":"image"`) ||
		strings.Contains(s, `"type": "image"`)
}

// isStreamRequest 检测是否为流式请求
func isStreamRequest(body []byte) bool {
	var req struct {
		Stream *bool `json:"stream"`
	}
	json.Unmarshal(body, &req)
	return req.Stream != nil && *req.Stream
}

// normalizeBody 规范化请求体，确保 MIMO API 兼容
// 1. 移除 stream_options（MIMO 不支持）
// 2. 将 list[dict] 或 dict 格式的 content 转为纯字符串
// 3. 移除 Anthropic 特有字段（context_management, output_config, metadata）
func normalizeBody(body []byte) []byte {
	var reqMap map[string]any
	if err := json.Unmarshal(body, &reqMap); err != nil {
		return body
	}
	changed := false
	// 移除 stream_options
	if _, exists := reqMap["stream_options"]; exists {
		delete(reqMap, "stream_options")
		changed = true
	}
	// 移除 Anthropic 特有字段（bridge 不需要）
	for _, key := range []string{"context_management", "output_config", "metadata"} {
		if _, exists := reqMap[key]; exists {
			delete(reqMap, key)
			changed = true
		}
	}
	// 规范化 messages content
	if msgs, ok := reqMap["messages"].([]any); ok {
		for _, m := range msgs {
			if msg, ok := m.(map[string]any); ok {
				if content, ok := msg["content"]; ok {
					switch c := content.(type) {
					case []any:
						// list[dict] 格式 → 保留多模态结构，仅规范化 text 部分
						hasNonText := false
						for _, p := range c {
							if part, ok := p.(map[string]any); ok {
								if part["type"] != "text" {
									hasNonText = true
									break
								}
							}
						}
						if !hasNonText {
							// 纯文本列表 → 拼接为字符串
							var texts []string
							for _, p := range c {
								if part, ok := p.(map[string]any); ok {
									if part["type"] == "text" {
										if t, ok := part["text"].(string); ok {
											texts = append(texts, t)
										}
									}
								} else if s, ok := p.(string); ok {
									texts = append(texts, s)
								}
							}
							msg["content"] = strings.Join(texts, "\n")
							changed = true
						}
						// 有多模态内容 → 保持原样不动
					case map[string]any:
						// dict 格式 → 序列化为 JSON 字符串
						if b, err := json.Marshal(c); err == nil {
							msg["content"] = string(b)
						} else {
							msg["content"] = fmt.Sprintf("%v", c)
						}
						changed = true
					}
				}
			}
		}
	}
	if !changed {
		return body
	}
	out, err := json.Marshal(reqMap)
	if err != nil {
		return body
	}
	return out
}

// replaceModel 在 JSON body 中替换 model 字段（结构化操作）
func replaceModel(body []byte, newModel string) []byte {
	var reqMap map[string]any
	if err := json.Unmarshal(body, &reqMap); err != nil {
		return body
	}
	reqMap["model"] = newModel
	out, err := json.Marshal(reqMap)
	if err != nil {
		return body
	}
	return out
}

func getRequestModel(body []byte) string {
	var req struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return ""
	}
	return req.Model
}

// extractUsageFromChunks 从 SSE 流式块中提取 token 用量
func extractUsageFromChunks(chunks [][]byte) metrics.TokenUsage {
	for i := len(chunks) - 1; i >= 0 && i >= len(chunks)-10; i-- {
		data := string(chunks[i])
		for _, line := range strings.Split(data, "\n") {
			line = strings.TrimSpace(line)
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			jsonData := strings.TrimPrefix(line, "data: ")
			if jsonData == "[DONE]" || jsonData == "" {
				continue
			}
			var chunk struct {
				Usage *struct {
					PromptTokens     int `json:"prompt_tokens"`
					CompletionTokens int `json:"completion_tokens"`
					PromptDetails    *struct {
						CachedTokens int `json:"cached_tokens"`
					} `json:"prompt_tokens_details"`
				} `json:"usage"`
			}
			if err := json.Unmarshal([]byte(jsonData), &chunk); err != nil {
				continue
			}
			if chunk.Usage != nil && (chunk.Usage.PromptTokens > 0 || chunk.Usage.CompletionTokens > 0) {
				cached := 0
				if chunk.Usage.PromptDetails != nil {
					cached = chunk.Usage.PromptDetails.CachedTokens
				}
				return metrics.TokenUsage{
					InputTokens:  chunk.Usage.PromptTokens,
					OutputTokens: chunk.Usage.CompletionTokens,
					CacheTokens:  cached,
				}
			}
		}
	}
	return metrics.TokenUsage{}
}

// extractUsageFromBody 从非流式响应体中提取 token 用量
func extractUsageFromBody(body json.RawMessage) metrics.TokenUsage {
	if body == nil {
		return metrics.TokenUsage{}
	}
	var resp struct {
		Usage *struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			PromptDetails    *struct {
				CachedTokens int `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Usage == nil {
		return metrics.TokenUsage{}
	}
	cached := 0
	if resp.Usage.PromptDetails != nil {
		cached = resp.Usage.PromptDetails.CachedTokens
	}
	return metrics.TokenUsage{
		InputTokens:  resp.Usage.PromptTokens,
		OutputTokens: resp.Usage.CompletionTokens,
		CacheTokens:  cached,
	}
}

func sendToAvailableNode(pool *gateway.NodePool, method, path string, body []byte) (*gateway.PendingRequest, *gateway.Node, error) {
	node := pool.GetAvailable()
	if node == nil {
		return nil, nil, fmt.Errorf("no available nodes")
	}
	reqID := gateway.GenerateID()
	pending, err := pool.SendToNode(node, reqID, method, path, json.RawMessage(body))
	if err != nil {
		pool.HandleRequestError(node, 500)
		return nil, nil, err
	}
	return pending, node, nil
}

// recordRequestError 记录请求错误到 ErrorStore
func recordRequestError(pool *gateway.NodePool, nodeID, path string, status int, errMsg string) {
	if pool.ErrorStore != nil {
		pool.ErrorStore.Add(gateway.RequestError{
			NodeID:  nodeID,
			Status:  status,
			Path:    path,
			Message: truncate(errMsg, 500),
		})
	}
}

// extractErrorMessage 从 bridge error body 中提取可读错误信息
func extractErrorMessage(body json.RawMessage) string {
	if body == nil {
		return "unknown error"
	}
	// bridge 发来的 body 可能是 JSON 字符串（带引号），尝试先解码
	var strErr string
	if err := json.Unmarshal(body, &strErr); err == nil {
		return strErr
	}
	return string(body)
}

// handleProxyRequest 通用代理请求处理（模型映射 + 发送 + 流式/非流式分发）
func handleProxyRequest(ctx context.Context, pool *gateway.NodePool, path string, applyMapping bool, w http.ResponseWriter, body []byte) {
	if applyMapping {
		model := getRequestModel(body)
		if model != "" {
			mapped := ApplyModelMapping(model)
			if mapped != model {
				body = replaceModel(body, mapped)
				slog.Debug("模型映射", "from", model, "to", mapped)
			}
		}

		if containsImage(body) {
			curModel := getRequestModel(body)
			if curModel != "" && !strings.Contains(curModel, "mimo-v2.5") {
				body = replaceModel(body, "mimo-v2.5")
				slog.Info("图片请求自动降级", "to", "mimo-v2.5")
			}
		}
	}

	// 规范化请求体（移除 MIMO 不支持的字段，规范化 content 格式）
	body = normalizeBody(body)

	pending, node, err := sendToAvailableNode(pool, "POST", path, body)
	if err != nil {
		recordRequestError(pool, "", path, 503, err.Error())
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	defer pool.CleanupPending(pending.ReqID)

	if isStreamRequest(body) {
		handleStreamResponse(ctx, w, pending, pool, node)
	} else {
		handleNormalResponse(ctx, w, pending, pool, node)
	}
}

// HandleChatCompletions OpenAI Chat Completions
func HandleChatCompletions(pool *gateway.NodePool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := readBody(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, "Invalid request body")
			return
		}
		handleProxyRequest(r.Context(), pool, "/v1/chat/completions", true, w, body)
	}
}

// HandleResponses OpenAI Responses API
func HandleResponses(pool *gateway.NodePool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := readBody(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, "Invalid request body")
			return
		}
		handleProxyRequest(r.Context(), pool, "/v1/responses", false, w, body)
	}
}

// HandleMessages Anthropic Messages API
func HandleMessages(pool *gateway.NodePool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := readBody(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, "Invalid request body")
			return
		}
		// 保留原始路径，让 bridge 区分 OpenAI/Anthropic 格式
		handleProxyRequest(r.Context(), pool, r.URL.Path, false, w, body)
	}
}

// HandleModels 模型列表
func HandleModels(pool *gateway.NodePool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		models := pool.GetModels()
		if models == nil {
			models = []string{}
		}
		type Model struct {
			ID      string `json:"id"`
			Object  string `json:"object"`
			Created int64  `json:"created"`
		}
		resp := struct {
			Object string  `json:"object"`
			Data   []Model `json:"data"`
		}{Object: "list", Data: make([]Model, len(models))}
		for i, m := range models {
			resp.Data[i] = Model{ID: m, Object: "model", Created: time.Now().Unix()}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}

// HandleModelMappingGet 获取模型映射
func HandleModelMappingGet() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		modelMappingMu.RLock()
		defer modelMappingMu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(modelMapping)
	}
}

// HandleModelMappingPut 更新模型映射
func HandleModelMappingPut(path string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var mapping map[string]string
		if err := json.NewDecoder(r.Body).Decode(&mapping); err != nil {
			writeError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		if err := SaveModelMapping(path, mapping); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}
}

// HandleModelMappingDelete 重置模型映射
func HandleModelMappingDelete(path string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		SaveModelMapping(path, make(map[string]string))
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}
}

// HandleNodesStatus 节点状态
func HandleNodesStatus(pool *gateway.NodePool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		total := pool.GetNodeCount()
		available := pool.GetAvailableCount()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]int{"total": total, "available": available})
	}
}

// HandleAvailableModels 可用模型列表
func HandleAvailableModels(pool *gateway.NodePool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		models := pool.GetModels()
		if models == nil {
			models = []string{}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"models": models, "count": len(models)})
	}
}

// handleStreamResponse 流式响应处理（带保活 + context 取消）
func handleStreamResponse(ctx context.Context, w http.ResponseWriter, pending *gateway.PendingRequest, pool *gateway.NodePool, node *gateway.Node) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "Streaming not supported")
		return
	}

	keepaliveTicker := time.NewTicker(60 * time.Second)
	defer keepaliveTicker.Stop()

	chunkTimer := time.NewTimer(600 * time.Second)
	defer chunkTimer.Stop()

	// 累积最近的 chunk 用于提取 token 用量
	var chunkBuffer [][]byte

	for {
		select {
		case <-ctx.Done():
			slog.Debug("客户端断开，清理流式请求", "reqID", pending.ReqID)
			return
		case msg, ok := <-pending.Response:
			if !ok {
				return
			}
			chunkTimer.Reset(600 * time.Second)

			switch msg.Type {
			case "start":
				if msg.Status != 0 && msg.Status != 200 {
					slog.Warn("上游返回非200状态码", "status", msg.Status, "node", node.ID)
				}
			case "chunk":
				// 累积 chunk 用于提取 usage（最多保留 20 个）
				if msg.Body != nil {
					body := unwrapJSON(msg.Body)
					chunkBuffer = append(chunkBuffer, body)
					if len(chunkBuffer) > 20 {
						chunkBuffer = chunkBuffer[len(chunkBuffer)-20:]
					}
					fmt.Fprintf(w, "data: %s\n\n", sanitizeUTF8(body))
				}
				flusher.Flush()
			case "finish":
				// 提取 token 用量
				usage := extractUsageFromChunks(chunkBuffer)
				metrics.Get().RecordRequest(true, usage, 0)
				if usage.InputTokens > 0 || usage.OutputTokens > 0 {
					slog.Debug("流式 token 用量", "input", usage.InputTokens, "output", usage.OutputTokens, "cache", usage.CacheTokens)
				}
				fmt.Fprintf(w, "data: [DONE]\n\n")
				flusher.Flush()
				return
			case "error":
				metrics.Get().RecordRequest(false, metrics.TokenUsage{}, 0)
				statusCode := msg.Status
				if statusCode == 0 {
					statusCode = 502
				}
				pool.HandleRequestError(node, statusCode)
				errMsg := extractErrorMessage(msg.Body)
				slog.Error("上游流式请求失败", "status", statusCode, "node", node.ID, "body", errMsg)
				recordRequestError(pool, node.ID, pending.ReqID, statusCode, errMsg)
				errData, _ := json.Marshal(map[string]any{"error": map[string]any{"message": errMsg, "type": "upstream_error", "code": statusCode}})
				fmt.Fprintf(w, "data: %s\n\n", errData)
				flusher.Flush()
				return
			}

		case <-keepaliveTicker.C:
			fmt.Fprintf(w, ": keep-alive\n\n")
			flusher.Flush()

		case <-chunkTimer.C:
			pool.HandleRequestError(node, 504)
			slog.Warn("流式 chunk 超时", "node", node.ID)
			writeSSEError(w, "Stream chunk timeout")
			flusher.Flush()
			return
		}
	}
}

// handleNormalResponse 非流式响应处理（支持 context 取消）
func handleNormalResponse(ctx context.Context, w http.ResponseWriter, pending *gateway.PendingRequest, pool *gateway.NodePool, node *gateway.Node) {
	timeout := time.After(5 * time.Minute)
	var responseBody json.RawMessage
	startReceived := false

	for {
		select {
		case <-ctx.Done():
			slog.Debug("客户端断开，清理非流式请求", "reqID", pending.ReqID)
			return
		case msg, ok := <-pending.Response:
			if !ok {
				if responseBody != nil {
					w.Header().Set("Content-Type", "application/json")
					w.Write(responseBody)
				} else {
					writeError(w, http.StatusBadGateway, "Node disconnected")
				}
				return
			}
			switch msg.Type {
			case "start":
				startReceived = true
				if msg.Status != 0 && msg.Status != 200 {
					slog.Warn("上游返回非200状态码", "status", msg.Status, "node", node.ID)
				}
			case "chunk":
				if msg.Body != nil {
					responseBody = unwrapJSON(msg.Body)
				}
			case "finish":
				// 提取 token 用量
				finishBody := msg.Body
				if finishBody == nil {
					finishBody = responseBody
				}
				usage := extractUsageFromBody(finishBody)
				metrics.Get().RecordRequest(true, usage, 0)
				if usage.InputTokens > 0 || usage.OutputTokens > 0 {
					slog.Debug("非流式 token 用量", "input", usage.InputTokens, "output", usage.OutputTokens, "cache", usage.CacheTokens)
				}
				w.Header().Set("Content-Type", "application/json")
				if msg.Body != nil {
					w.Write(msg.Body)
				} else if responseBody != nil {
					w.Write(responseBody)
				} else if !startReceived {
					writeError(w, http.StatusBadGateway, "Empty response from upstream")
				}
				return
			case "error":
				metrics.Get().RecordRequest(false, metrics.TokenUsage{}, 0)
				statusCode := msg.Status
				if statusCode == 0 {
					statusCode = 502
				}
				pool.HandleRequestError(node, statusCode)
				errMsg := extractErrorMessage(msg.Body)
				slog.Error("上游请求失败", "status", statusCode, "node", node.ID, "body", errMsg)
				recordRequestError(pool, node.ID, pending.ReqID, statusCode, errMsg)
				// 返回标准 JSON 错误响应
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadGateway)
				json.NewEncoder(w).Encode(map[string]any{
					"error": map[string]any{
						"message": errMsg,
						"type":    "upstream_error",
						"code":    statusCode,
					},
				})
				return
			}
		case <-timeout:
			pool.HandleRequestError(node, 504)
			writeError(w, http.StatusGatewayTimeout, "Request timeout")
			return
		}
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"message": message},
	})
}

func writeSSEError(w http.ResponseWriter, message string) {
	data, _ := json.Marshal(map[string]any{
		"error": map[string]any{"message": message},
	})
	fmt.Fprintf(w, "data: %s\n\n", data)
}

func readBody(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(io.LimitReader(r.Body, 10<<20))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// sanitizeUTF8 清理非法 UTF-8 字节
func sanitizeUTF8(b []byte) []byte {
	if utf8.Valid(b) {
		return b
	}
	return []byte(strings.ToValidUTF8(string(b), "�"))
}

// unwrapJSON 如果 body 是 JSON 字符串（带引号），解包为内部 JSON
func unwrapJSON(b []byte) []byte {
	if len(b) < 2 || b[0] != '"' || b[len(b)-1] != '"' {
		return b
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return b
	}
	return []byte(s)
}
