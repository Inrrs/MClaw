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
	"mclaw/internal/utils"
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
	// 截断 max_tokens（MIMO 限制）
	if mt, ok := reqMap["max_tokens"].(float64); ok && mt > 16384 {
		reqMap["max_tokens"] = 16384
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
			Message: utils.Truncate(errMsg, 500),
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

// prepareRequest 预处理请求体：模型映射 + 图片降级 + 格式规范化
func prepareRequest(body []byte, applyMapping bool) []byte {
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
			if curModel != "" && curModel != "mimo-v2.5" && curModel != "mimo-v2-flash" {
				body = replaceModel(body, "mimo-v2.5")
				slog.Info("图片请求自动降级", "from", curModel, "to", "mimo-v2.5")
			}
		}
	}
	return normalizeBody(body)
}

// handleProxyRequest 通用代理请求处理（模型映射 + 发送 + 流式/非流式分发）
// skipNormalize 为 true 时跳过 prepareRequest（已转换的 Anthropic 请求不需要二次处理）
func handleProxyRequest(ctx context.Context, pool *gateway.NodePool, path string, skipNormalize bool, w http.ResponseWriter, body []byte) {
	if !skipNormalize {
		body = prepareRequest(body, true)
	}
	// 调试：只在 DEBUG 级别记录请求体
	slog.Debug("bridge 请求", "path", path, "skipNormalize", skipNormalize, "body_len", len(body))

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
// bridge_fallback.py 处理 system prompt 注入和格式转换，网关只做模型映射
func HandleChatCompletions(pool *gateway.NodePool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := readBody(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, "Invalid request body")
			return
		}
		// 模型映射
		model := getRequestModel(body)
		if model != "" {
			mapped := ApplyModelMapping(model)
			if mapped != model {
				body = replaceModel(body, mapped)
			}
		}
		// 图片请求自动降级：mimo-v2.5-pro 不支持图片，切到 mimo-v2.5
		// bridge 已修：mimo-v2.5 不注入 system prompt，不会 400
		if containsImage(body) {
			curModel := getRequestModel(body)
			if curModel != "" && curModel != "mimo-v2.5" && curModel != "mimo-v2-flash" {
				body = replaceModel(body, "mimo-v2.5")
				slog.Info("图片请求自动降级", "from", curModel, "to", "mimo-v2.5")
			}
		}
		handleProxyRequest(r.Context(), pool, "/v1/chat/completions", true, w, body)
	}
}
func HandleResponses(pool *gateway.NodePool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := readBody(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, "Invalid request body")
			return
		}
		handleProxyRequest(r.Context(), pool, "/v1/responses", true, w, body)
	}
}

// HandleMessages Anthropic Messages API
// 网关做 Anthropic→OpenAI 请求转换 + OpenAI→Anthropic 响应转换
// bridge 只做纯透传（对标 mimi3 极简 bridge）
func HandleMessages(pool *gateway.NodePool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := readBody(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, "Invalid request body")
			return
		}
		isAnthropic := strings.Contains(r.URL.Path, "/anthropic/")
		if isAnthropic {
			body = convertAnthropicToOpenAI(body)
			rw := &anthropicResponseWriter{ResponseWriter: w}
			handleProxyRequest(r.Context(), pool, "/v1/chat/completions", true, rw, body)
			rw.flush()
		} else {
			handleProxyRequest(r.Context(), pool, "/v1/chat/completions", true, w, body)
		}
	}
}

// anthropicResponseWriter 包装 ResponseWriter，拦截写入做 OpenAI→Anthropic 响应转换
type anthropicResponseWriter struct {
	http.ResponseWriter
	buf []byte
}

func (w *anthropicResponseWriter) Write(b []byte) (int, error) {
	w.buf = append(w.buf, b...)
	return len(b), nil
}

func (w *anthropicResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *anthropicResponseWriter) flush() {
	if len(w.buf) == 0 {
		return
	}
	converted := convertOpenAIToAnthropic(w.buf)
	w.ResponseWriter.Header().Set("Content-Type", "application/json")
	w.ResponseWriter.Write(converted)
}

// convertAnthropicToOpenAI 将 Anthropic Messages 格式转为 OpenAI Chat Completions 格式
func convertAnthropicToOpenAI(body []byte) []byte {
	var req map[string]any
	if err := json.Unmarshal(body, &req); err != nil {
		return body
	}
	result := make(map[string]any)
	if model, ok := req["model"].(string); ok { result["model"] = model }
	if stream, ok := req["stream"].(bool); ok { result["stream"] = stream }
	if mt, ok := req["max_tokens"].(float64); ok && mt >= 100 {
		if mt > 16384 { mt = 16384 }
		result["max_tokens"] = int(mt)
	} else {
		result["max_tokens"] = 4096
	}

	// messages
	msgs := make([]map[string]any, 0)
	if sys, ok := req["system"]; ok && sys != nil {
		sysText := ""
		switch s := sys.(type) {
		case string: sysText = s
		case []any:
			parts := make([]string, 0)
			for _, b := range s {
				if block, ok := b.(map[string]any); ok {
					if t, ok := block["text"].(string); ok { parts = append(parts, t) }
				}
			}
			sysText = strings.Join(parts, " ")
		}
		if sysText != "" { msgs = append(msgs, map[string]any{"role": "system", "content": sysText}) }
	}
	if messages, ok := req["messages"].([]any); ok {
		for _, m := range messages {
			msg, ok := m.(map[string]any)
			if !ok { continue }
			role, _ := msg["role"].(string)
			content := msg["content"]
			switch c := content.(type) {
			case string:
				msgs = append(msgs, map[string]any{"role": role, "content": c})
			case []any:
				hasToolUse := false
				hasToolResult := false
				for _, b := range c {
					if block, ok := b.(map[string]any); ok {
						switch block["type"] {
						case "tool_use": hasToolUse = true
						case "tool_result": hasToolResult = true
						}
					}
				}
				if hasToolResult {
					for _, b := range c {
						if block, ok := b.(map[string]any); ok && block["type"] == "tool_result" {
							rc := ""
							switch r := block["content"].(type) {
							case string: rc = r
							case []any:
								parts := make([]string, 0)
								for _, rb := range r {
									if rbMap, ok := rb.(map[string]any); ok {
										if t, ok := rbMap["text"].(string); ok { parts = append(parts, t) }
									}
								}
								rc = strings.Join(parts, "\n")
							}
							toolCallID, _ := block["tool_use_id"].(string)
							msgs = append(msgs, map[string]any{"role": "tool", "tool_call_id": toolCallID, "content": rc})
						}
					}
				} else if hasToolUse {
					textParts := make([]string, 0)
					toolCalls := make([]map[string]any, 0)
					for _, b := range c {
						if block, ok := b.(map[string]any); ok {
							switch block["type"] {
							case "text":
								if t, ok := block["text"].(string); ok { textParts = append(textParts, t) }
							case "tool_use":
								inputJSON, _ := json.Marshal(block["input"])
								toolCalls = append(toolCalls, map[string]any{
									"id":   block["id"],
									"type": "function",
									"function": map[string]any{"name": block["name"], "arguments": string(inputJSON)},
								})
							}
						}
					}
					oaiMsg := map[string]any{"role": "assistant"}
					if len(textParts) > 0 { oaiMsg["content"] = strings.Join(textParts, "\n") }
					if len(toolCalls) > 0 { oaiMsg["tool_calls"] = toolCalls }
					msgs = append(msgs, oaiMsg)
				} else {
					parts := make([]string, 0)
					for _, b := range c {
						if block, ok := b.(map[string]any); ok {
							if block["type"] == "text" {
								if t, ok := block["text"].(string); ok { parts = append(parts, t) }
							}
						}
					}
					msgs = append(msgs, map[string]any{"role": role, "content": strings.Join(parts, "\n")})
				}
			default:
				msgs = append(msgs, map[string]any{"role": role, "content": fmt.Sprintf("%v", c)})
			}
		}
	}
	result["messages"] = msgs

	// tools
	if tools, ok := req["tools"].([]any); ok && len(tools) > 0 {
		oaiTools := make([]map[string]any, 0, len(tools))
		for _, t := range tools {
			tool, ok := t.(map[string]any)
			if !ok { continue }
			if tool["type"] == "function" { oaiTools = append(oaiTools, tool); continue }
			fn := map[string]any{"name": tool["name"], "description": tool["description"]}
			if schema, ok := tool["input_schema"]; ok { fn["parameters"] = schema }
			oaiTools = append(oaiTools, map[string]any{"type": "function", "function": fn})
		}
		if len(oaiTools) > 0 { result["tools"] = oaiTools }
	}

	// tool_choice
	if tc, ok := req["tool_choice"]; ok {
		switch v := tc.(type) {
		case map[string]any:
			switch v["type"] {
			case "auto": result["tool_choice"] = "auto"
			case "any": result["tool_choice"] = "required"
			case "tool":
				if name, ok := v["name"].(string); ok {
					result["tool_choice"] = map[string]any{"type": "function", "function": map[string]any{"name": name}}
				}
			}
		case string: result["tool_choice"] = v
		}
	}

	out, err := json.Marshal(result)
	if err != nil { return body }
	return out
}

// convertOpenAIToAnthropic 将 OpenAI Chat Completion 响应转为 Anthropic Messages 格式
func convertOpenAIToAnthropic(body []byte) []byte {
	var oai map[string]any
	if err := json.Unmarshal(body, &oai); err != nil { return body }
	choices, ok := oai["choices"].([]any)
	if !ok || len(choices) == 0 { return body }
	choice, ok := choices[0].(map[string]any)
	if !ok { return body }
	msg, ok := choice["message"].(map[string]any)
	if !ok { return body }

	blocks := make([]map[string]any, 0)
	if rt, ok := msg["reasoning_content"].(string); ok && rt != "" {
		blocks = append(blocks, map[string]any{"type": "thinking", "thinking": rt})
	}
	if tcs, ok := msg["tool_calls"].([]any); ok && len(tcs) > 0 {
		for _, tc := range tcs {
			tcMap, ok := tc.(map[string]any)
			if !ok { continue }
			fn, _ := tcMap["function"].(map[string]any)
			if fn == nil { continue }
			var inp any
			argsStr, _ := fn["arguments"].(string)
			if err := json.Unmarshal([]byte(argsStr), &inp); err != nil {
				inp = map[string]any{"raw": argsStr}
			}
			blocks = append(blocks, map[string]any{"type": "tool_use", "id": tcMap["id"], "name": fn["name"], "input": inp})
		}
	} else if content, ok := msg["content"].(string); ok && content != "" {
		blocks = append(blocks, map[string]any{"type": "text", "text": content})
	} else {
		blocks = append(blocks, map[string]any{"type": "text", "text": ""})
	}

	sr := "end_turn"
	if fr, ok := choice["finish_reason"].(string); ok {
		switch fr {
		case "tool_calls": sr = "tool_use"
		case "length": sr = "max_tokens"
		case "stop": sr = "end_turn"
		default: sr = fr
		}
	}
	model, _ := oai["model"].(string)
	if model == "" { model = "mimo-v2.5-pro" }
	usage, _ := oai["usage"].(map[string]any)
	inputTokens, outputTokens := 0, 0
	if usage != nil {
		if v, ok := usage["prompt_tokens"].(float64); ok { inputTokens = int(v) }
		if v, ok := usage["completion_tokens"].(float64); ok { outputTokens = int(v) }
	}

	result := map[string]any{
		"id": fmt.Sprintf("msg_%x", time.Now().UnixNano())[:16+len("msg_")],
		"type": "message", "role": "assistant", "content": blocks,
		"model": model, "stop_reason": sr, "stop_sequence": nil,
		"usage": map[string]any{"input_tokens": inputTokens, "output_tokens": outputTokens},
	}
	out, err := json.Marshal(result)
	if err != nil { return body }
	return out
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
				if msg.Body != nil {
					body := unwrapJSON(msg.Body)
					// 首个 chunk 记录错误详情
					if len(chunkBuffer) == 0 {
						var errCheck map[string]any
						if json.Unmarshal(body, &errCheck) == nil {
							if e, ok := errCheck["error"]; ok {
								errJSON, _ := json.Marshal(e)
								slog.Debug("上游错误详情", "body", string(errJSON))
							}
						}
					}
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
