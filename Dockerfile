# 构建阶段
FROM golang:1.23-alpine AS builder

WORKDIR /app

# 安装依赖
RUN apk add --no-cache git

# 设置 Go 代理
ENV GOPROXY=https://goproxy.cn,direct

# 复制依赖文件
COPY go.mod go.sum ./
RUN go mod download

# 复制源代码
COPY . .

# 构建
RUN CGO_ENABLED=0 GOOS=linux go build -o mclaw cmd/gateway/main.go

# 运行阶段
FROM alpine:latest

WORKDIR /app

# 安装 ca 证书
RUN apk add --no-cache ca-certificates tzdata

# 设置时区
ENV TZ=Asia/Shanghai

# 复制二进制文件
COPY --from=builder /app/mclaw .

# 创建目录
RUN mkdir -p /app/users /app/data /app/logs

# 复制示例配置
COPY data/config.example.json /app/data/config.json

# 暴露端口
EXPOSE 8900

# 入口点
ENTRYPOINT ["./mclaw"]
CMD ["-config", "/app/data/config.json", "-log-dir", "/app/logs"]
