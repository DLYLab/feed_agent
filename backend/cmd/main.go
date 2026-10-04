package main

import (
	"context"
	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/db"
	apphttp "feedsystem_video_go/internal/http"
	rabbitmq "feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/observability"
	"log"
	"strconv"
	"time"
)

func main() {
	// --- 1. 配置加载阶段 ---
	// 从指定路径加载 YAML 配置文件
	log.Printf("Loading config from configs/config.yaml")
	const configPath = "configs/config.yaml"
	cfg, usedDefault, err := config.LoadLocalDev(configPath) // 读取配置，并解析到Config结构体中
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	// 如果没找到文件，系统会使用预设的默认值，确保程序能跑起来
	if usedDefault {
		log.Printf("Config File %s not found, using default local config", configPath)
	} else {
		log.Printf("Config loaded from file: %s", configPath)
	}

	// --- 2. 数据库初始化阶段 ---
	// 初始化 GORM 数据库连接
	//log.Printf("Database config: %v", cfg.Database)
	sqlDB, err := db.NewDB(cfg.Database)
	if err != nil {
		log.Fatalf("Failed to connect database: %v", err)
	}
	if err := db.AutoMigrate(sqlDB); err != nil {
		log.Fatalf("Failed to auto migrate database: %v", err)
	}
	defer db.CloseDB(sqlDB)

	// --- 3. Redis 缓存初始化 (弱依赖设计) ---
	// 连接 Redis，用于 Token 校验缓存和 Feed 流性能优化
	cache, err := rediscache.NewFromEnv(&cfg.Redis)
	if err != nil {
		// 如果配置错误，仅打印日志但不退出，cache 设为 nil 触发后续业务降级
		log.Printf("Redis config error (cache disabled): %v", err)
		cache = nil
	} else {
		// 进行健康检查 (Ping)，设置 300ms 超时防止启动阻塞
		pingCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		if err := cache.Ping(pingCtx); err != nil {
			log.Printf("Redis not available (cache disabled): %v", err)
			_ = cache.Close()
			// 连接不可用时降级为 MySQL 直读模式
			cache = nil
		} else {
			defer cache.Close()
			log.Printf("Redis connected (cache enabled)")
		}
	}

	// --- 4. RabbitMQ 初始化 (弱依赖设计) ---
	// 连接 RabbitMQ，用于点赞、评论、关注等事件的异步处理
	rmq, err := rabbitmq.NewRabbitMQ(&cfg.RabbitMQ)
	if err != nil {
		log.Printf("RabbitMQ config error (disabled): %v", err)
		rmq = nil
	} else {
		// 连接成功后，后续 Handler 优先发送事件到 MQ
		defer rmq.Close()
		log.Printf("RabbitMQ connected")
	}

	// --- 5. 性能监控初始化 ---
	// 启动 Pprof 服务器，用于运行时查看 CPU、内存和协程状态
	pprofServer, err := observability.NewPprofServer(
		"API",
		cfg.ObservabilityConfig.Pprof.Enabled,
		cfg.ObservabilityConfig.Pprof.ApiAddr,
	)
	if err != nil {
		log.Printf("Failed to start API pprof server: %v", err)
	}
	defer pprofServer.Close()

	// --- 6. 路由与 HTTP 服务启动 ---
	// 将 DB、Redis、MQ 实例注入到路由中（依赖注入模式）
	r := apphttp.SetRouter(sqlDB, cache, rmq)
	log.Printf("Server is running on port %d", cfg.Server.Port)
	// 启动 Gin HTTP 服务器，监听指定端口
	if err := r.Run(":" + strconv.Itoa(cfg.Server.Port)); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}
}
