package http

import (
	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/feed"
	"feedsystem_video_go/internal/middleware/jwt"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/middleware/ratelimit"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"
	"feedsystem_video_go/internal/worker"
	"log"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// SetRouter 构建 Gin HTTP 路由，注入数据库、Redis 缓存和 RabbitMQ 依赖，
// 并注册各业务模块的路由、中间件与异步 worker。
//
// 系统架构说明：
// 1. 三层架构：Repository(数据访问层) -> Service(业务逻辑层) -> Handler(HTTP接口层)
// 2. 模块化设计：每个业务模块独立，便于维护和扩展
// 3. 中间件链：JWT认证、限流、缓存等中间件按需组合
// 4. 异步处理：通过RabbitMQ实现点赞、评论、关注等操作的异步处理，提升响应速度
//
// 参数说明：
//   - db: GORM数据库连接，用于持久化存储
//   - cache: Redis客户端，用于缓存和分布式锁
//   - rmq: RabbitMQ连接，用于异步消息队列
//
// 返回值：配置好的Gin引擎实例
func SetRouter(db *gorm.DB, cache *rediscache.Client, rmq *rabbitmq.RabbitMQ) *gin.Engine {
	// 初始化Gin引擎，使用默认中间件（日志、恢复等）
	r := gin.Default()

	// 安全配置：不信任任何代理，防止X-Forwarded-For等头部被伪造
	if err := r.SetTrustedProxies(nil); err != nil {
		log.Printf("SetTrustedProxies failed: %v", err)
	}

	// 静态资源访问：将本地文件上传目录映射到URL路径
	// 前端可以通过 /static/文件名 访问上传的视频封面和文件
	// 例如：http://localhost:8080/static/videos/123.mp4
	r.Static("/static", "./.run/uploads")

	// ============================================================================
	// 1. 限流器配置 (Rate Limiting)
	// ============================================================================
	// 针对不同接口类型设置防刷策略，保护系统稳定性：
	// - 登录/注册按 IP 限制：防止暴力破解和恶意注册
	// - 点赞/评论/关注按账号限流：防止单个用户刷数据
	// 限流器使用Redis存储计数，支持分布式部署

	// 账户安全相关限流（基于IP）：
	// - 登录限流：每分钟最多10次，防止暴力破解密码
	// - 注册限流：每小时最多5次，防止恶意注册大量账号
	loginLimiter := ratelimit.Limit(cache, "account_login", 3, time.Minute, ratelimit.KeyByIP) // redis key : feedsystem:ratelimit:account_login:ip
	registerLimiter := ratelimit.Limit(cache, "account_register", 5, time.Hour, ratelimit.KeyByIP)

	// 业务操作限流（基于账号ID）：
	// - 点赞限流：每分钟最多30次，防止刷赞
	// - 评论限流：每分钟最多10次，防止刷评论
	// - 关注限流：每分钟最多20次，防止恶意关注/取关
	likeLimiter := ratelimit.Limit(cache, "like_write", 30, time.Minute, ratelimit.KeyByAccount) // redis key : feedsystem:ratelimit:like_write:account_id
	commentLimiter := ratelimit.Limit(cache, "comment_write", 10, time.Minute, ratelimit.KeyByAccount)
	socialLimiter := ratelimit.Limit(cache, "social_write", 20, time.Minute, ratelimit.KeyByAccount)

	// ============================================================================
	// 2. 模块化设计与依赖注入 (Dependency Injection)
	// ============================================================================
	// 系统拆分为 Account、Video、Like、Comment、Social、Feed 等模块，
	// 每个模块遵循 Repository -> Service -> Handler 的三层架构：
	// - Repository: 数据访问层，直接操作数据库
	// - Service: 业务逻辑层，处理核心业务逻辑
	// - Handler: HTTP接口层，处理请求和响应
	// 通过构造函数将 db、cache、rmq 等依赖注入到各模块中，实现松耦合

	// ============================================================================
	// Account 模块 - 用户账户管理
	// ============================================================================
	// 功能：用户注册、登录、登出、修改密码、重命名、查询用户信息等
	// 设计特点：
	// 1. 密码使用bcrypt加密存储
	// 2. 登录成功后生成JWT token
	// 3. 支持token黑名单机制实现登出
	// 4. 用户信息缓存到Redis，减少数据库查询
	accountRepository := account.NewAccountRepository(db)
	accountService := account.NewAccountService(accountRepository, cache)
	accountHandler := account.NewAccountHandler(accountService)

	// 公开接口（无需认证） - 路径前缀：/account
	accountGroup := r.Group("/account")
	{
		// 用户注册 - 需要注册限流，防止恶意注册
		accountGroup.POST("/register", registerLimiter, accountHandler.CreateAccount)
		// 用户登录 - 需要登录限流，防止暴力破解
		accountGroup.POST("/login", loginLimiter, accountHandler.Login) // redis key : "account:%d", account.ID
		// 修改密码 - 需要旧密码验证安全性
		accountGroup.POST("/changePassword", accountHandler.ChangePassword)
		// 用户查询接口（公开） - 支持按ID或用户名查询
		accountGroup.POST("/findByID", accountHandler.FindByID)
		accountGroup.POST("/findByUsername", accountHandler.FindByUsername)
	}

	// 受保护接口（需要JWT认证）
	// JWTAuth中间件：验证token有效性，提取用户信息到上下文，检查token黑名单
	protectedAccountGroup := accountGroup.Group("")
	protectedAccountGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		// 用户登出 - 将当前token加入黑名单，使其失效
		protectedAccountGroup.POST("/logout", accountHandler.Logout)
		// 重命名 - 修改用户名，需要验证新用户名是否可用
		protectedAccountGroup.POST("/rename", accountHandler.Rename)
	}

	// Video 模块 - 视频管理
	// ============================================================================
	// 功能：视频上传、封面设置、发布、查询视频列表和详情
	// 设计特点：
	// 1. 视频文件分片上传，支持大文件
	// 2. 封面图片单独上传，支持预览
	// 3. 视频发布后触发异步处理（如转码、生成缩略图）
	// 4. 视频热度通过RabbitMQ异步更新
	videoRepository := video.NewVideoRepository(db)

	// 初始化热度消息队列（用于异步更新视频热度）
	popularityMQ, err := rabbitmq.NewPopularityMQ(rmq)
	if err != nil {
		log.Printf("PopularityMQ init failed (mq disabled): %v", err)
		popularityMQ = nil
	}

	videoService := video.NewVideoService(videoRepository, cache, popularityMQ)
	videoHandler := video.NewVideoHandler(videoService, accountService)

	// 公开接口（无需认证） - 路径前缀：/video
	videoGroup := r.Group("/video")
	{
		// 按作者ID查询视频列表 - 用于用户主页展示
		videoGroup.POST("/listByAuthorID", videoHandler.ListByAuthorID)
		// 获取视频详情 - 包括视频信息、作者信息、点赞数等
		videoGroup.POST("/getDetail", videoHandler.GetDetail) // key:  "video:detail:id=%d", id
	}

	// 受保护接口（需要JWT认证） - 只有登录用户才能上传和发布视频
	protectedVideoGroup := videoGroup.Group("")
	protectedVideoGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		// 上传视频文件 - 支持分片上传，返回文件路径
		protectedVideoGroup.POST("/uploadVideo", videoHandler.UploadVideo)
		// 上传封面图片 - 为视频设置封面
		protectedVideoGroup.POST("/uploadCover", videoHandler.UploadCover)
		// 发布视频 - 将上传的视频和封面关联，设置为公开状态
		protectedVideoGroup.POST("/publish", videoHandler.PublishVideo) // 异步发布
	}

	// Like 模块 - 点赞功能
	// ============================================================================
	// 功能：点赞/取消点赞、查询点赞状态、获取用户点赞列表
	// 设计特点：
	// 1. 点赞操作异步化，通过RabbitMQ处理，提高响应速度
	// 2. 点赞状态缓存到Redis，减少数据库查询
	// 3. 支持防刷机制，限制用户点赞频率
	// 4. 点赞时同步更新视频热度

	// 初始化点赞消息队列（用于异步处理点赞操作）
	likeMQ, err := rabbitmq.NewLikeMQ(rmq)
	if err != nil {
		log.Printf("LikeMQ init failed (mq disabled): %v", err)
		likeMQ = nil
	}

	likeRepository := video.NewLikeRepository(db)
	likeService := video.NewLikeService(likeRepository, videoRepository, cache, likeMQ, popularityMQ)
	likeHandler := video.NewLikeHandler(likeService)

	// 所有点赞接口都需要认证（防止匿名点赞）
	likeGroup := r.Group("/like")
	protectedLikeGroup := likeGroup.Group("")
	protectedLikeGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		// 点赞视频 - 需要限流，防止刷赞,  异步消息
		protectedLikeGroup.POST("/like", likeLimiter, likeHandler.Like) // key: "hot:video:1m:" + now.Format("200601021504"); member: VideoID, score: +1; Expire: 2h
		// 取消点赞 - 需要限流
		protectedLikeGroup.POST("/unlike", likeLimiter, likeHandler.Unlike) // score: -1
		// 查询是否已点赞 - 快速从缓存获取状态
		protectedLikeGroup.POST("/isLiked", likeHandler.IsLiked)
		// 获取用户点赞的视频列表 - 用于"我的喜欢"页面
		protectedLikeGroup.POST("/listMyLikedVideos", likeHandler.ListMyLikedVideos)
	}

	// Comment 模块
	//    提供评论列表、发表评论、删除评论等接口。
	// ============================================================================
	// 功能：发表评论、删除评论、获取评论列表
	// 设计特点：
	// 1. 评论操作异步化，通过RabbitMQ处理
	// 2. 评论列表分页查询，支持按时间排序
	// 3. 评论内容缓存，提高读取性能
	// 4. 评论时同步更新视频热度

	// 初始化评论消息队列（用于异步处理评论操作）
	commentRepository := video.NewCommentRepository(db)
	commentMQ, err := rabbitmq.NewCommentMQ(rmq)
	if err != nil {
		log.Printf("CommentMQ init failed (mq disabled): %v", err)
		commentMQ = nil
	}

	commentService := video.NewCommentService(commentRepository, videoRepository, cache, commentMQ, popularityMQ)
	commentHandler := video.NewCommentHandler(commentService, accountService)

	// 公开接口（无需认证） - 路径前缀：/comment
	commentGroup := r.Group("/comment")
	{
		// 获取视频的所有评论 - 支持分页，按时间倒序排列
		commentGroup.POST("/listAll", commentHandler.GetAllComments)
	}

	// 受保护接口（需要JWT认证） - 只有登录用户才能发表和删除评论
	protectedCommentGroup := commentGroup.Group("")
	protectedCommentGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		// 发表评论 - 需要限流，防止刷评论
		protectedCommentGroup.POST("/publish", commentLimiter, commentHandler.PublishComment)
		// 删除评论 - 需要限流，只能删除自己的评论
		protectedCommentGroup.POST("/delete", commentLimiter, commentHandler.DeleteComment)
	}

	// Social 模块 - 社交关系功能
	// ============================================================================
	// 功能：关注/取关用户、获取粉丝列表、获取关注列表
	// 设计特点：
	// 1. 关注关系异步处理，通过RabbitMQ更新timeline
	// 2. 支持双向关系查询（粉丝和关注）
	// 3. 关注操作限流，防止恶意关注/取关
	// 4. 关注关系变化时更新用户的feed流

	// 初始化社交消息队列（用于异步处理关注关系）
	socialMQ, err := rabbitmq.NewSocialMQ(rmq)
	if err != nil {
		log.Printf("SocialMQ init failed (mq disabled): %v", err)
		socialMQ = nil
	}

	socialRepository := social.NewSocialRepository(db)
	socialService := social.NewSocialService(socialRepository, accountRepository, socialMQ)
	socialHandler := social.NewSocialHandler(socialService)

	// 所有社交接口都需要认证（关注关系需要用户身份）
	socialGroup := r.Group("/social")
	protectedSocialGroup := socialGroup.Group("")
	protectedSocialGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		// 关注用户 - 需要限流，建立关注关系// 异步消息
		protectedSocialGroup.POST("/follow", socialLimiter, socialHandler.Follow)
		// 取消关注 - 需要限流，解除关注关系
		protectedSocialGroup.POST("/unfollow", socialLimiter, socialHandler.Unfollow)
		// 获取所有粉丝 - 查询关注我的用户列表
		protectedSocialGroup.POST("/getAllFollowers", socialHandler.GetAllFollowers)
		// 获取所有关注的人 - 查询我关注的用户列表
		protectedSocialGroup.POST("/getAllVloggers", socialHandler.GetAllVloggers)
	}

	// Feed 模块 - 视频流推荐功能
	// ============================================================================
	// 功能：最新视频、热门视频、点赞排行、关注流等推荐列表
	// 设计特点：
	// 1. 使用SoftJWTAuth中间件，支持未登录用户浏览
	// 2. 多种推荐算法：时间排序、热度排序、点赞数排序
	// 3. 关注流只对登录用户开放
	// 4. 列表数据缓存，提高响应速度

	feedRepository := feed.NewFeedRepository(db)
	feedService := feed.NewFeedService(feedRepository, likeRepository, cache)
	feedHandler := feed.NewFeedHandler(feedService)

	// 公开接口（支持未登录用户） - 路径前缀：/feed
	// SoftJWTAuth中间件：如果用户已登录，提取用户信息；如果未登录，继续处理
	feedGroup := r.Group("/feed")
	feedGroup.Use(jwt.SoftJWTAuth(accountRepository, cache))
	{
		// 最新视频列表 - 按发布时间倒序排列
		feedGroup.POST("/listLatest", feedHandler.ListLatest) // 有关redis : "feed:global_timeline"
		// 点赞数排行 - 按点赞数从高到低排列
		feedGroup.POST("/listLikesCount", feedHandler.ListLikesCount) // 无关redis
		// 热门视频 - 综合热度算法（点赞、评论、时间等因素）
		feedGroup.POST("/listByPopularity", feedHandler.ListByPopularity) // key: dest := "hot:video:merge:1m:" + asOf.Format("200601021504")
	}

	// 受保护接口（需要登录） - 关注流只对登录用户开放
	protectedFeedGroup := feedGroup.Group("")
	protectedFeedGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		// 关注流 - 只显示关注用户的视频，按时间倒序
		protectedFeedGroup.POST("/listByFollowing", feedHandler.ListByFollowing) // cacheKey = fmt.Sprintf("feed:listByFollowing:limit=%d:accountID=%d:before=%d", limit, viewerAccountID, before)
	}
	// Worker 启动 - 异步任务处理
	// ============================================================================
	// 功能：启动后台worker处理异步任务，包括outbox轮询和timeline消费
	// 设计特点：
	// 1. Outbox模式：保证消息至少发送一次，避免数据不一致
	// 2. 定时轮询：定期检查outbox表，发送未处理的消息
	// 3. 消息消费：处理timeline更新、热度计算等异步任务
	// 4. 容错机制：worker失败后可以重启，消息不会丢失

	// 初始化timeline消息队列（用于用户feed流更新）
	timelineMQ, err := rabbitmq.NewTimelineMQ(rmq)
	if err != nil {
		log.Printf("timelineMQ init failed (mq disabled): %v", err)
		socialMQ = nil
	}

	// 启动Outbox轮询器：定期从数据库outbox表读取未发送的消息，发送到RabbitMQ
	// 保证即使服务重启，未发送的消息也不会丢失
	worker.StartOutboxPoller(db, timelineMQ)

	// 启动消息消费者：从RabbitMQ队列消费消息，更新用户timeline和缓存
	// 队列名称：video.timeline.update.queue
	worker.StartConsumer(timelineMQ, "video.timeline.update.queue", cache)

	// 返回配置好的Gin引擎
	return r
}
