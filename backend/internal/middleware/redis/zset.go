package redis

import (
	"context"
	"errors"
	"time"

	redis "github.com/redis/go-redis/v9"
)

// 本文件提供了Redis有序集合(ZSet)操作的封装
// 有序集合是Redis中一种特殊的数据结构，每个成员都有一个分数(score)
// 成员按分数从小到大排序，分数可以重复，但成员必须唯一
//
// 主要应用场景：
// 1. 排行榜系统（如视频热度、用户积分）
// 2. 带权重的消息队列
// 3. 时间线排序
// 4. 范围查询和排名查询

// ZincrBy 为有序集合中指定成员的分数增加指定的增量
// 如果成员不存在，会先创建该成员并设置初始分数为0，然后增加增量
//
// 参数：
//
//	ctx: 上下文
//	key: 有序集合的键名
//	member: 成员名称
//	score: 要增加的分数（可以为负数表示减少）
//
// 返回值：
//
//	error: 操作错误，如果客户端未初始化则返回nil
//
// 使用场景：
//   - 用户积分增减
//   - 视频点赞数/播放数统计
//   - 实时排行榜更新
func (c *Client) ZincrBy(ctx context.Context, key string, member string, score float64) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZIncrBy(ctx, key, score, member).Err()
}

// ZAdd 向有序集合中添加一个或多个成员，或者更新已存在成员的分数
// 如果成员已存在，会更新其分数并重新排序
//
// 参数：
//
//	ctx: 上下文
//	key: 有序集合的键名
//	members: 一个或多个redis.Z结构体，包含成员和分数
//	        redis.Z结构体定义：type Z struct { Score float64; Member interface{} }
//
// 返回值：
//
//	error: 操作错误，如果客户端未初始化则返回nil
//
// 使用场景：
//   - 初始化排行榜数据
//   - 批量添加或更新成员分数
//   - 数据迁移或同步
func (c *Client) ZAdd(ctx context.Context, key string, members ...redis.Z) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZAdd(ctx, key, members...).Err()
}

// ZRemRangeByRank 移除有序集合中排名在指定区间内的所有成员
// 排名从0开始，0表示分数最低的成员，-1表示分数最高的成员
//
// 参数：
//
//	ctx: 上下文
//	key: 有序集合的键名
//	start: 起始排名（包含）
//	stop: 结束排名（包含）
//
// 返回值：
//
//	error: 操作错误，如果客户端未初始化则返回nil
//
// 使用场景：
//   - 清理排行榜末尾数据
//   - 定期清理过期或无效数据
//   - 数据分片处理
func (c *Client) ZRemRangeByRank(ctx context.Context, key string, start int64, stop int64) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZRemRangeByRank(ctx, key, start, stop).Err()
}

// ZRangeWithScores 返回有序集合中指定排名区间内的成员及其分数
// 按分数从小到大排序，返回包含成员和分数的redis.Z结构体数组
//
// 参数：
//
//	ctx: 上下文
//	key: 有序集合的键名
//	start: 起始排名（包含）
//	stop: 结束排名（包含）
//
// 返回值：
//
//	[]redis.Z: 成员和分数的数组，如果客户端未初始化则返回错误
//	error: 操作错误
//
// 使用场景：
//   - 获取排行榜前N名及其分数
//   - 分页查询有序集合数据
//   - 数据分析统计
func (c *Client) ZRangeWithScores(ctx context.Context, key string, start int64, stop int64) ([]redis.Z, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("redis client not initialized")
	}
	return c.rdb.ZRangeWithScores(ctx, key, start, stop).Result()
}

// Expire 为键设置过期时间
// 过期后键会自动被删除，适用于临时数据存储
//
// 参数：
//
//	ctx: 上下文
//	key: 要设置过期时间的键名
//	ttl: 过期时间，如：time.Hour * 24 表示24小时
//
// 返回值：
//
//	error: 操作错误，如果客户端未初始化则返回nil
//
// 使用场景：
//   - 缓存数据设置过期时间
//   - 临时会话存储
//   - 限流计数器
func (c *Client) Expire(ctx context.Context, key string, ttl time.Duration) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.Expire(ctx, key, ttl).Err()
}

// ZUnionStore 计算多个有序集合的并集，并将结果存储到新的有序集合中
// 可以指定聚合函数来处理相同成员的分数（SUM、MIN、MAX）
//
// 参数：
//
//	ctx: 上下文
//	dst: 目标有序集合的键名，用于存储并集结果
//	keys: 要计算并集的有序集合键名数组
//	aggregate: 聚合函数，可选值："SUM"（求和）、"MIN"（最小值）、"MAX"（最大值）
//
// 返回值：
//
//	error: 操作错误，如果客户端未初始化则返回nil
//
// 使用场景：
//   - 多维度排行榜合并
//   - 跨数据源统计
//   - 复杂的数据聚合计算
func (c *Client) ZUnionStore(ctx context.Context, dst string, keys []string, aggregate string) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZUnionStore(ctx, dst, &redis.ZStore{
		Keys:      keys,
		Aggregate: aggregate,
	}).Err()
}

// Exists 检查键是否存在
// 可以用于判断缓存是否有效或数据是否已加载
//
// 参数：
//
//	ctx: 上下文
//	key: 要检查的键名
//
// 返回值：
//
//	bool: 键是否存在
//	error: 操作错误，如果客户端未初始化则返回false和nil
//
// 使用场景：
//   - 缓存命中检查
//   - 数据存在性验证
//   - 防重复处理
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	if c == nil || c.rdb == nil {
		return false, nil
	}
	n, err := c.rdb.Exists(ctx, key).Result()
	return n > 0, err
}

// ZRevRange 返回有序集合中指定排名区间内的成员（按分数从高到低排序）
// 与ZRange相反，ZRevRange返回分数最高的成员在前
// 只返回成员名称，不包含分数
//
// 参数：
//
//	ctx: 上下文
//	key: 有序集合的键名
//	start: 起始排名（包含），0表示分数最高的成员
//	stop: 结束排名（包含），-1表示分数最低的成员
//
// 返回值：
//
//	[]string: 成员名称数组，如果客户端未初始化则返回nil
//	error: 操作错误
//
// 使用场景：
//   - 获取排行榜（分数从高到低）
//   - 热门内容推荐
//   - 实时数据展示
func (c *Client) ZRevRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	return c.rdb.ZRevRange(ctx, key, start, stop).Result()
}

// ZRevRangeByScore 返回有序集合中分数在指定区间内的成员（按分数从高到低排序）
// 支持分页查询，可以指定偏移量和返回数量
// 只返回成员名称，不包含分数
//
// 参数：
//
//	ctx: 上下文
//	key: 有序集合的键名
//	max: 分数上限（包含），如："100"、"+inf"表示正无穷
//	min: 分数下限（包含），如："0"、"-inf"表示负无穷
//	offset: 偏移量，跳过前N个匹配的成员
//	count: 返回的成员数量
//
// 返回值：
//
//	[]string: 成员名称数组，如果客户端未初始化则返回nil
//	error: 操作错误
//
// 使用场景：
//   - 按分数范围查询（如：查询积分在100-500之间的用户）
//   - 分页查询特定分数区间的数据
//   - 数据筛选和过滤
func (c *Client) ZRevRangeByScore(ctx context.Context, key string, max, min string, offset, count int64) ([]string, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	return c.rdb.ZRevRangeByScore(ctx, key, &redis.ZRangeBy{
		Max:    max,
		Min:    min,
		Offset: offset,
		Count:  count,
	}).Result()
}
