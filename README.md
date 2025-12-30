# nostos-redis

Redis extension for the [Nostos](https://github.com/pegesund/nostos) programming language.

## Features

- **Non-blocking async operations** - Redis operations run asynchronously using Tokio and deliver results via Nostos message passing
- **Key-value**: GET, SET, DEL, EXISTS
- **Key operations**: KEYS, EXPIRE, TTL, RENAME, TYPE
- **Counter operations**: INCR, INCRBY, DECR, DECRBY
- **List operations**: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN
- **Hash operations**: HSET, HGET, HDEL, HGETALL, HMSET, HMGET, HINCRBY, HKEYS, HVALS, HLEN, HEXISTS
- **Set operations**: SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SUNION, SINTER, SDIFF
- **Sorted sets**: ZADD, ZSCORE, ZRANK, ZRANGE, ZREVRANGE, ZINCRBY, ZCARD, ZCOUNT
- **Streams**: XADD, XLEN, XRANGE, XREAD + Consumer Groups (XGROUP, XREADGROUP, XACK)
- **Pub/Sub**: PUBLISH, SUBSCRIBE, PSUBSCRIBE
- **Utility**: PING, FLUSHDB

## Installation

Add to your project's `nostos.toml`:

```toml
[extensions]
redis = { git = "https://github.com/pegesund/nostos-redis" }
```

Or build manually:

```bash
cargo build --release
```

## Usage

```nostos
import redis
use redis.*

main() = {
    # Connect to Redis (synchronous - blocks briefly)
    conn = redisConnect("redis://127.0.0.1/")

    # Set and get a value
    ("ok", _) = redisSet(conn, "mykey", "hello")
    ("ok", value) = redisGet(conn, "mykey")
    println("Got: " ++ value)  # Got: hello

    # Check if key exists
    ("ok", exists) = redisExists(conn, "mykey")
    println("Exists: " ++ show(exists))  # Exists: true

    # Counter operations
    redisSet(conn, "counter", "0")
    ("ok", v1) = redisIncr(conn, "counter")      # v1 = 1
    ("ok", v2) = redisIncrby(conn, "counter", 5) # v2 = 6
    ("ok", v3) = redisDecr(conn, "counter")      # v3 = 5

    # List operations
    redisLpush(conn, "mylist", "item1")
    redisLpush(conn, "mylist", "item2")
    ("ok", items) = redisLrange(conn, "mylist", 0, -1)
    println("List: " ++ show(items))  # List: [item2, item1]

    # Hash operations
    redisHset(conn, "user:1", "name", "Alice")
    redisHmset(conn, "user:1", [("age", "30"), ("city", "NYC")])
    ("ok", name) = redisHget(conn, "user:1", "name")
    ("ok", values) = redisHmget(conn, "user:1", ["name", "age"])

    # Set operations
    redisSadd(conn, "set1", "a")
    redisSadd(conn, "set1", "b")
    redisSadd(conn, "set2", "b")
    redisSadd(conn, "set2", "c")
    ("ok", union) = redisSunion(conn, ["set1", "set2"])  # [a, b, c]
    ("ok", inter) = redisSinter(conn, ["set1", "set2"])  # [b]

    # Sorted sets (leaderboard example)
    redisZadd(conn, "scores", 100.0, "alice")
    redisZadd(conn, "scores", 85.0, "bob")
    redisZadd(conn, "scores", 92.0, "charlie")
    ("ok", top3) = redisZrevrange(conn, "scores", 0, 2)  # [alice, charlie, bob]
    ("ok", aliceScore) = redisZscore(conn, "scores", "alice")  # 100.0

    # Streams
    ("ok", id) = redisXadd(conn, "events", [("type", "click"), ("page", "/home")])
    ("ok", entries) = redisXrange(conn, "events", "-", "+")

    # Cleanup
    redisDel(conn, "mykey")
    redisDel(conn, "mylist")
    redisDel(conn, "user:1")

    0
}
```

## Pub/Sub Example

```nostos
import redis
use redis.*

# Subscriber process
subscriber() = {
    redisSubscribe("redis://127.0.0.1/", "notifications")
    loop()
}

loop() = {
    receive {
        ("message", channel, payload) -> {
            println("Got message on " ++ channel ++ ": " ++ payload)
            loop()
        }
    }
}

# Publisher (in another process)
publisher() = {
    conn = redisConnect("redis://127.0.0.1/")
    redisPublish(conn, "notifications", "Hello subscribers!")
}

main() = {
    spawn subscriber()
    sleep(100)  # Give subscriber time to connect
    publisher()
    sleep(100)  # Give message time to be delivered
    0
}
```

## API

### Blocking API (recommended for simple use)

All operations return a result tuple: `("ok", value)` on success or `("error", msg)` on failure.

```nostos
# These functions call Redis and wait for the result
("ok", value) = redisGet(conn, "key")
("ok", _) = redisSet(conn, "key", "value")
```

### Async API (for concurrent operations)

For advanced use cases where you want to fire off multiple Redis operations concurrently:

```nostos
# Fire off multiple operations without waiting
redisSetAsync(conn, "key1", "value1")
redisSetAsync(conn, "key2", "value2")
redisSetAsync(conn, "key3", "value3")

# Collect all results
result1 = receive { msg -> msg }
result2 = receive { msg -> msg }
result3 = receive { msg -> msg }
```

### Complete API Reference

#### Connection
- `redisConnect(url: String) -> Conn` - Connect to Redis

#### Key-Value
- `redisGet(conn, key)` - Get value
- `redisSet(conn, key, value)` - Set value
- `redisDel(conn, key)` - Delete key
- `redisExists(conn, key)` - Check if key exists

#### Key Operations
- `redisKeys(conn, pattern)` - Find keys matching pattern
- `redisExpire(conn, key, seconds)` - Set TTL
- `redisTtl(conn, key)` - Get remaining TTL
- `redisRename(conn, key, newkey)` - Rename key
- `redisType(conn, key)` - Get key type

#### Counters
- `redisIncr(conn, key)` - Increment by 1
- `redisIncrby(conn, key, amount)` - Increment by amount
- `redisDecr(conn, key)` - Decrement by 1
- `redisDecrby(conn, key, amount)` - Decrement by amount

#### Lists
- `redisLpush(conn, key, value)` - Push to head
- `redisRpush(conn, key, value)` - Push to tail
- `redisLpop(conn, key)` - Pop from head
- `redisRpop(conn, key)` - Pop from tail
- `redisLrange(conn, key, start, stop)` - Get range
- `redisLlen(conn, key)` - Get length

#### Hashes
- `redisHset(conn, key, field, value)` - Set field
- `redisHget(conn, key, field)` - Get field
- `redisHdel(conn, key, field)` - Delete field
- `redisHgetall(conn, key)` - Get all fields and values
- `redisHmset(conn, key, [(field, value), ...])` - Set multiple fields
- `redisHmget(conn, key, [field1, field2, ...])` - Get multiple fields
- `redisHincrby(conn, key, field, amount)` - Increment field
- `redisHkeys(conn, key)` - Get all field names
- `redisHvals(conn, key)` - Get all values
- `redisHlen(conn, key)` - Get number of fields
- `redisHexists(conn, key, field)` - Check if field exists

#### Sets
- `redisSadd(conn, key, member)` - Add member
- `redisSrem(conn, key, member)` - Remove member
- `redisSmembers(conn, key)` - Get all members
- `redisSismember(conn, key, member)` - Check membership
- `redisScard(conn, key)` - Get set size
- `redisSunion(conn, [key1, key2, ...])` - Union of sets
- `redisSinter(conn, [key1, key2, ...])` - Intersection of sets
- `redisSdiff(conn, [key1, key2, ...])` - Difference of sets

#### Sorted Sets
- `redisZadd(conn, key, score, member)` - Add with score
- `redisZscore(conn, key, member)` - Get score
- `redisZrank(conn, key, member)` - Get rank (0-indexed)
- `redisZrange(conn, key, start, stop)` - Get by rank (low to high)
- `redisZrevrange(conn, key, start, stop)` - Get by rank (high to low)
- `redisZincrby(conn, key, increment, member)` - Increment score
- `redisZcard(conn, key)` - Get count
- `redisZcount(conn, key, min, max)` - Count in score range

#### Streams
- `redisXadd(conn, stream, [(field, value), ...])` - Add entry
- `redisXlen(conn, stream)` - Get stream length
- `redisXrange(conn, stream, start, end)` - Get entries (use "-" and "+" for min/max)
- `redisXread(conn, [(stream, lastId), ...])` - Read entries ("0" for all, "$" for new)
- `redisXreadCount(conn, streams, count)` - Read with count limit
- `redisXreadBlock(conn, streams, count, blockMs)` - Blocking read

#### Stream Consumer Groups
- `redisXgroupCreate(conn, stream, group, startId)` - Create consumer group
- `redisXgroupCreateMkstream(conn, stream, group, startId)` - Create group + stream
- `redisXreadgroup(conn, group, consumer, [(stream, ">"), ...])` - Read as consumer
- `redisXreadgroupCount(conn, group, consumer, streams, count)` - With count limit
- `redisXreadgroupBlock(conn, group, consumer, streams, count, blockMs)` - Blocking
- `redisXack(conn, stream, group, [id1, id2, ...])` - Acknowledge messages
- `redisXinfoGroups(conn, stream)` - Get consumer group info

#### Pub/Sub
- `redisPublish(conn, channel, message)` - Publish message
- `redisSubscribe(url, channel)` - Subscribe to channel (spawns listener)
- `redisPsubscribe(url, pattern)` - Subscribe to pattern

#### Utility
- `redisPing(conn)` - Test connection
- `redisFlushdb(conn)` - Clear database (DANGER!)

### Helper Functions

```nostos
# Unwrap a result, throwing on error
value = unwrapRedis(redisGet(conn, "key"))

# Check if result is ok
if isRedisOk(result) then ...

# Get error message (returns Option)
match redisError(result) {
    Some(msg) -> println("Error: " ++ msg),
    None -> println("Success!")
}
```

## Running Tests

```bash
# Ensure Redis is running locally
redis-server &

# Run the extension tests
for f in tests/*.nos; do
    ./target/release/nostos --extension target/release/libnostos_redis.so "$f"
done
```

## Requirements

- Nostos with extension support
- Redis server running

## License

MIT
