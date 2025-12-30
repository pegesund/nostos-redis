# nostos-redis

Redis extension for the [Nostos](https://github.com/pegesund/nostos) programming language.

## Features

- Key-value operations: GET, SET, DEL, EXISTS
- Key operations: KEYS, EXPIRE, TTL
- List operations: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN
- Hash operations: HSET, HGET, HDEL, HGETALL
- Set operations: SADD, SREM, SMEMBERS, SISMEMBER
- Pub/Sub: PUBLISH
- Utility: PING, FLUSHDB

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
    # Connect to Redis
    conn = redisConnect("redis://127.0.0.1/")

    # Set and get a value
    ("ok", _) = redisSet(conn, "mykey", "hello")
    ("ok", value) = redisGet(conn, "mykey")
    println("Got: " ++ value)  # Got: hello

    # Check if key exists
    ("ok", exists) = redisExists(conn, "mykey")
    println("Exists: " ++ show(exists))  # Exists: true

    # List operations
    redisLpush(conn, "mylist", "item1")
    redisLpush(conn, "mylist", "item2")
    ("ok", items) = redisLrange(conn, "mylist", 0, -1)
    println("List: " ++ show(items))  # List: [item2, item1]

    # Hash operations
    redisHset(conn, "user:1", "name", "Alice")
    ("ok", name) = redisHget(conn, "user:1", "name")
    println("Name: " ++ name)  # Name: Alice

    # Cleanup
    redisDel(conn, "mykey")
    redisDel(conn, "mylist")
    redisDel(conn, "user:1")

    0
}
```

## API

All operations return a result tuple: `("ok", value)` on success or `("error", msg)` on failure.

### Helper Functions

```nostos
# Unwrap a result, throwing on error
value = unwrapRedis(redisGet(conn, "key"))

# Check if result is ok
if isRedisOk(result) then ...

# Get error message (returns Option)
match redisError(result) {
    Some(msg) -> println("Error: " ++ msg)
    None -> println("Success!")
}
```

## Requirements

- Nostos with extension support
- Redis server running

## License

MIT
