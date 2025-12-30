//! Nostos Redis Extension
//!
//! Synchronous Redis client for Nostos. All operations block until completion
//! and return results directly.
//!
//! # Usage Pattern
//!
//! ```nostos
//! # Connect
//! conn = redisConnect("redis://127.0.0.1/")
//!
//! # Operations return Result tuples directly
//! result = redisSet(conn, "key", "value")  # ("ok", "OK") or ("error", msg)
//! result = redisGet(conn, "key")           # ("ok", value) or ("error", msg)
//! ```

use nostos_extension::*;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use std::sync::Arc;

declare_extension!("redis", "0.1.0", register);

// Type IDs for GC handles
const TYPE_CONNECTION: u64 = 1;

// Cleanup function - ConnectionManager is Arc-based, just drop it
fn redis_cleanup(ptr: usize, type_id: u64) {
    if type_id == TYPE_CONNECTION {
        unsafe {
            let _ = Box::from_raw(ptr as *mut ConnectionManager);
        }
    }
}

fn register(reg: &mut ExtRegistry) {
    // Connection management
    reg.add("Redis.connect", redis_connect);

    // Key-value operations
    reg.add("Redis.get", redis_get);
    reg.add("Redis.set", redis_set);
    reg.add("Redis.del", redis_del);
    reg.add("Redis.exists", redis_exists);

    // Key operations
    reg.add("Redis.keys", redis_keys);
    reg.add("Redis.expire", redis_expire);
    reg.add("Redis.ttl", redis_ttl);

    // List operations
    reg.add("Redis.lpush", redis_lpush);
    reg.add("Redis.rpush", redis_rpush);
    reg.add("Redis.lpop", redis_lpop);
    reg.add("Redis.rpop", redis_rpop);
    reg.add("Redis.lrange", redis_lrange);
    reg.add("Redis.llen", redis_llen);

    // Hash operations
    reg.add("Redis.hset", redis_hset);
    reg.add("Redis.hget", redis_hget);
    reg.add("Redis.hdel", redis_hdel);
    reg.add("Redis.hgetall", redis_hgetall);

    // Set operations
    reg.add("Redis.sadd", redis_sadd);
    reg.add("Redis.srem", redis_srem);
    reg.add("Redis.smembers", redis_smembers);
    reg.add("Redis.sismember", redis_sismember);

    // Pub/Sub
    reg.add("Redis.publish", redis_publish);

    // Utility
    reg.add("Redis.ping", redis_ping);
    reg.add("Redis.flushdb", redis_flushdb);
}

// Helper to extract connection from GcHandle
fn get_conn(value: &Value) -> Result<ConnectionManager, String> {
    let handle = value.as_gc_handle()?;
    if handle.type_id != TYPE_CONNECTION {
        return Err("Expected Redis connection handle".to_string());
    }
    unsafe {
        let conn = &*(handle.ptr as *const ConnectionManager);
        Ok(conn.clone()) // ConnectionManager is cheap to clone
    }
}

// Helper to create connection handle
fn conn_handle(conn: ConnectionManager) -> Value {
    Value::gc_handle(Box::new(conn), TYPE_CONNECTION, redis_cleanup)
}

// Convert Redis value to Nostos Value
fn redis_to_value(rv: redis::Value) -> Value {
    match rv {
        redis::Value::Nil => Value::None,
        redis::Value::Int(i) => Value::Int(i),
        redis::Value::BulkString(bytes) => {
            match String::from_utf8(bytes) {
                Ok(s) => Value::String(Arc::new(s)),
                Err(e) => Value::Bytes(Arc::new(e.into_bytes())),
            }
        }
        redis::Value::Array(arr) => {
            Value::List(Arc::new(arr.into_iter().map(redis_to_value).collect()))
        }
        redis::Value::SimpleString(s) => Value::String(Arc::new(s)),
        redis::Value::Okay => Value::String(Arc::new("OK".to_string())),
        redis::Value::Map(map) => {
            let pairs: Vec<Value> = map
                .into_iter()
                .map(|(k, v)| Value::Tuple(Arc::new(vec![redis_to_value(k), redis_to_value(v)])))
                .collect();
            Value::List(Arc::new(pairs))
        }
        redis::Value::Double(f) => Value::Float(f),
        redis::Value::Boolean(b) => Value::Bool(b),
        redis::Value::VerbatimString { format: _, text } => Value::String(Arc::new(text)),
        redis::Value::BigNumber(n) => Value::String(Arc::new(n.to_string())),
        redis::Value::Set(set) => {
            Value::List(Arc::new(set.into_iter().map(redis_to_value).collect()))
        }
        redis::Value::Attribute { data, attributes: _ } => redis_to_value(*data),
        redis::Value::Push { kind: _, data } => {
            Value::List(Arc::new(data.into_iter().map(redis_to_value).collect()))
        }
        redis::Value::ServerError(e) => Value::String(Arc::new(format!("Error: {}", e.details().unwrap_or("unknown")))),
    }
}

// Wrap result for reply
fn result_to_value(result: Result<Value, String>) -> Value {
    match result {
        Ok(v) => Value::Tuple(Arc::new(vec![Value::String(Arc::new("ok".to_string())), v])),
        Err(e) => Value::Tuple(Arc::new(vec![Value::String(Arc::new("error".to_string())), Value::String(Arc::new(e))])),
    }
}

//=============================================================================
// Connection Management
//=============================================================================

/// Connect to Redis synchronously (blocks briefly during initial connection)
/// Returns a connection handle that can be used for async operations.
///
/// Args: [url: String]
/// Returns: Connection handle (GcHandle)
fn redis_connect(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let url = args[0].as_string()?;

    // We need to establish the connection - use the tokio handle
    let handle = ctx.tokio_handle();
    let conn = handle.block_on(async {
        let client = redis::Client::open(url)
            .map_err(|e| format!("Failed to create Redis client: {}", e))?;
        ConnectionManager::new(client)
            .await
            .map_err(|e| format!("Failed to connect to Redis: {}", e))
    })?;

    Ok(conn_handle(conn))
}

//=============================================================================
// Key-Value Operations
//=============================================================================

/// GET key - retrieve value for key
/// Args: [conn, key: String]
fn redis_get(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let value: redis::Value = conn.get(&key).await
            .map_err(|e| format!("GET failed: {}", e))?;
        Ok::<_, String>(redis_to_value(value))
    });

    Ok(result_to_value(result))
}

/// SET key value - set key to value
/// Args: [conn, key: String, value: String]
fn redis_set(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let value = args[2].as_string()?.to_string();
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let _: () = conn.set(&key, &value).await
            .map_err(|e| format!("SET failed: {}", e))?;
        Ok::<_, String>(Value::String(Arc::new("OK".to_string())))
    });

    Ok(result_to_value(result))
}

/// DEL keys - delete one or more keys
/// Args: [conn, key: String] or [conn, keys: List[String]]
fn redis_del(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let handle = ctx.tokio_handle();

    let keys: Vec<String> = if let Ok(key) = args[1].as_string() {
        vec![key.to_string()]
    } else if let Ok(list) = args[1].as_list() {
        list.iter()
            .map(|v| v.as_string().map(|s| s.to_string()))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        return Err("DEL: expected String or List[String]".to_string());
    };

    let result = handle.block_on(async {
        let count: i64 = conn.del(&keys).await
            .map_err(|e| format!("DEL failed: {}", e))?;
        Ok::<_, String>(Value::Int(count))
    });

    Ok(result_to_value(result))
}

/// EXISTS key - check if key exists
/// Args: [conn, key: String]
fn redis_exists(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let exists: bool = conn.exists(&key).await
            .map_err(|e| format!("EXISTS failed: {}", e))?;
        Ok::<_, String>(Value::Bool(exists))
    });

    Ok(result_to_value(result))
}

//=============================================================================
// Key Operations
//=============================================================================

/// KEYS pattern - find keys matching pattern
/// Args: [conn, pattern: String]
fn redis_keys(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let pattern = args[1].as_string()?.to_string();
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let keys: Vec<String> = conn.keys(&pattern).await
            .map_err(|e| format!("KEYS failed: {}", e))?;
        Ok::<_, String>(Value::List(Arc::new(keys.into_iter().map(|s| Value::String(Arc::new(s))).collect())))
    });

    Ok(result_to_value(result))
}

/// EXPIRE key seconds - set TTL on key
/// Args: [conn, key: String, seconds: Int]
fn redis_expire(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let seconds = args[2].as_i64()?;
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let set: bool = conn.expire(&key, seconds).await
            .map_err(|e| format!("EXPIRE failed: {}", e))?;
        Ok::<_, String>(Value::Bool(set))
    });

    Ok(result_to_value(result))
}

/// TTL key - get remaining TTL
/// Args: [conn, key: String]
fn redis_ttl(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let ttl: i64 = conn.ttl(&key).await
            .map_err(|e| format!("TTL failed: {}", e))?;
        Ok::<_, String>(Value::Int(ttl))
    });

    Ok(result_to_value(result))
}

//=============================================================================
// List Operations
//=============================================================================

/// LPUSH key value - push to head of list
/// Args: [conn, key: String, value: String]
fn redis_lpush(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let value = args[2].as_string()?.to_string();
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let len: i64 = conn.lpush(&key, &value).await
            .map_err(|e| format!("LPUSH failed: {}", e))?;
        Ok::<_, String>(Value::Int(len))
    });

    Ok(result_to_value(result))
}

/// RPUSH key value - push to tail of list
/// Args: [conn, key: String, value: String]
fn redis_rpush(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let value = args[2].as_string()?.to_string();
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let len: i64 = conn.rpush(&key, &value).await
            .map_err(|e| format!("RPUSH failed: {}", e))?;
        Ok::<_, String>(Value::Int(len))
    });

    Ok(result_to_value(result))
}

/// LPOP key - pop from head of list
/// Args: [conn, key: String]
fn redis_lpop(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let value: redis::Value = conn.lpop(&key, None).await
            .map_err(|e| format!("LPOP failed: {}", e))?;
        Ok::<_, String>(redis_to_value(value))
    });

    Ok(result_to_value(result))
}

/// RPOP key - pop from tail of list
/// Args: [conn, key: String]
fn redis_rpop(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let value: redis::Value = conn.rpop(&key, None).await
            .map_err(|e| format!("RPOP failed: {}", e))?;
        Ok::<_, String>(redis_to_value(value))
    });

    Ok(result_to_value(result))
}

/// LRANGE key start stop - get range of list elements
/// Args: [conn, key: String, start: Int, stop: Int]
fn redis_lrange(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let start = args[2].as_i64()? as isize;
    let stop = args[3].as_i64()? as isize;
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let values: Vec<String> = conn.lrange(&key, start, stop).await
            .map_err(|e| format!("LRANGE failed: {}", e))?;
        Ok::<_, String>(Value::List(Arc::new(values.into_iter().map(|s| Value::String(Arc::new(s))).collect())))
    });

    Ok(result_to_value(result))
}

/// LLEN key - get list length
/// Args: [conn, key: String]
fn redis_llen(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let len: i64 = conn.llen(&key).await
            .map_err(|e| format!("LLEN failed: {}", e))?;
        Ok::<_, String>(Value::Int(len))
    });

    Ok(result_to_value(result))
}

//=============================================================================
// Hash Operations
//=============================================================================

/// HSET key field value - set hash field
/// Args: [conn, key: String, field: String, value: String]
fn redis_hset(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let field = args[2].as_string()?.to_string();
    let value = args[3].as_string()?.to_string();
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let _: () = conn.hset(&key, &field, &value).await
            .map_err(|e| format!("HSET failed: {}", e))?;
        Ok::<_, String>(Value::String(Arc::new("OK".to_string())))
    });

    Ok(result_to_value(result))
}

/// HGET key field - get hash field
/// Args: [conn, key: String, field: String]
fn redis_hget(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let field = args[2].as_string()?.to_string();
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let value: redis::Value = conn.hget(&key, &field).await
            .map_err(|e| format!("HGET failed: {}", e))?;
        Ok::<_, String>(redis_to_value(value))
    });

    Ok(result_to_value(result))
}

/// HDEL key field - delete hash field
/// Args: [conn, key: String, field: String]
fn redis_hdel(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let field = args[2].as_string()?.to_string();
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let count: i64 = conn.hdel(&key, &field).await
            .map_err(|e| format!("HDEL failed: {}", e))?;
        Ok::<_, String>(Value::Int(count))
    });

    Ok(result_to_value(result))
}

/// HGETALL key - get all hash fields and values
/// Args: [conn, key: String]
fn redis_hgetall(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let map: Vec<(String, String)> = conn.hgetall(&key).await
            .map_err(|e| format!("HGETALL failed: {}", e))?;
        let pairs: Vec<Value> = map.into_iter()
            .map(|(k, v)| Value::Tuple(Arc::new(vec![
                Value::String(Arc::new(k)),
                Value::String(Arc::new(v))
            ])))
            .collect();
        Ok::<_, String>(Value::List(Arc::new(pairs)))
    });

    Ok(result_to_value(result))
}

//=============================================================================
// Set Operations
//=============================================================================

/// SADD key member - add member to set
/// Args: [conn, key: String, member: String]
fn redis_sadd(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let member = args[2].as_string()?.to_string();
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let added: i64 = conn.sadd(&key, &member).await
            .map_err(|e| format!("SADD failed: {}", e))?;
        Ok::<_, String>(Value::Int(added))
    });

    Ok(result_to_value(result))
}

/// SREM key member - remove member from set
/// Args: [conn, key: String, member: String]
fn redis_srem(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let member = args[2].as_string()?.to_string();
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let removed: i64 = conn.srem(&key, &member).await
            .map_err(|e| format!("SREM failed: {}", e))?;
        Ok::<_, String>(Value::Int(removed))
    });

    Ok(result_to_value(result))
}

/// SMEMBERS key - get all set members
/// Args: [conn, key: String]
fn redis_smembers(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let members: Vec<String> = conn.smembers(&key).await
            .map_err(|e| format!("SMEMBERS failed: {}", e))?;
        Ok::<_, String>(Value::List(Arc::new(members.into_iter().map(|s| Value::String(Arc::new(s))).collect())))
    });

    Ok(result_to_value(result))
}

/// SISMEMBER key member - check if member is in set
/// Args: [conn, key: String, member: String]
fn redis_sismember(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let member = args[2].as_string()?.to_string();
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let is_member: bool = conn.sismember(&key, &member).await
            .map_err(|e| format!("SISMEMBER failed: {}", e))?;
        Ok::<_, String>(Value::Bool(is_member))
    });

    Ok(result_to_value(result))
}

//=============================================================================
// Pub/Sub
//=============================================================================

/// PUBLISH channel message - publish message to channel
/// Args: [conn, channel: String, message: String]
fn redis_publish(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let channel = args[1].as_string()?.to_string();
    let message = args[2].as_string()?.to_string();
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let receivers: i64 = conn.publish(&channel, &message).await
            .map_err(|e| format!("PUBLISH failed: {}", e))?;
        Ok::<_, String>(Value::Int(receivers))
    });

    Ok(result_to_value(result))
}

//=============================================================================
// Utility
//=============================================================================

/// PING - test connection
/// Args: [conn]
fn redis_ping(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let pong: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(|e| format!("PING failed: {}", e))?;
        Ok::<_, String>(Value::String(Arc::new(pong)))
    });

    Ok(result_to_value(result))
}

/// FLUSHDB - clear current database (use with caution!)
/// Args: [conn]
fn redis_flushdb(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let handle = ctx.tokio_handle();

    let result = handle.block_on(async {
        let _: () = redis::cmd("FLUSHDB")
            .query_async(&mut conn)
            .await
            .map_err(|e| format!("FLUSHDB failed: {}", e))?;
        Ok::<_, String>(Value::String(Arc::new("OK".to_string())))
    });

    Ok(result_to_value(result))
}
