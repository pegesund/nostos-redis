//! Nostos Redis Extension
//!
//! Async Redis client for Nostos. Operations are non-blocking and return
//! results via message passing to the calling process.
//!
//! # Usage Pattern
//!
//! ```nostos
//! # Connect (returns connection handle)
//! conn = redisConnect("redis://127.0.0.1/")
//!
//! # Start async operation (returns Unit immediately)
//! redisSet(conn, "key", "value")
//!
//! # Wait for result via receive
//! result = receive { msg -> msg }  # ("ok", "OK") or ("error", msg)
//! ```

use nostos_extension::*;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use futures::StreamExt;

declare_extension!("redis", "0.1.0", register);

// Type IDs for GC handles
const TYPE_CONNECTION: u64 = 1;
const TYPE_SUBSCRIPTION: u64 = 2;

// Subscription handle for pub/sub
struct Subscription {
    cancel: Arc<AtomicBool>,
}

// Cleanup function - ConnectionManager is Arc-based, just drop it
fn redis_cleanup(ptr: usize, type_id: u64) {
    match type_id {
        TYPE_CONNECTION => unsafe {
            let _ = Box::from_raw(ptr as *mut ConnectionManager);
        },
        TYPE_SUBSCRIPTION => unsafe {
            let sub = Box::from_raw(ptr as *mut Subscription);
            // Signal cancellation when subscription is garbage collected
            sub.cancel.store(true, Ordering::SeqCst);
        },
        _ => {}
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
    reg.add("Redis.rename", redis_rename);
    reg.add("Redis.type", redis_type);
    reg.add("Redis.scan", redis_scan);
    reg.add("Redis.persist", redis_persist);

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
    reg.add("Redis.hmset", redis_hmset);
    reg.add("Redis.hmget", redis_hmget);
    reg.add("Redis.hincrby", redis_hincrby);
    reg.add("Redis.hkeys", redis_hkeys);
    reg.add("Redis.hvals", redis_hvals);
    reg.add("Redis.hlen", redis_hlen);
    reg.add("Redis.hexists", redis_hexists);

    // Set operations
    reg.add("Redis.sadd", redis_sadd);
    reg.add("Redis.srem", redis_srem);
    reg.add("Redis.smembers", redis_smembers);
    reg.add("Redis.sismember", redis_sismember);
    reg.add("Redis.scard", redis_scard);
    reg.add("Redis.spop", redis_spop);
    reg.add("Redis.srandmember", redis_srandmember);
    reg.add("Redis.sunion", redis_sunion);
    reg.add("Redis.sinter", redis_sinter);
    reg.add("Redis.sdiff", redis_sdiff);

    // Sorted Set operations
    reg.add("Redis.zadd", redis_zadd);
    reg.add("Redis.zrem", redis_zrem);
    reg.add("Redis.zscore", redis_zscore);
    reg.add("Redis.zrank", redis_zrank);
    reg.add("Redis.zrange", redis_zrange);
    reg.add("Redis.zrevrange", redis_zrevrange);
    reg.add("Redis.zrangebyscore", redis_zrangebyscore);
    reg.add("Redis.zincrby", redis_zincrby);
    reg.add("Redis.zcard", redis_zcard);
    reg.add("Redis.zcount", redis_zcount);

    // Pub/Sub
    reg.add("Redis.publish", redis_publish);
    reg.add("Redis.subscribe", redis_subscribe);
    reg.add("Redis.psubscribe", redis_psubscribe);
    reg.add("Redis.unsubscribe", redis_unsubscribe);

    // Streams
    reg.add("Redis.xadd", redis_xadd);
    reg.add("Redis.xread", redis_xread);
    reg.add("Redis.xrange", redis_xrange);
    reg.add("Redis.xlen", redis_xlen);
    reg.add("Redis.xinfo_groups", redis_xinfo_groups);
    reg.add("Redis.xgroup_create", redis_xgroup_create);
    reg.add("Redis.xreadgroup", redis_xreadgroup);
    reg.add("Redis.xack", redis_xack);

    // Utility
    reg.add("Redis.ping", redis_ping);
    reg.add("Redis.flushdb", redis_flushdb);
    reg.add("Redis.incr", redis_incr);
    reg.add("Redis.incrby", redis_incrby);
    reg.add("Redis.decr", redis_decr);
    reg.add("Redis.decrby", redis_decrby);
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

// Helper to create subscription handle
fn sub_handle(sub: Subscription) -> Value {
    Value::gc_handle(Box::new(sub), TYPE_SUBSCRIPTION, redis_cleanup)
}

// Helper to get subscription from handle
fn get_sub(value: &Value) -> Result<Arc<AtomicBool>, String> {
    let handle = value.as_gc_handle()?;
    if handle.type_id != TYPE_SUBSCRIPTION {
        return Err("Expected subscription handle".to_string());
    }
    unsafe {
        let sub = &*(handle.ptr as *const Subscription);
        Ok(sub.cancel.clone())
    }
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

/// GET key - retrieve value for key (async)
/// Args: [conn, key: String]
/// Returns: Unit (result sent via message)
fn redis_get(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let value: redis::Value = conn.get(&key).await
                .map_err(|e| format!("GET failed: {}", e))?;
            Ok::<_, String>(redis_to_value(value))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// SET key value - set key to value (async)
/// Args: [conn, key: String, value: String]
/// Returns: Unit (result sent via message)
fn redis_set(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let value = args[2].as_string()?.to_string();
    
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let _: () = conn.set(&key, &value).await
                .map_err(|e| format!("SET failed: {}", e))?;
            Ok::<_, String>(Value::String(Arc::new("OK".to_string())))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// DEL keys - delete one or more keys (async)
/// Args: [conn, key: String] or [conn, keys: List[String]]
/// Returns: Unit (result sent via message)
fn redis_del(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    
    let ctx_clone = ctx.clone();

    let keys: Vec<String> = if let Ok(key) = args[1].as_string() {
        vec![key.to_string()]
    } else if let Ok(list) = args[1].as_list() {
        list.iter()
            .map(|v| v.as_string().map(|s| s.to_string()))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        return Err("DEL: expected String or List[String]".to_string());
    };

    ctx.spawn_async(async move {
        let result = async {
            let count: i64 = conn.del(&keys).await
                .map_err(|e| format!("DEL failed: {}", e))?;
            Ok::<_, String>(Value::Int(count))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// EXISTS key - check if key exists (async)
/// Args: [conn, key: String]
/// Returns: Unit (result sent via message)
fn redis_exists(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let exists: bool = conn.exists(&key).await
                .map_err(|e| format!("EXISTS failed: {}", e))?;
            Ok::<_, String>(Value::Bool(exists))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

//=============================================================================
// Key Operations
//=============================================================================

/// KEYS pattern - find keys matching pattern (async)
/// Args: [conn, pattern: String]
/// Returns: Unit (result sent via message)
fn redis_keys(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let pattern = args[1].as_string()?.to_string();
    
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let keys: Vec<String> = conn.keys(&pattern).await
                .map_err(|e| format!("KEYS failed: {}", e))?;
            Ok::<_, String>(Value::List(Arc::new(keys.into_iter().map(|s| Value::String(Arc::new(s))).collect())))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// EXPIRE key seconds - set TTL on key (async)
/// Args: [conn, key: String, seconds: Int]
/// Returns: Unit (result sent via message)
fn redis_expire(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let seconds = args[2].as_i64()?;
    
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let set: bool = conn.expire(&key, seconds).await
                .map_err(|e| format!("EXPIRE failed: {}", e))?;
            Ok::<_, String>(Value::Bool(set))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// TTL key - get remaining TTL (async)
/// Args: [conn, key: String]
/// Returns: Unit (result sent via message)
fn redis_ttl(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let ttl: i64 = conn.ttl(&key).await
                .map_err(|e| format!("TTL failed: {}", e))?;
            Ok::<_, String>(Value::Int(ttl))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

//=============================================================================
// List Operations
//=============================================================================

/// LPUSH key value - push to head of list (async)
/// Args: [conn, key: String, value: String]
/// Returns: Unit (result sent via message)
fn redis_lpush(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let value = args[2].as_string()?.to_string();
    
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let len: i64 = conn.lpush(&key, &value).await
                .map_err(|e| format!("LPUSH failed: {}", e))?;
            Ok::<_, String>(Value::Int(len))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// RPUSH key value - push to tail of list (async)
/// Args: [conn, key: String, value: String]
/// Returns: Unit (result sent via message)
fn redis_rpush(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let value = args[2].as_string()?.to_string();
    
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let len: i64 = conn.rpush(&key, &value).await
                .map_err(|e| format!("RPUSH failed: {}", e))?;
            Ok::<_, String>(Value::Int(len))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// LPOP key - pop from head of list (async)
/// Args: [conn, key: String]
/// Returns: Unit (result sent via message)
fn redis_lpop(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let value: redis::Value = conn.lpop(&key, None).await
                .map_err(|e| format!("LPOP failed: {}", e))?;
            Ok::<_, String>(redis_to_value(value))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// RPOP key - pop from tail of list (async)
/// Args: [conn, key: String]
/// Returns: Unit (result sent via message)
fn redis_rpop(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let value: redis::Value = conn.rpop(&key, None).await
                .map_err(|e| format!("RPOP failed: {}", e))?;
            Ok::<_, String>(redis_to_value(value))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// LRANGE key start stop - get range of list elements (async)
/// Args: [conn, key: String, start: Int, stop: Int]
/// Returns: Unit (result sent via message)
fn redis_lrange(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let start = args[2].as_i64()? as isize;
    let stop = args[3].as_i64()? as isize;
    
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let values: Vec<String> = conn.lrange(&key, start, stop).await
                .map_err(|e| format!("LRANGE failed: {}", e))?;
            Ok::<_, String>(Value::List(Arc::new(values.into_iter().map(|s| Value::String(Arc::new(s))).collect())))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// LLEN key - get list length (async)
/// Args: [conn, key: String]
/// Returns: Unit (result sent via message)
fn redis_llen(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let len: i64 = conn.llen(&key).await
                .map_err(|e| format!("LLEN failed: {}", e))?;
            Ok::<_, String>(Value::Int(len))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

//=============================================================================
// Hash Operations
//=============================================================================

/// HSET key field value - set hash field (async)
/// Args: [conn, key: String, field: String, value: String]
/// Returns: Unit (result sent via message)
fn redis_hset(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let field = args[2].as_string()?.to_string();
    let value = args[3].as_string()?.to_string();
    
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let _: () = conn.hset(&key, &field, &value).await
                .map_err(|e| format!("HSET failed: {}", e))?;
            Ok::<_, String>(Value::String(Arc::new("OK".to_string())))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// HGET key field - get hash field (async)
/// Args: [conn, key: String, field: String]
/// Returns: Unit (result sent via message)
fn redis_hget(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let field = args[2].as_string()?.to_string();
    
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let value: redis::Value = conn.hget(&key, &field).await
                .map_err(|e| format!("HGET failed: {}", e))?;
            Ok::<_, String>(redis_to_value(value))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// HDEL key field - delete hash field (async)
/// Args: [conn, key: String, field: String]
/// Returns: Unit (result sent via message)
fn redis_hdel(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let field = args[2].as_string()?.to_string();
    
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let count: i64 = conn.hdel(&key, &field).await
                .map_err(|e| format!("HDEL failed: {}", e))?;
            Ok::<_, String>(Value::Int(count))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// HGETALL key - get all hash fields and values (async)
/// Args: [conn, key: String]
/// Returns: Unit (result sent via message)
fn redis_hgetall(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let map: Vec<(String, String)> = conn.hgetall(&key).await
                .map_err(|e| format!("HGETALL failed: {}", e))?;
            let pairs: Vec<Value> = map.into_iter()
                .map(|(k, v)| Value::Tuple(Arc::new(vec![
                    Value::String(Arc::new(k)),
                    Value::String(Arc::new(v))
                ])))
                .collect();
            Ok::<_, String>(Value::List(Arc::new(pairs)))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

//=============================================================================
// Set Operations
//=============================================================================

/// SADD key member - add member to set (async)
/// Args: [conn, key: String, member: String]
/// Returns: Unit (result sent via message)
fn redis_sadd(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let member = args[2].as_string()?.to_string();
    
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let added: i64 = conn.sadd(&key, &member).await
                .map_err(|e| format!("SADD failed: {}", e))?;
            Ok::<_, String>(Value::Int(added))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// SREM key member - remove member from set (async)
/// Args: [conn, key: String, member: String]
/// Returns: Unit (result sent via message)
fn redis_srem(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let member = args[2].as_string()?.to_string();
    
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let removed: i64 = conn.srem(&key, &member).await
                .map_err(|e| format!("SREM failed: {}", e))?;
            Ok::<_, String>(Value::Int(removed))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// SMEMBERS key - get all set members (async)
/// Args: [conn, key: String]
/// Returns: Unit (result sent via message)
fn redis_smembers(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let members: Vec<String> = conn.smembers(&key).await
                .map_err(|e| format!("SMEMBERS failed: {}", e))?;
            Ok::<_, String>(Value::List(Arc::new(members.into_iter().map(|s| Value::String(Arc::new(s))).collect())))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// SISMEMBER key member - check if member is in set (async)
/// Args: [conn, key: String, member: String]
/// Returns: Unit (result sent via message)
fn redis_sismember(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let member = args[2].as_string()?.to_string();
    
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let is_member: bool = conn.sismember(&key, &member).await
                .map_err(|e| format!("SISMEMBER failed: {}", e))?;
            Ok::<_, String>(Value::Bool(is_member))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

//=============================================================================
// Pub/Sub
//=============================================================================

/// PUBLISH channel message - publish message to channel (async)
/// Args: [conn, channel: String, message: String]
/// Returns: Unit (result sent via message)
fn redis_publish(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let channel = args[1].as_string()?.to_string();
    let message = args[2].as_string()?.to_string();
    
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let receivers: i64 = conn.publish(&channel, &message).await
                .map_err(|e| format!("PUBLISH failed: {}", e))?;
            Ok::<_, String>(Value::Int(receivers))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

//=============================================================================
// Utility
//=============================================================================

/// PING - test connection (async)
/// Args: [conn]
/// Returns: Unit (result sent via message)
fn redis_ping(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let pong: String = redis::cmd("PING")
                .query_async(&mut conn)
                .await
                .map_err(|e| format!("PING failed: {}", e))?;
            Ok::<_, String>(Value::String(Arc::new(pong)))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// FLUSHDB - clear current database (use with caution!) (async)
/// Args: [conn]
/// Returns: Unit (result sent via message)
fn redis_flushdb(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let _: () = redis::cmd("FLUSHDB")
                .query_async(&mut conn)
                .await
                .map_err(|e| format!("FLUSHDB failed: {}", e))?;
            Ok::<_, String>(Value::String(Arc::new("OK".to_string())))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

//=============================================================================
// Additional Key Operations
//=============================================================================

/// RENAME key newkey - rename a key (async)
/// Args: [conn, key: String, newkey: String]
/// Returns: Unit (result sent via message)
fn redis_rename(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let newkey = args[2].as_string()?.to_string();

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let _: () = conn.rename(&key, &newkey).await
                .map_err(|e| format!("RENAME failed: {}", e))?;
            Ok::<_, String>(Value::String(Arc::new("OK".to_string())))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// TYPE key - get the type of key (async)
/// Args: [conn, key: String]
/// Returns: Unit (result sent via message) - returns string like "string", "list", "set", "zset", "hash", "stream"
fn redis_type(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let key_type: String = redis::cmd("TYPE")
                .arg(&key)
                .query_async(&mut conn)
                .await
                .map_err(|e| format!("TYPE failed: {}", e))?;
            Ok::<_, String>(Value::String(Arc::new(key_type)))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// SCAN cursor [MATCH pattern] [COUNT count] - incrementally iterate keys (async)
/// Args: [conn, cursor: Int, pattern: String, count: Int]
/// Returns: Unit (result sent via message) - returns (next_cursor, [keys])
fn redis_scan(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let cursor = args[1].as_i64()? as u64;
    let pattern = if args.len() > 2 { Some(args[2].as_string()?.to_string()) } else { None };
    let count = if args.len() > 3 { Some(args[3].as_i64()? as usize) } else { None };

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let mut cmd = redis::cmd("SCAN");
            cmd.arg(cursor);
            if let Some(p) = &pattern {
                cmd.arg("MATCH").arg(p);
            }
            if let Some(c) = count {
                cmd.arg("COUNT").arg(c);
            }
            let (next_cursor, keys): (u64, Vec<String>) = cmd
                .query_async(&mut conn)
                .await
                .map_err(|e| format!("SCAN failed: {}", e))?;

            let keys_list = Value::List(Arc::new(keys.into_iter().map(|s| Value::String(Arc::new(s))).collect()));
            Ok::<_, String>(Value::Tuple(Arc::new(vec![Value::Int(next_cursor as i64), keys_list])))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// PERSIST key - remove expiration from key (async)
/// Args: [conn, key: String]
/// Returns: Unit (result sent via message) - returns Bool
fn redis_persist(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let removed: bool = conn.persist(&key).await
                .map_err(|e| format!("PERSIST failed: {}", e))?;
            Ok::<_, String>(Value::Bool(removed))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

//=============================================================================
// Additional Hash Operations
//=============================================================================

/// HMSET key field value [field value ...] - set multiple hash fields (async)
/// Args: [conn, key: String, fields: List[(String, String)]]
/// Returns: Unit (result sent via message)
fn redis_hmset(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let fields_list = args[2].as_list()?;

    let mut fields: Vec<(String, String)> = Vec::new();
    for item in fields_list.iter() {
        let tuple = item.as_tuple()?;
        if tuple.len() != 2 {
            return Err("HMSET: expected list of (field, value) tuples".to_string());
        }
        fields.push((tuple[0].as_string()?.to_string(), tuple[1].as_string()?.to_string()));
    }

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let _: () = conn.hset_multiple(&key, &fields).await
                .map_err(|e| format!("HMSET failed: {}", e))?;
            Ok::<_, String>(Value::String(Arc::new("OK".to_string())))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// HMGET key field [field ...] - get multiple hash fields (async)
/// Args: [conn, key: String, fields: List[String]]
/// Returns: Unit (result sent via message) - returns List of values
fn redis_hmget(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let fields_list = args[2].as_list()?;

    let fields: Vec<String> = fields_list.iter()
        .map(|v| v.as_string().map(|s| s.to_string()))
        .collect::<Result<Vec<_>, _>>()?;

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let values: Vec<redis::Value> = conn.hget(&key, &fields).await
                .map_err(|e| format!("HMGET failed: {}", e))?;
            Ok::<_, String>(Value::List(Arc::new(values.into_iter().map(redis_to_value).collect())))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// HINCRBY key field increment - increment hash field by integer (async)
/// Args: [conn, key: String, field: String, increment: Int]
/// Returns: Unit (result sent via message) - returns new value
fn redis_hincrby(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let field = args[2].as_string()?.to_string();
    let increment = args[3].as_i64()?;

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let new_value: i64 = conn.hincr(&key, &field, increment).await
                .map_err(|e| format!("HINCRBY failed: {}", e))?;
            Ok::<_, String>(Value::Int(new_value))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// HKEYS key - get all hash field names (async)
/// Args: [conn, key: String]
/// Returns: Unit (result sent via message) - returns List[String]
fn redis_hkeys(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let keys: Vec<String> = conn.hkeys(&key).await
                .map_err(|e| format!("HKEYS failed: {}", e))?;
            Ok::<_, String>(Value::List(Arc::new(keys.into_iter().map(|s| Value::String(Arc::new(s))).collect())))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// HVALS key - get all hash values (async)
/// Args: [conn, key: String]
/// Returns: Unit (result sent via message) - returns List[String]
fn redis_hvals(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let vals: Vec<String> = conn.hvals(&key).await
                .map_err(|e| format!("HVALS failed: {}", e))?;
            Ok::<_, String>(Value::List(Arc::new(vals.into_iter().map(|s| Value::String(Arc::new(s))).collect())))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// HLEN key - get number of hash fields (async)
/// Args: [conn, key: String]
/// Returns: Unit (result sent via message) - returns Int
fn redis_hlen(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let len: i64 = conn.hlen(&key).await
                .map_err(|e| format!("HLEN failed: {}", e))?;
            Ok::<_, String>(Value::Int(len))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// HEXISTS key field - check if hash field exists (async)
/// Args: [conn, key: String, field: String]
/// Returns: Unit (result sent via message) - returns Bool
fn redis_hexists(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let field = args[2].as_string()?.to_string();

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let exists: bool = conn.hexists(&key, &field).await
                .map_err(|e| format!("HEXISTS failed: {}", e))?;
            Ok::<_, String>(Value::Bool(exists))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

//=============================================================================
// Additional Set Operations
//=============================================================================

/// SCARD key - get set cardinality (size) (async)
/// Args: [conn, key: String]
/// Returns: Unit (result sent via message) - returns Int
fn redis_scard(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let count: i64 = conn.scard(&key).await
                .map_err(|e| format!("SCARD failed: {}", e))?;
            Ok::<_, String>(Value::Int(count))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// SPOP key [count] - remove and return random member(s) (async)
/// Args: [conn, key: String] or [conn, key: String, count: Int]
/// Returns: Unit (result sent via message) - returns String or List[String]
fn redis_spop(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let count = if args.len() > 2 { Some(args[2].as_i64()? as usize) } else { None };

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            if let Some(c) = count {
                // Use raw command for SPOP with count
                let members: Vec<String> = redis::cmd("SPOP")
                    .arg(&key)
                    .arg(c)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| format!("SPOP failed: {}", e))?;
                Ok::<_, String>(Value::List(Arc::new(members.into_iter().map(|s| Value::String(Arc::new(s))).collect())))
            } else {
                let member: redis::Value = conn.spop(&key).await
                    .map_err(|e| format!("SPOP failed: {}", e))?;
                Ok::<_, String>(redis_to_value(member))
            }
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// SRANDMEMBER key [count] - get random member(s) without removing (async)
/// Args: [conn, key: String] or [conn, key: String, count: Int]
/// Returns: Unit (result sent via message) - returns String or List[String]
fn redis_srandmember(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let count = if args.len() > 2 { Some(args[2].as_i64()?) } else { None };

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            if let Some(c) = count {
                // Use raw command for SRANDMEMBER with count
                let members: Vec<String> = redis::cmd("SRANDMEMBER")
                    .arg(&key)
                    .arg(c)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| format!("SRANDMEMBER failed: {}", e))?;
                Ok::<_, String>(Value::List(Arc::new(members.into_iter().map(|s| Value::String(Arc::new(s))).collect())))
            } else {
                let member: redis::Value = conn.srandmember(&key).await
                    .map_err(|e| format!("SRANDMEMBER failed: {}", e))?;
                Ok::<_, String>(redis_to_value(member))
            }
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// SUNION key [key ...] - return union of sets (async)
/// Args: [conn, keys: List[String]]
/// Returns: Unit (result sent via message) - returns List[String]
fn redis_sunion(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let keys_list = args[1].as_list()?;
    let keys: Vec<String> = keys_list.iter()
        .map(|v| v.as_string().map(|s| s.to_string()))
        .collect::<Result<Vec<_>, _>>()?;

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let members: Vec<String> = conn.sunion(&keys).await
                .map_err(|e| format!("SUNION failed: {}", e))?;
            Ok::<_, String>(Value::List(Arc::new(members.into_iter().map(|s| Value::String(Arc::new(s))).collect())))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// SINTER key [key ...] - return intersection of sets (async)
/// Args: [conn, keys: List[String]]
/// Returns: Unit (result sent via message) - returns List[String]
fn redis_sinter(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let keys_list = args[1].as_list()?;
    let keys: Vec<String> = keys_list.iter()
        .map(|v| v.as_string().map(|s| s.to_string()))
        .collect::<Result<Vec<_>, _>>()?;

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let members: Vec<String> = conn.sinter(&keys).await
                .map_err(|e| format!("SINTER failed: {}", e))?;
            Ok::<_, String>(Value::List(Arc::new(members.into_iter().map(|s| Value::String(Arc::new(s))).collect())))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// SDIFF key [key ...] - return difference of sets (async)
/// Args: [conn, keys: List[String]]
/// Returns: Unit (result sent via message) - returns List[String]
fn redis_sdiff(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let keys_list = args[1].as_list()?;
    let keys: Vec<String> = keys_list.iter()
        .map(|v| v.as_string().map(|s| s.to_string()))
        .collect::<Result<Vec<_>, _>>()?;

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let members: Vec<String> = conn.sdiff(&keys).await
                .map_err(|e| format!("SDIFF failed: {}", e))?;
            Ok::<_, String>(Value::List(Arc::new(members.into_iter().map(|s| Value::String(Arc::new(s))).collect())))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

//=============================================================================
// Sorted Set Operations
//=============================================================================

/// ZADD key score member [score member ...] - add to sorted set (async)
/// Args: [conn, key: String, score: Float, member: String] or [conn, key: String, items: List[(Float, String)]]
/// Returns: Unit (result sent via message) - returns count of new elements
fn redis_zadd(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();

    // Support both single item and list of items
    let items: Vec<(f64, String)> = if args.len() == 4 {
        // Single: zadd(conn, key, score, member)
        let score = args[2].as_f64()?;
        let member = args[3].as_string()?.to_string();
        vec![(score, member)]
    } else {
        // List: zadd(conn, key, [(score, member), ...])
        let list = args[2].as_list()?;
        list.iter().map(|item| {
            let tuple = item.as_tuple()?;
            if tuple.len() != 2 {
                return Err("ZADD: expected (score, member) tuples".to_string());
            }
            Ok((tuple[0].as_f64()?, tuple[1].as_string()?.to_string()))
        }).collect::<Result<Vec<_>, _>>()?
    };

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let count: i64 = conn.zadd_multiple(&key, &items).await
                .map_err(|e| format!("ZADD failed: {}", e))?;
            Ok::<_, String>(Value::Int(count))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// ZREM key member [member ...] - remove from sorted set (async)
/// Args: [conn, key: String, member: String] or [conn, key: String, members: List[String]]
/// Returns: Unit (result sent via message) - returns count removed
fn redis_zrem(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();

    let members: Vec<String> = if let Ok(m) = args[2].as_string() {
        vec![m.to_string()]
    } else {
        let list = args[2].as_list()?;
        list.iter().map(|v| v.as_string().map(|s| s.to_string())).collect::<Result<Vec<_>, _>>()?
    };

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let count: i64 = conn.zrem(&key, &members).await
                .map_err(|e| format!("ZREM failed: {}", e))?;
            Ok::<_, String>(Value::Int(count))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// ZSCORE key member - get score of member (async)
/// Args: [conn, key: String, member: String]
/// Returns: Unit (result sent via message) - returns Float or None
fn redis_zscore(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let member = args[2].as_string()?.to_string();

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let score: Option<f64> = conn.zscore(&key, &member).await
                .map_err(|e| format!("ZSCORE failed: {}", e))?;
            match score {
                Some(s) => Ok::<_, String>(Value::Float(s)),
                None => Ok(Value::None),
            }
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// ZRANK key member - get rank (0-based index) of member (async)
/// Args: [conn, key: String, member: String]
/// Returns: Unit (result sent via message) - returns Int or None
fn redis_zrank(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let member = args[2].as_string()?.to_string();

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let rank: Option<i64> = conn.zrank(&key, &member).await
                .map_err(|e| format!("ZRANK failed: {}", e))?;
            match rank {
                Some(r) => Ok::<_, String>(Value::Int(r)),
                None => Ok(Value::None),
            }
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// ZRANGE key start stop [WITHSCORES] - get range by rank (async)
/// Args: [conn, key: String, start: Int, stop: Int] or [conn, key: String, start: Int, stop: Int, withScores: Bool]
/// Returns: Unit (result sent via message) - returns List[String] or List[(String, Float)]
fn redis_zrange(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let start = args[2].as_i64()? as isize;
    let stop = args[3].as_i64()? as isize;
    let with_scores = if args.len() > 4 { args[4].as_bool()? } else { false };

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            if with_scores {
                let items: Vec<(String, f64)> = conn.zrange_withscores(&key, start, stop).await
                    .map_err(|e| format!("ZRANGE failed: {}", e))?;
                let list: Vec<Value> = items.into_iter()
                    .map(|(m, s)| Value::Tuple(Arc::new(vec![Value::String(Arc::new(m)), Value::Float(s)])))
                    .collect();
                Ok::<_, String>(Value::List(Arc::new(list)))
            } else {
                let members: Vec<String> = conn.zrange(&key, start, stop).await
                    .map_err(|e| format!("ZRANGE failed: {}", e))?;
                Ok::<_, String>(Value::List(Arc::new(members.into_iter().map(|s| Value::String(Arc::new(s))).collect())))
            }
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// ZREVRANGE key start stop [WITHSCORES] - get range by rank, high to low (async)
/// Args: [conn, key: String, start: Int, stop: Int] or [conn, key: String, start: Int, stop: Int, withScores: Bool]
/// Returns: Unit (result sent via message) - returns List[String] or List[(String, Float)]
fn redis_zrevrange(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let start = args[2].as_i64()? as isize;
    let stop = args[3].as_i64()? as isize;
    let with_scores = if args.len() > 4 { args[4].as_bool()? } else { false };

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            if with_scores {
                let items: Vec<(String, f64)> = conn.zrevrange_withscores(&key, start, stop).await
                    .map_err(|e| format!("ZREVRANGE failed: {}", e))?;
                let list: Vec<Value> = items.into_iter()
                    .map(|(m, s)| Value::Tuple(Arc::new(vec![Value::String(Arc::new(m)), Value::Float(s)])))
                    .collect();
                Ok::<_, String>(Value::List(Arc::new(list)))
            } else {
                let members: Vec<String> = conn.zrevrange(&key, start, stop).await
                    .map_err(|e| format!("ZREVRANGE failed: {}", e))?;
                Ok::<_, String>(Value::List(Arc::new(members.into_iter().map(|s| Value::String(Arc::new(s))).collect())))
            }
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// ZRANGEBYSCORE key min max [WITHSCORES] - get range by score (async)
/// Args: [conn, key: String, min: Float, max: Float] or with withScores: Bool
/// Returns: Unit (result sent via message)
fn redis_zrangebyscore(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let min = args[2].as_f64()?;
    let max = args[3].as_f64()?;
    let with_scores = if args.len() > 4 { args[4].as_bool()? } else { false };

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            if with_scores {
                let items: Vec<(String, f64)> = conn.zrangebyscore_withscores(&key, min, max).await
                    .map_err(|e| format!("ZRANGEBYSCORE failed: {}", e))?;
                let list: Vec<Value> = items.into_iter()
                    .map(|(m, s)| Value::Tuple(Arc::new(vec![Value::String(Arc::new(m)), Value::Float(s)])))
                    .collect();
                Ok::<_, String>(Value::List(Arc::new(list)))
            } else {
                let members: Vec<String> = conn.zrangebyscore(&key, min, max).await
                    .map_err(|e| format!("ZRANGEBYSCORE failed: {}", e))?;
                Ok::<_, String>(Value::List(Arc::new(members.into_iter().map(|s| Value::String(Arc::new(s))).collect())))
            }
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// ZINCRBY key increment member - increment score (async)
/// Args: [conn, key: String, increment: Float, member: String]
/// Returns: Unit (result sent via message) - returns new score
fn redis_zincrby(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let increment = args[2].as_f64()?;
    let member = args[3].as_string()?.to_string();

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let new_score: f64 = conn.zincr(&key, &member, increment).await
                .map_err(|e| format!("ZINCRBY failed: {}", e))?;
            Ok::<_, String>(Value::Float(new_score))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// ZCARD key - get sorted set cardinality (async)
/// Args: [conn, key: String]
/// Returns: Unit (result sent via message) - returns Int
fn redis_zcard(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let count: i64 = conn.zcard(&key).await
                .map_err(|e| format!("ZCARD failed: {}", e))?;
            Ok::<_, String>(Value::Int(count))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// ZCOUNT key min max - count members in score range (async)
/// Args: [conn, key: String, min: Float, max: Float]
/// Returns: Unit (result sent via message) - returns Int
fn redis_zcount(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let min = args[2].as_f64()?;
    let max = args[3].as_f64()?;

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let count: i64 = conn.zcount(&key, min, max).await
                .map_err(|e| format!("ZCOUNT failed: {}", e))?;
            Ok::<_, String>(Value::Int(count))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

//=============================================================================
// Pub/Sub - Subscribe
//=============================================================================

/// SUBSCRIBE channel - subscribe to a channel (async, long-running)
/// Args: [url: String, channel: String]
/// Returns: Subscription handle
///
/// Messages are delivered as ("message", channel, payload) tuples to the caller's mailbox.
/// The subscription runs until unsubscribe is called or the handle is garbage collected.
fn redis_subscribe(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let url = args[0].as_string()?.to_string();
    let channel = args[1].as_string()?.to_string();

    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_clone = cancel.clone();
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        // Create a dedicated connection for pub/sub
        let client = match redis::Client::open(url) {
            Ok(c) => c,
            Err(e) => {
                ctx_clone.reply(result_to_value(Err(format!("Failed to connect: {}", e))));
                return;
            }
        };

        let mut pubsub = match client.get_async_pubsub().await {
            Ok(ps) => ps,
            Err(e) => {
                ctx_clone.reply(result_to_value(Err(format!("Failed to get pubsub connection: {}", e))));
                return;
            }
        };

        if let Err(e) = pubsub.subscribe(&channel).await {
            ctx_clone.reply(result_to_value(Err(format!("Failed to subscribe: {}", e))));
            return;
        }

        // Send confirmation
        ctx_clone.reply(result_to_value(Ok(Value::String(Arc::new("subscribed".to_string())))));

        // Message receive loop
        let mut stream = pubsub.on_message();
        while !cancel_clone.load(Ordering::SeqCst) {
            tokio::select! {
                msg = stream.next() => {
                    match msg {
                        Some(msg) => {
                            let channel: String = msg.get_channel_name().to_string();
                            let payload: String = msg.get_payload().unwrap_or_default();

                            let tuple = Value::Tuple(Arc::new(vec![
                                Value::String(Arc::new("message".to_string())),
                                Value::String(Arc::new(channel)),
                                Value::String(Arc::new(payload)),
                            ]));
                            ctx_clone.reply(tuple);
                        }
                        None => break, // Stream ended
                    }
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                    // Check cancel flag periodically
                }
            }
        }
    });

    Ok(sub_handle(Subscription { cancel }))
}

/// PSUBSCRIBE pattern - subscribe to channels matching pattern (async, long-running)
/// Args: [url: String, pattern: String]
/// Returns: Subscription handle
fn redis_psubscribe(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let url = args[0].as_string()?.to_string();
    let pattern = args[1].as_string()?.to_string();

    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_clone = cancel.clone();
    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let client = match redis::Client::open(url) {
            Ok(c) => c,
            Err(e) => {
                ctx_clone.reply(result_to_value(Err(format!("Failed to connect: {}", e))));
                return;
            }
        };

        let mut pubsub = match client.get_async_pubsub().await {
            Ok(ps) => ps,
            Err(e) => {
                ctx_clone.reply(result_to_value(Err(format!("Failed to get pubsub connection: {}", e))));
                return;
            }
        };

        if let Err(e) = pubsub.psubscribe(&pattern).await {
            ctx_clone.reply(result_to_value(Err(format!("Failed to psubscribe: {}", e))));
            return;
        }

        ctx_clone.reply(result_to_value(Ok(Value::String(Arc::new("subscribed".to_string())))));

        let mut stream = pubsub.on_message();
        while !cancel_clone.load(Ordering::SeqCst) {
            tokio::select! {
                msg = stream.next() => {
                    match msg {
                        Some(msg) => {
                            let channel: String = msg.get_channel_name().to_string();
                            let pat: String = msg.get_pattern::<String>().unwrap_or_default();
                            let payload: String = msg.get_payload().unwrap_or_default();

                            let tuple = Value::Tuple(Arc::new(vec![
                                Value::String(Arc::new("pmessage".to_string())),
                                Value::String(Arc::new(pat)),
                                Value::String(Arc::new(channel)),
                                Value::String(Arc::new(payload)),
                            ]));
                            ctx_clone.reply(tuple);
                        }
                        None => break,
                    }
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
            }
        }
    });

    Ok(sub_handle(Subscription { cancel }))
}

/// UNSUBSCRIBE - cancel a subscription
/// Args: [subscription_handle]
/// Returns: Unit
fn redis_unsubscribe(args: &[Value], _ctx: &ExtContext) -> Result<Value, String> {
    let cancel = get_sub(&args[0])?;
    cancel.store(true, Ordering::SeqCst);
    Ok(Value::Unit)
}

//=============================================================================
// Streams
//=============================================================================

/// XADD stream [MAXLEN len] * field value [field value ...] - append to stream (async)
/// Args: [conn, stream: String, fields: List[(String, String)]] or [conn, stream: String, id: String, fields: ...]
/// Returns: Unit (result sent via message) - returns entry ID
fn redis_xadd(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let stream = args[1].as_string()?.to_string();

    // Check if third arg is ID string or fields list
    let (id, fields_arg_idx) = if args[2].as_list().is_ok() {
        ("*".to_string(), 2)
    } else {
        (args[2].as_string()?.to_string(), 3)
    };

    let fields_list = args[fields_arg_idx].as_list()?;
    let fields: Vec<(String, String)> = fields_list.iter().map(|item| {
        let tuple = item.as_tuple()?;
        if tuple.len() != 2 {
            return Err("XADD: expected (field, value) tuples".to_string());
        }
        Ok((tuple[0].as_string()?.to_string(), tuple[1].as_string()?.to_string()))
    }).collect::<Result<Vec<_>, _>>()?;

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let entry_id: String = conn.xadd(&stream, &id, &fields).await
                .map_err(|e| format!("XADD failed: {}", e))?;
            Ok::<_, String>(Value::String(Arc::new(entry_id)))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// XREAD [COUNT count] [BLOCK ms] STREAMS stream [stream ...] id [id ...] - read from streams (async)
/// Args: [conn, streams: List[(String, String)], count: Option[Int], block_ms: Option[Int]]
/// where each tuple is (stream_name, last_id) - use "0" for beginning or "$" for new only
/// Returns: Unit (result sent via message)
fn redis_xread(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let streams_list = args[1].as_list()?;
    let count = if args.len() > 2 { args[2].as_i64().ok().map(|v| v as usize) } else { None };
    let block_ms = if args.len() > 3 { args[3].as_i64().ok().map(|v| v as usize) } else { None };

    let mut stream_names: Vec<String> = Vec::new();
    let mut stream_ids: Vec<String> = Vec::new();
    for item in streams_list.iter() {
        let tuple = item.as_tuple()?;
        if tuple.len() != 2 {
            return Err("XREAD: expected (stream, id) tuples".to_string());
        }
        stream_names.push(tuple[0].as_string()?.to_string());
        stream_ids.push(tuple[1].as_string()?.to_string());
    }

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let mut cmd = redis::cmd("XREAD");
            if let Some(c) = count {
                cmd.arg("COUNT").arg(c);
            }
            if let Some(b) = block_ms {
                cmd.arg("BLOCK").arg(b);
            }
            cmd.arg("STREAMS");
            for name in &stream_names {
                cmd.arg(name);
            }
            for id in &stream_ids {
                cmd.arg(id);
            }

            let result: redis::Value = cmd.query_async(&mut conn).await
                .map_err(|e| format!("XREAD failed: {}", e))?;
            Ok::<_, String>(redis_to_value(result))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// XRANGE stream start end [COUNT count] - get range of entries (async)
/// Args: [conn, stream: String, start: String, end: String] or with count: Int
/// Use "-" for start and "+" for end to get all
/// Returns: Unit (result sent via message)
fn redis_xrange(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let stream = args[1].as_string()?.to_string();
    let start = args[2].as_string()?.to_string();
    let end = args[3].as_string()?.to_string();
    let count = if args.len() > 4 { Some(args[4].as_i64()? as usize) } else { None };

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let mut cmd = redis::cmd("XRANGE");
            cmd.arg(&stream).arg(&start).arg(&end);
            if let Some(c) = count {
                cmd.arg("COUNT").arg(c);
            }

            let result: redis::Value = cmd.query_async(&mut conn).await
                .map_err(|e| format!("XRANGE failed: {}", e))?;
            Ok::<_, String>(redis_to_value(result))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// XLEN stream - get stream length (async)
/// Args: [conn, stream: String]
/// Returns: Unit (result sent via message) - returns Int
fn redis_xlen(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let stream = args[1].as_string()?.to_string();

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let len: i64 = conn.xlen(&stream).await
                .map_err(|e| format!("XLEN failed: {}", e))?;
            Ok::<_, String>(Value::Int(len))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// XINFO GROUPS stream - get consumer groups for stream (async)
/// Args: [conn, stream: String]
/// Returns: Unit (result sent via message)
fn redis_xinfo_groups(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let stream = args[1].as_string()?.to_string();

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let result: redis::Value = redis::cmd("XINFO")
                .arg("GROUPS")
                .arg(&stream)
                .query_async(&mut conn)
                .await
                .map_err(|e| format!("XINFO GROUPS failed: {}", e))?;
            Ok::<_, String>(redis_to_value(result))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// XGROUP CREATE stream group id [MKSTREAM] - create consumer group (async)
/// Args: [conn, stream: String, group: String, id: String, mkstream: Bool]
/// Use "$" for id to only get new messages, "0" to get all
/// Returns: Unit (result sent via message)
fn redis_xgroup_create(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let stream = args[1].as_string()?.to_string();
    let group = args[2].as_string()?.to_string();
    let id = args[3].as_string()?.to_string();
    let mkstream = if args.len() > 4 { args[4].as_bool()? } else { false };

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let mut cmd = redis::cmd("XGROUP");
            cmd.arg("CREATE").arg(&stream).arg(&group).arg(&id);
            if mkstream {
                cmd.arg("MKSTREAM");
            }

            let _: () = cmd.query_async(&mut conn).await
                .map_err(|e| format!("XGROUP CREATE failed: {}", e))?;
            Ok::<_, String>(Value::String(Arc::new("OK".to_string())))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// XREADGROUP GROUP group consumer [COUNT count] [BLOCK ms] STREAMS stream id - read as consumer (async)
/// Args: [conn, group: String, consumer: String, streams: List[(String, String)], count: Option[Int], block_ms: Option[Int]]
/// Use ">" as id to get only new messages assigned to this consumer
/// Returns: Unit (result sent via message)
fn redis_xreadgroup(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let group = args[1].as_string()?.to_string();
    let consumer = args[2].as_string()?.to_string();
    let streams_list = args[3].as_list()?;
    let count = if args.len() > 4 { args[4].as_i64().ok().map(|v| v as usize) } else { None };
    let block_ms = if args.len() > 5 { args[5].as_i64().ok().map(|v| v as usize) } else { None };

    let mut stream_names: Vec<String> = Vec::new();
    let mut stream_ids: Vec<String> = Vec::new();
    for item in streams_list.iter() {
        let tuple = item.as_tuple()?;
        if tuple.len() != 2 {
            return Err("XREADGROUP: expected (stream, id) tuples".to_string());
        }
        stream_names.push(tuple[0].as_string()?.to_string());
        stream_ids.push(tuple[1].as_string()?.to_string());
    }

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let mut cmd = redis::cmd("XREADGROUP");
            cmd.arg("GROUP").arg(&group).arg(&consumer);
            if let Some(c) = count {
                cmd.arg("COUNT").arg(c);
            }
            if let Some(b) = block_ms {
                cmd.arg("BLOCK").arg(b);
            }
            cmd.arg("STREAMS");
            for name in &stream_names {
                cmd.arg(name);
            }
            for id in &stream_ids {
                cmd.arg(id);
            }

            let result: redis::Value = cmd.query_async(&mut conn).await
                .map_err(|e| format!("XREADGROUP failed: {}", e))?;
            Ok::<_, String>(redis_to_value(result))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// XACK stream group id [id ...] - acknowledge message processing (async)
/// Args: [conn, stream: String, group: String, ids: List[String]]
/// Returns: Unit (result sent via message) - returns count acknowledged
fn redis_xack(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let stream = args[1].as_string()?.to_string();
    let group = args[2].as_string()?.to_string();
    let ids_list = args[3].as_list()?;
    let ids: Vec<String> = ids_list.iter()
        .map(|v| v.as_string().map(|s| s.to_string()))
        .collect::<Result<Vec<_>, _>>()?;

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let count: i64 = conn.xack(&stream, &group, &ids).await
                .map_err(|e| format!("XACK failed: {}", e))?;
            Ok::<_, String>(Value::Int(count))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

//=============================================================================
// Atomic Counter Operations
//=============================================================================

/// INCR key - increment by 1 (async)
/// Args: [conn, key: String]
/// Returns: Unit (result sent via message) - returns new value
fn redis_incr(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let new_value: i64 = conn.incr(&key, 1i64).await
                .map_err(|e| format!("INCR failed: {}", e))?;
            Ok::<_, String>(Value::Int(new_value))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// INCRBY key increment - increment by amount (async)
/// Args: [conn, key: String, increment: Int]
/// Returns: Unit (result sent via message) - returns new value
fn redis_incrby(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let increment = args[2].as_i64()?;

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let new_value: i64 = conn.incr(&key, increment).await
                .map_err(|e| format!("INCRBY failed: {}", e))?;
            Ok::<_, String>(Value::Int(new_value))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// DECR key - decrement by 1 (async)
/// Args: [conn, key: String]
/// Returns: Unit (result sent via message) - returns new value
fn redis_decr(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let new_value: i64 = conn.decr(&key, 1i64).await
                .map_err(|e| format!("DECR failed: {}", e))?;
            Ok::<_, String>(Value::Int(new_value))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}

/// DECRBY key decrement - decrement by amount (async)
/// Args: [conn, key: String, decrement: Int]
/// Returns: Unit (result sent via message) - returns new value
fn redis_decrby(args: &[Value], ctx: &ExtContext) -> Result<Value, String> {
    let mut conn = get_conn(&args[0])?;
    let key = args[1].as_string()?.to_string();
    let decrement = args[2].as_i64()?;

    let ctx_clone = ctx.clone();

    ctx.spawn_async(async move {
        let result = async {
            let new_value: i64 = conn.decr(&key, decrement).await
                .map_err(|e| format!("DECRBY failed: {}", e))?;
            Ok::<_, String>(Value::Int(new_value))
        }.await;
        ctx_clone.reply(result_to_value(result));
    });

    Ok(Value::Unit)
}
