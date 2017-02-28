package jmind.redis;

import jmind.base.lang.ScoreValue;
import jmind.base.lang.shard.ConsistentHashLoadBalance;
import jmind.base.lang.shard.LoadBalance;
import jmind.redis.out.CommandOut;
import jmind.redis.out.SortArgs;
import jmind.redis.protocol.Command;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @param <K>
 * @param <V>
 * @author wbxie
 *         2014-2-10
 * @see http://manual.csser.com/redis/index.html
 * 启动服务
 * /usr/local/redis/bin/redis-server /usr/local/redis/etc/redis.conf
 * <p>
 * 停止服务
 * /usr/local/redis/bin/redis-cli shutdown
 */
public class RedisCmd<K, V> {
    final LoadBalance<RedisHandler<K, V>> shards;



    public RedisCmd(List<RedisHandler<K, V>> redis) {
        this(new ConsistentHashLoadBalance<RedisHandler<K, V>>(redis));

    }

    public RedisCmd(LoadBalance<RedisHandler<K, V>> list) {
        this.shards = list;

    }

    public RedisHandler<K, V> getShard(K key) {
        return shards.getShard(key.toString());
    }

    public void close() {
        for (RedisHandler<K, V> shard : shards.getShards()) {
            shard.close();
        }
    }

    public <T> T await(Future<T> future) {
        @SuppressWarnings("unchecked")
        Command<String, String, T> cmd = (Command<String, String, T>) future;
        if (!cmd.await(shards.getFisrt().timeout, TimeUnit.SECONDS)) {
            cmd.cancel(true);
            throw new RedisException("Command timed out");
        }
        CommandOut<String, String, T> output = cmd.getOutput();
        if (output.hasError())
            throw new RedisException(output.getError());
        return output.get();
    }

    /*******************Key*********************************/
    public long del(K... keys) {
        long i = 0;
        for (K key : keys) {
            try {
                i += getShard(key).del(key).get();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (ExecutionException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return i;

    }

    public Future<Long> ttl(K key) {
        return getShard(key).ttl(key);
    }

    public Future<Long> pttl(K key) {
        return getShard(key).pttl(key);
    }

    public Future<Boolean> exists(K key) {
        return getShard(key).exists(key);
    }

    public Future<Boolean> move(K key, int db) {
        return getShard(key).move(key, db);
    }

    public Future<String> type(K key) {
        return getShard(key).type(key);
    }

    public Future<Boolean> expire(K key, int seconds) {
        return getShard(key).expire(key, seconds);
    }

    public Future<Boolean> expireat(K key, Date timestamp) {
        return expireat(key, timestamp.getTime() / 1000);
    }

    public Future<Boolean> expireat(K key, long timestamp) {
        return getShard(key).expireat(key, timestamp);
    }

    public Future<Boolean> persist(K key) {
        return getShard(key).persist(key);
    }

    public Future<Boolean> pexpire(K key, long milliseconds) {
        return getShard(key).pexpire(key, milliseconds);
    }

    public Future<Boolean> pexpireat(K key, Date timestamp) {
        return pexpireat(key, timestamp.getTime());
    }

    public Future<Boolean> pexpireat(K key, long timestamp) {
        return getShard(key).pexpireat(key, timestamp);
    }

    public Future<List<V>> sort(K key) {
        return getShard(key).sort(key);
    }

    public Future<List<V>> sort(K key, SortArgs sortArgs) {
        return getShard(key).sort(key, sortArgs);
    }

    public Future<Long> sortStore(K key, SortArgs sortArgs, K destination) {

        return getShard(key).sortStore(key, sortArgs, destination);
    }

    /*******************String*********************************/
    public Future<String> set(K key, V value) {
        return getShard(key).set(key, value);
    }

    public Future<String> setex(K key, long seconds, V value) {
        return getShard(key).setex(key, seconds, value);
    }

    public Future<Long> setnx(K key, V value) {

        return getShard(key).setnx(key, value);
    }

    public Future<Long> setrange(K key, long offset, V value) {
        return getShard(key).setrange(key, offset, value);
    }

    public Future<Long> append(K key, V value) {
        return getShard(key).append(key, value);
    }

    public Future<V> get(K key) {
        return getShard(key).get(key);

    }

    public List<V> mget(K... keys) {
        List<V> list = new ArrayList<V>();
        for (K key : keys) {
            try {
                list.add(get(key).get());
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (ExecutionException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return list;
    }

    public Future<V> getrange(K key, long start, long end) {
        return getShard(key).getrange(key, start, end);
    }

    public Future<V> getset(K key, V value) {
        return getShard(key).getset(key, value);

    }

    public Future<Long> strlen(K key) {
        return getShard(key).strlen(key);
    }

    public Future<Long> decr(K key) {
        return getShard(key).decr(key);
    }

    public Future<Long> decrby(K key, long amount) {
        return getShard(key).decrby(key, amount);
    }

    public Future<Long> incr(K key) {
        return getShard(key).incr(key);
    }

    public Future<Long> incrby(K key, long amount) {
        return getShard(key).incrby(key, amount);
    }

    public Future<Double> incrbyfloat(K key, double amount) {
        return getShard(key).incrbyfloat(key, amount);
    }

    public Future<Long> setbit(K key, long offset, int value) {
        return getShard(key).setbit(key, offset, value);
    }

    public Future<Long> getbit(K key, long offset) {
        return getShard(key).getbit(key, offset);
    }

    /*******************HASH*********************************/
    public Future<Long> hdel(K key, K... fields) {
        return getShard(key).hdel(key, fields);
    }

    public Future<Boolean> hexists(K key, K field) {
        return getShard(key).hexists(key, field);
    }

    public Future<V> hget(K key, K field) {
        return getShard(key).hget(key, field);
    }

    public Future<Long> hincrby(K key, K field, long amount) {
        return getShard(key).hincrby(key, field, amount);
    }

    public Future<Double> hincrbyfloat(K key, K field, double amount) {
        return getShard(key).hincrbyfloat(key, field, amount);
    }

    public Future<Map<K, V>> hgetall(K key) {
        return getShard(key).hgetall(key);
    }

    public Future<Set<K>> hkeys(K key) {
        return getShard(key).hkeys(key);
    }

    public Future<Long> hlen(K key) {
        return getShard(key).hlen(key);
    }

    public Future<List<V>> hmget(K key, K... fields) {
        return getShard(key).hmget(key, fields);
    }

    public Future<String> hmset(K key, Map<K, V> map) {
        return getShard(key).hmset(key, map);
    }

    public Future<Long> hset(K key, K field, V value) {
        return getShard(key).hset(key, field, value);
    }

    public Future<Long> hsetnx(K key, K field, V value) {
        return getShard(key).hsetnx(key, field, value);
    }

    public Future<List<V>> hvals(K key) {
        return getShard(key).hvals(key);
    }

    /*******************List*********************************/

    public Future<V> lindex(K key, long index) {
        return getShard(key).lindex(key, index);
    }

    public Future<Long> linsert(K key, boolean before, V pivot, V value) {
        return getShard(key).linsert(key, before, pivot, value);
    }

    public Future<Long> llen(K key) {
        return getShard(key).llen(key);
    }

    public Future<V> lpop(K key) {
        return getShard(key).lpop(key);
    }

    public Future<Long> lpush(K key, V... values) {
        return getShard(key).lpush(key, values);
    }

    public Future<Long> lpushx(K key, V value) {
        return getShard(key).lpushx(key, value);
    }

    public Future<List<V>> lrange(K key, long start, long stop) {
        return getShard(key).lrange(key, start, stop);
    }

    public Future<Long> lrem(K key, long count, V value) {
        return getShard(key).lrem(key, count, value);
    }

    public Future<String> lset(K key, long index, V value) {
        return getShard(key).lset(key, index, value);
    }

    public Future<String> ltrim(K key, long start, long stop) {

        return getShard(key).ltrim(key, start, stop);
    }

    public Future<V> rpop(K key) {
        return getShard(key).rpop(key);
    }

    public Future<V> rpoplpush(K key, K destination) {

        return getShard(key).rpoplpush(key, destination);
    }

    public Future<Long> rpush(K key, V... values) {
        return getShard(key).rpush(key, values);
    }

    public Future<Long> rpushx(K key, V value) {
        return getShard(key).rpushx(key, value);
    }

    public Future<V> brpoplpush(long timeout, K key, K destination) {
        return getShard(key).brpoplpush(timeout, key, destination);
    }

    /***************Set******************************/
    public Future<Long> sadd(K key, V... members) {
        return getShard(key).sadd(key, members);
    }

    public Future<Long> scard(K key) {
        return getShard(key).scard(key);
    }

    public Future<Long> srem(K key, V... members) {
        return getShard(key).srem(key, members);
    }

    public Future<V> spop(K key) {
        return getShard(key).spop(key);
    }

    public Future<V> srandmember(K key) {
        return getShard(key).srandmember(key);
    }

    public Future<Set<V>> srandmember(K key, long count) {
        return getShard(key).srandmember(key, count);
    }

    public Future<Boolean> sismember(K key, V member) {
        return getShard(key).sismember(key, member);
    }

    public Future<Set<V>> smembers(K key) {
        return getShard(key).smembers(key);
    }

    /************Sorted Set***********************/

    public Future<Long> zadd(K key, double score, V member) {
        return getShard(key).zadd(key, score, member);
    }

    public Future<Long> zadd(K key, Object... scoresAndValues) {
        return getShard(key).zadd(key, scoresAndValues);
    }

    public Future<Long> zcard(K key) {
        return getShard(key).zcard(key);
    }

    public Future<Long> zcount(K key, double min, double max) {
        return getShard(key).zcount(key, min, max);
    }

    public Future<Long> zcount(K key, String min, String max) {
        return getShard(key).zcount(key, min, max);
    }

    public Future<Double> zincrby(K key, double amount, K member) {
        return getShard(key).zincrby(key, amount, member);

    }

    public Future<List<V>> zrange(K key, long start, long stop) {
        return getShard(key).zrange(key, start, stop);
    }

    public Future<List<ScoreValue<V>>> zrangeWithScores(K key, long start, long stop) {
        return getShard(key).zrangeWithScores(key, start, stop);
    }

    public Future<List<V>> zrangebyscore(K key, double min, double max) {
        return getShard(key).zrangebyscore(key, min, max);
    }

    public Future<List<V>> zrangebyscore(K key, String min, String max) {
        return getShard(key).zrangebyscore(key, min, max);
    }

    public Future<List<V>> zrangebyscore(K key, double min, double max, long offset, long count) {
        return getShard(key).zrangebyscore(key, min, max, offset, count);
    }

    public Future<List<V>> zrangebyscore(K key, String min, String max, long offset, long count) {
        return getShard(key).zrangebyscore(key, min, max);
    }

    public Future<List<ScoreValue<V>>> zrangebyscoreWithScores(K key, double min, double max) {
        return getShard(key).zrangebyscoreWithScores(key, min, max);
    }

    public Future<List<ScoreValue<V>>> zrangebyscoreWithScores(K key, String min, String max) {
        return getShard(key).zrangebyscoreWithScores(key, min, max);
    }

    public Future<List<ScoreValue<V>>> zrangebyscoreWithScores(K key, double min, double max, long offset, long count) {
        return getShard(key).zrangebyscoreWithScores(key, min, max, offset, count);
    }

    public Future<Long> zrank(K key, V member) {
        return getShard(key).zrank(key, member);
    }

    public Future<Long> zrem(K key, V... members) {
        return getShard(key).zrem(key, members);
    }

    public Future<Long> zremrangebyrank(K key, long start, long stop) {
        return getShard(key).zremrangebyrank(key, start, stop);
    }

    public Future<Long> zremrangebyscore(K key, double min, double max) {
        return getShard(key).zremrangebyscore(key, min, max);
    }

    public Future<List<V>> zrevrange(K key, long start, long stop) {
        return getShard(key).zrevrange(key, start, stop);
    }

    public Future<List<ScoreValue<V>>> zrevrangeWithScores(K key, long start, long stop) {
        return getShard(key).zrevrangeWithScores(key, start, stop);
    }

    public Future<Set<V>> zrevrangebyscore(K key, double max, double min) {
        return getShard(key).zrevrangebyscore(key, max, min);
    }

    public Future<List<V>> zrevrangebyscore(K key, double max, double min, long offset, long count) {
        return getShard(key).zrevrangebyscore(key, max, min, offset, count);
    }

    public Future<List<V>> zrevrangebyscore(K key, String max, String min, long offset, long count) {
        return getShard(key).zrevrangebyscore(key, max, min, offset, count);
    }

    public Future<List<ScoreValue<V>>> zrevrangebyscoreWithScores(K key, double max, double min) {
        return getShard(key).zrevrangebyscoreWithScores(key, max, min);
    }

    public Future<List<ScoreValue<V>>> zrevrangebyscoreWithScores(K key, double max, double min, long offset, long count) {
        return getShard(key).zrevrangebyscoreWithScores(key, max, min, offset, count);
    }

    public Future<Long> zrevrank(K key, V member) {
        return getShard(key).zrevrank(key, member);
    }

    public Future<Double> zscore(K key, V member) {
        return getShard(key).zscore(key, member);
    }

    /********other*************/
    public Future<byte[]> dump(K key) {
        return getShard(key).dump(key);
    }

}
