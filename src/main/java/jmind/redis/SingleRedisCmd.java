package jmind.redis;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import jmind.redis.out.KeyValue;
import jmind.redis.out.ZStoreArgs;

public class SingleRedisCmd<K, V> extends RedisCmd<K, V> {

    public SingleRedisCmd(List<RedisHandler<K, V>> redis) {
        super(redis);
    }

    public RedisHandler<K, V> getShard(K key) {
        return shards.getFisrt();
    }

    private RedisHandler<K, V> getShard() {
        return shards.getFisrt();
    }

    /*******************key*********************************/
    public Future<List<K>> keys(K pattern) {
        return getShard().keys(pattern);
    }

    public long del(K... keys) {
        try {
            return getShard().del(keys).get();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return 0;

    }

    public Future<V> randomkey() {
        return getShard().randomkey();
    }

    public Future<String> rename(K key, K newKey) {
        return getShard().rename(key, newKey);
    }

    public Future<Boolean> renamenx(K key, K newKey) {
        return getShard().renamenx(key, newKey);
    }

    public Future<String> restore(K key, long ttl, byte[] value) {
        return getShard().restore(key, ttl, value);
    }

    public Future<String> objectEncoding(K key) {
        return getShard().objectEncoding(key);
    }

    public Future<Long> objectIdletime(K key) {
        return getShard().objectIdletime(key);
    }

    public Future<Long> objectRefcount(K key) {
        return getShard().objectRefcount(key);
    }

    /*******************String*********************************/
    public Future<String> mset(Map<K, V> map) {
        return getShard().mset(map);
    }

    public Future<Boolean> msetnx(Map<K, V> map) {
        return getShard().msetnx(map);
    }

    public List<V> mget(K... keys) {
        try {
            return getShard().mget(keys).get();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    /************list**********/
    public Future<KeyValue<K, V>> blpop(long timeout, K... keys) {
        return getShard().blpop(timeout, keys);
    }

    public Future<KeyValue<K, V>> brpop(long timeout, K... keys) {
        return getShard().brpop(timeout, keys);
    }

    /**********Set********/
    public Future<Set<V>> sunion(K... keys) {
        return getShard().sunion(keys);
    }

    public Future<Long> sunionstore(K destination, K... keys) {
        return getShard().sunionstore(destination, keys);
    }

    public Future<Set<V>> sdiff(K... keys) {
        return getShard().sdiff(keys);
    }

    public Future<Long> sdiffstore(K destination, K... keys) {
        return getShard().sdiffstore(destination, keys);
    }

    public Future<Set<V>> sinter(K... keys) {
        return getShard().sinter(keys);
    }

    public Future<Long> sinterstore(K destination, K... keys) {
        return getShard().sinterstore(destination, keys);
    }

    public Future<Boolean> smove(K source, K destination, V member) {
        return getShard().smove(source, destination, member);
    }

    /*********Sorted Set***********/
    public Future<Long> zinterstore(K destination, K... keys) {
        return zinterstore(destination, new ZStoreArgs(), keys);
    }

    public Future<Long> zinterstore(K destination, ZStoreArgs storeArgs, K... keys) {

        return getShard().zinterstore(destination, storeArgs, keys);
    }

    public Future<Long> zunionstore(K destination, K... keys) {
        return zunionstore(destination, new ZStoreArgs(), keys);
    }

    public Future<Long> zunionstore(K destination, ZStoreArgs storeArgs, K... keys) {
        return getShard().zunionstore(destination, storeArgs, keys);
    }

    /**************bit********************/
    public Future<Long> bitcount(K key) {
        return getShard(key).bitcount(key);
    }

    public Future<Long> bitcount(K key, long start, long end) {
        return getShard(key).bitcount(key, start, end);
    }

    public Future<Long> bitopNot(K destination, K source) {
        return getShard().bitopNot(destination, source);
    }

    public Future<Long> bitopOr(K destination, K... keys) {

        return getShard().bitopOr(destination, keys);
    }

    public Future<Long> bitopXor(K destination, K... keys) {
        return getShard().bitopXor(destination, keys);
    }

    public Future<Long> bitopAnd(K destination, K... keys) {

        return getShard().bitopAnd(destination, keys);
    }

    public Future<String> ping() {
        return getShard().ping();
    }

    public void shutdown(boolean save) {
        getShard().shutdown(save);
    }
}
