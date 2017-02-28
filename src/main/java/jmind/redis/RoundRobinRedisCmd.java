package jmind.redis;

import java.util.List;

import jmind.base.lang.shard.RoundRobinLoadBalance;

/**
 * 轮询负载均衡
 * @param <K>
 * @param <V>
 */
public class RoundRobinRedisCmd<K, V> extends RedisCmd<K, V> {

    private final int size;

    public RoundRobinRedisCmd(List<RedisHandler<K, V>> redis) {
        super(new RoundRobinLoadBalance<RedisHandler<K, V>>(redis));
        this.size = redis.size();

    }

    public RedisHandler<K, V> getShard(K key) {
        RedisHandler<K, V> shard = null;
        for (int i = 0; i < size; i++) {
            shard = shards.getShard(null);
            if (shard.isConnect())
                return shard;
        }
        return shard;
    }

}
