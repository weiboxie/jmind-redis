package jmind.redis;

import jmind.base.algo.HashAlgorithms;
import jmind.base.lang.shard.RoundRobinLoadBalance;
import jmind.base.lang.shard.Time33HashLoadBalance;

import java.util.List;

/**
 * time 33 hash算法
 * @param <K>
 * @param <V>
 */
public class Time33RedisCmd<K, V> extends RedisCmd<K, V> {



    public Time33RedisCmd(List<RedisHandler<K, V>> redis) {
        super(new Time33HashLoadBalance<RedisHandler<K, V>>(redis));

    }

    public RedisHandler<K, V> getShard(K key) {
        RedisHandler<K, V> shard = shards.getShard(key.toString());
        if(shard.isConnect())
        return shard;
        else{
            // 形成一个闭环，允许挂断一台
            int size=shards.getShards().size();
            int index = (Math.abs(HashAlgorithms.time33(key.toString())) % size)+1;
            if(index==size)
                index=0;
            return shards.getShards().get(index);
        }
    }



}
