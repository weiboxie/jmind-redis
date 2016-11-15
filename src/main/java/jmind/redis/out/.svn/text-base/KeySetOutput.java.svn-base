package jmind.redis.out;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import jmind.redis.codec.RedisCodec;

/**
 *
 * @author wbxie
 * 2014-1-17
 * @param <K>
 * @param <V>
 */
public class KeySetOutput<K, V> extends CommandOut<K, V, Set<K>> {
    public KeySetOutput(RedisCodec<K, V> codec) {
        super(codec, new HashSet<K>());
    }

    @Override
    public void set(ByteBuffer bytes) {
        output.add(codec.decodeKey(bytes));
    }
}
