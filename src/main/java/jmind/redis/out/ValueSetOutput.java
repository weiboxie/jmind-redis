package jmind.redis.out;

import java.nio.ByteBuffer;
import java.util.LinkedHashSet;
import java.util.Set;

import jmind.redis.codec.RedisCodec;

/**
 * {@link Set} of value output.
 *
 * @param <V> Value type.
 *
 * @author wbxie
 */
public class ValueSetOutput<K, V> extends CommandOut<K, V, Set<V>> {
    public ValueSetOutput(RedisCodec<K, V> codec) {
        super(codec, new LinkedHashSet<V>());
    }

    @Override
    public void set(ByteBuffer bytes) {
        output.add(bytes == null ? null : codec.decodeValue(bytes));
    }
}
