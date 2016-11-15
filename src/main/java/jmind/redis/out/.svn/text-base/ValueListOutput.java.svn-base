package jmind.redis.out;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import jmind.redis.codec.RedisCodec;

/**
 * {@link List} of values output.
 *
 * @param <V> Value type.
 *
 * @author wbxie
 */
public class ValueListOutput<K, V> extends CommandOut<K, V, List<V>> {
    public ValueListOutput(RedisCodec<K, V> codec) {
        super(codec, new ArrayList<V>());
    }

    @Override
    public void set(ByteBuffer bytes) {
        output.add(bytes == null ? null : codec.decodeValue(bytes));
    }
}
