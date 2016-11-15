package jmind.redis.out;

import java.nio.ByteBuffer;

import jmind.redis.codec.RedisCodec;

/**
 * Value output.
 *
 * @param <V> Value type.
 *
 * @author wbxie
 */
public class ValueOutput<K, V> extends CommandOut<K, V, V> {
    public ValueOutput(RedisCodec<K, V> codec) {
        super(codec, null);
    }

    @Override
    public void set(ByteBuffer bytes) {
        output = (bytes == null) ? null : codec.decodeValue(bytes);
    }
}
