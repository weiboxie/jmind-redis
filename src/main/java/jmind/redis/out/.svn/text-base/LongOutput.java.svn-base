package jmind.redis.out;

import java.nio.ByteBuffer;

import jmind.redis.codec.RedisCodec;

/**
 * 64-bit integer output, may be null.
 *
 * @author wbxie
 */
public class LongOutput<K, V> extends CommandOut<K, V, Long> {
    public LongOutput(RedisCodec<K, V> codec) {
        super(codec, null);
    }

    @Override
    public void set(long integer) {
        output = integer;
    }

    @Override
    public void set(ByteBuffer bytes) {
        output = null;
    }
}
