package jmind.redis.out;

import java.nio.ByteBuffer;

import jmind.redis.codec.RedisCodec;

/**
 * Boolean output. The actual value is returned as an integer
 * where 0 indicates false and 1 indicates true, or as a null
 * bulk reply for script output.
 *
 * @author wbxie
 */
public class BooleanOutput<K, V> extends CommandOut<K, V, Boolean> {
    public BooleanOutput(RedisCodec<K, V> codec) {
        super(codec, null);
    }

    @Override
    public void set(long integer) {
        output = (integer == 1) ? Boolean.TRUE : Boolean.FALSE;
    }

    @Override
    public void set(ByteBuffer bytes) {
        output = (bytes != null) ? Boolean.TRUE : Boolean.FALSE;
    }
}
