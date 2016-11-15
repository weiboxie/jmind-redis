package jmind.redis.out;

import java.nio.ByteBuffer;

import jmind.redis.codec.RedisCodec;

/**
 * Double output, may be null.
 *
 * @author wbxie
 */
public class DoubleOutput<K, V> extends CommandOut<K, V, Double> {
    public DoubleOutput(RedisCodec<K, V> codec) {
        super(codec, null);
    }

    @Override
    public void set(ByteBuffer bytes) {
        output = (bytes == null) ? null : Double.parseDouble(decodeAscii(bytes));
    }
}
