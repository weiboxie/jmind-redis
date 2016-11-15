package jmind.redis.out;

import java.nio.ByteBuffer;

import jmind.redis.codec.RedisCodec;

/**
 * Byte array output.
 *
 * @author wbxie
 */
public class ByteArrayOutput<K, V> extends CommandOut<K, V, byte[]> {
    public ByteArrayOutput(RedisCodec<K, V> codec) {
        super(codec, null);
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (bytes != null) {
            output = new byte[bytes.remaining()];
            bytes.get(output);
        }
    }
}
