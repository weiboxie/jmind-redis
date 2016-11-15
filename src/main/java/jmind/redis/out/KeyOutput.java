package jmind.redis.out;

import java.nio.ByteBuffer;

import jmind.redis.codec.RedisCodec;

/**
 * Key output.
 *
 * @param <K> Key type.
 *
 * @author wbxie
 */
public class KeyOutput<K, V> extends CommandOut<K, V, K> {
    public KeyOutput(RedisCodec<K, V> codec) {
        super(codec, null);
    }

    @Override
    public void set(ByteBuffer bytes) {
        output = (bytes == null) ? null : codec.decodeKey(bytes);
    }
}
