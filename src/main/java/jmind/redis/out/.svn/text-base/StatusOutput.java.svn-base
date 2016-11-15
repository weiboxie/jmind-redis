package jmind.redis.out;

import static jmind.redis.protocol.Charsets.buffer;

import java.nio.ByteBuffer;

import jmind.redis.codec.RedisCodec;

/**
 * Status message output.
 *
 * @author wbxie
 */
public class StatusOutput<K, V> extends CommandOut<K, V, String> {
    private static final ByteBuffer OK = buffer("OK");

    public StatusOutput(RedisCodec<K, V> codec) {
        super(codec, null);
    }

    @Override
    public void set(ByteBuffer bytes) {
        output = OK.equals(bytes) ? "OK" : decodeAscii(bytes);
    }
}
