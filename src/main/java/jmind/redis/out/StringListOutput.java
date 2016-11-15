package jmind.redis.out;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import jmind.redis.codec.RedisCodec;

/**
 * {@link List} of string output.
 *
 * @author wbxie
 */
public class StringListOutput<K, V> extends CommandOut<K, V, List<String>> {
    public StringListOutput(RedisCodec<K, V> codec) {
        super(codec, new ArrayList<String>());
    }

    @Override
    public void set(ByteBuffer bytes) {
        output.add(bytes == null ? null : decodeAscii(bytes));
    }
}
