package jmind.redis.out;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import jmind.redis.codec.RedisCodec;

/**
 * {@link List} of keys output.
 *
 * @param <K> Key type.
 *
 * @author wbxie
 */
public class KeyListOutput<K, V> extends CommandOut<K, V, List<K>> {
    public KeyListOutput(RedisCodec<K, V> codec) {
        super(codec, new ArrayList<K>());
    }

    @Override
    public void set(ByteBuffer bytes) {
        output.add(codec.decodeKey(bytes));
    }
}
