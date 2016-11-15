package jmind.redis.out;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import jmind.redis.codec.RedisCodec;

/**
 * {@link Map} of keys and values output.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 *
 * @author wbxie
 */
public class MapOutput<K, V> extends CommandOut<K, V, Map<K, V>> {
    private K key;

    public MapOutput(RedisCodec<K, V> codec) {
        super(codec, new HashMap<K, V>());
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (key == null) {
            key = codec.decodeKey(bytes);
            return;
        }

        V value = (bytes == null) ? null : codec.decodeValue(bytes);
        output.put(key, value);
        key = null;
    }
}
