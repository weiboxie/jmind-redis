package jmind.redis.out;

import java.util.ArrayList;
import java.util.List;

import jmind.redis.codec.RedisCodec;

/**
 * {@link java.util.List} of boolean output.
 *
 * @author wbxie
 */
public class BooleanListOutput<K, V> extends CommandOut<K, V, List<Boolean>> {
    public BooleanListOutput(RedisCodec<K, V> codec) {
        super(codec, new ArrayList<Boolean>());
    }

    @Override
    public void set(long integer) {
        output.add((integer == 1) ? Boolean.TRUE : Boolean.FALSE);
    }
}
