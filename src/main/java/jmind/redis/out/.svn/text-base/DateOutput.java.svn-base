package jmind.redis.out;

import java.util.Date;

import jmind.redis.codec.RedisCodec;

/**
 * Date output with no milliseconds.
 *
 * @author wbxie
 */
public class DateOutput<K, V> extends CommandOut<K, V, Date> {
    public DateOutput(RedisCodec<K, V> codec) {
        super(codec, null);
    }

    @Override
    public void set(long time) {
        output = new Date(time * 1000);
    }
}
