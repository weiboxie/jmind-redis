package jmind.redis.out;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import jmind.base.lang.ScoreValue;
import jmind.redis.codec.RedisCodec;

/**
 * {@link List} of values and their associated scores.
 *
 * @param <V> Value type.
 *
 * @author wbxie
 */
public class ScoredValueListOutput<K, V> extends CommandOut<K, V, List<ScoreValue<V>>> {
    private V value;

    public ScoredValueListOutput(RedisCodec<K, V> codec) {
        super(codec, new ArrayList<ScoreValue<V>>());
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (value == null) {
            value = codec.decodeValue(bytes);
            return;
        }

        double score = Double.parseDouble(decodeAscii(bytes));
        output.add(new ScoreValue<V>(score, value));
        value = null;
    }
}
