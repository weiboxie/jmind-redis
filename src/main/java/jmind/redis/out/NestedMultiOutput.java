package jmind.redis.out;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import jmind.redis.RedisException;
import jmind.redis.codec.RedisCodec;

/**
 * {@link List} of command outputs, possibly deeply nested.
 *
 * @author wbxie
 */
public class NestedMultiOutput<K, V> extends CommandOut<K, V, List<Object>> {
    private LinkedList<List<Object>> stack;
    private int depth;

    public NestedMultiOutput(RedisCodec<K, V> codec) {
        super(codec, new ArrayList<Object>());
        stack = new LinkedList<List<Object>>();
        depth = 1;
    }

    @Override
    public void set(long integer) {
        output.add(integer);
    }

    @Override
    public void set(ByteBuffer bytes) {
        output.add(bytes == null ? null : codec.decodeValue(bytes));
    }

    @Override
    public void setError(ByteBuffer error) {
        output.add(new RedisException(decodeAscii(error)));
    }

    @Override
    public void complete(int depth) {
        if (depth > this.depth) {
            Object o = output.remove(output.size() - 1);
            stack.push(output);
            output = new ArrayList<Object>();
            output.add(o);
        } else if (depth > 0 && depth < this.depth) {
            stack.peek().add(output);
            output = stack.pop();
        }
        this.depth = depth;
    }
}
