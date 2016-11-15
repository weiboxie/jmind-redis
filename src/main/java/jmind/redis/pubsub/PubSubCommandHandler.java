package jmind.redis.pubsub;

import java.util.concurrent.BlockingQueue;

import jmind.redis.codec.RedisCodec;
import jmind.redis.out.CommandOut;
import jmind.redis.protocol.Command;
import jmind.redis.protocol.CommandHandler;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;

/**
 * A netty {@link ChannelHandler} responsible for writing redis pub/sub commands
 * and reading the response stream from the server.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 *
 * @author wbxie
 */
public class PubSubCommandHandler<K, V> extends CommandHandler<K, V> {
    private RedisCodec<K, V> codec;
    private PubSubOut<K, V> output;

    /**
     * Initialize a new instance.
     *
     * @param queue Command queue.
     * @param codec Codec.
     */
    public PubSubCommandHandler(BlockingQueue<Command<K, V, ?>> queue, RedisCodec<K, V> codec) {
        super(queue);
        this.codec = codec;
        this.output = new PubSubOut<K, V>(codec);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ChannelBuffer buffer) throws InterruptedException {
        while (output.type() == null && !queue.isEmpty()) {
            CommandOut<K, V, ?> output = queue.peek().getOutput();
            if (!rsm.decode(buffer, output))
                return;
            queue.take().complete();
            if (output instanceof PubSubOut)
                Channels.fireMessageReceived(ctx, output);
        }

        while (rsm.decode(buffer, output)) {
            Channels.fireMessageReceived(ctx, output);
            output = new PubSubOut<K, V>(codec);
        }
    }

}
