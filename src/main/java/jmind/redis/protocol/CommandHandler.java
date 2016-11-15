package jmind.redis.protocol;

import java.util.concurrent.BlockingQueue;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * A netty {@link ChannelHandler} responsible for writing redis commands and
 * reading responses from the server.
 *
 * @author wbxie
 */
public class CommandHandler<K, V> extends SimpleChannelHandler {
    protected BlockingQueue<Command<K, V, ?>> queue;
    protected ChannelBuffer buffer;
    protected RedisStateMachine<K, V> rsm;

    /**
     * Initialize a new instance that handles commands from the supplied queue.
     *
     * @param queue The command queue.
     */
    public CommandHandler(BlockingQueue<Command<K, V, ?>> queue) {
        this.queue = queue;
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        buffer = ChannelBuffers.dynamicBuffer(ctx.getChannel().getConfig().getBufferFactory());
        rsm = new RedisStateMachine<K, V>();
    }

    @Override
    public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Command<?, ?, ?> cmd = (Command<?, ?, ?>) e.getMessage();
        Channel channel = ctx.getChannel();
        ChannelBuffer buf = ChannelBuffers.dynamicBuffer(channel.getConfig().getBufferFactory());
        cmd.encode(buf);
        Channels.write(ctx, e.getFuture(), buf);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        ChannelBuffer input = (ChannelBuffer) e.getMessage();
        if (!input.readable())
            return;

        buffer.discardReadBytes();
        buffer.writeBytes(input);

        decode(ctx, buffer);
    }

    protected void decode(ChannelHandlerContext ctx, ChannelBuffer buffer) throws InterruptedException {
        while (!queue.isEmpty() && rsm.decode(buffer, queue.peek().getOutput())) {
            Command<K, V, ?> cmd = queue.take();
            cmd.complete();
        }
    }
}
