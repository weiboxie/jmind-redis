package jmind.redis.protocol;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;

/**
 * A netty {@link ChannelHandler} responsible for monitoring the channel and
 * reconnecting when the connection is lost.
 *
 * @author wbxie
 */
public class RedisWatchdog extends SimpleChannelUpstreamHandler implements TimerTask {
    private final ClientBootstrap bootstrap;
    private Channel channel;
    private final ChannelGroup channels;
    private Timer timer;
    private boolean reconnect;
    private int attempts;
    private final SocketAddress serverAddress;
    private Class<? extends ChannelHandler> handlerType;

    /**
     * Create a new watchdog that adds to new connections to the supplied {@link ChannelGroup}
     * and establishes a new {@link Channel} when disconnected, while reconnect is true.
     *
     * @param bootstrap Configuration for new channels.
     * @param channels  ChannelGroup to add new channels to.
     * @param timer     Timer used for delayed reconnect.
     */

    public RedisWatchdog(ClientBootstrap bootstrap, ChannelGroup channels, Timer timer, SocketAddress serverAddress,
            Class<? extends ChannelHandler> handlerType) {
        this.bootstrap = bootstrap;
        this.channels = channels;
        this.timer = timer;
        this.serverAddress = serverAddress;
        this.handlerType = handlerType;
    }

    public void setReconnect(boolean reconnect) {
        this.reconnect = reconnect;
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        channel = ctx.getChannel();
        channels.add(channel);
        attempts = 0;
        ctx.sendUpstream(e);
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        if (reconnect) {
            if (attempts < 8)
                attempts++;
            int timeout = 2 << attempts;
            timer.newTimeout(this, timeout, TimeUnit.SECONDS);
        }
        ctx.sendUpstream(e);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        e.getCause().printStackTrace();
        ctx.getChannel().close();

    }

    /**
     * Reconnect to the remote address that the closed channel was connected to.
     * This creates a new {@link ChannelPipeline} with the same handler instances
     * contained in the old channel's pipeline.
     *
     * @param timeout Timer task handle.
     *
     * @throws Exception when reconnection fails.
     */
    @Override
    public void run(Timeout timeout) throws Exception {
        ChannelPipeline old = channel.getPipeline();
        CommandHandler<?, ?> handler = old.get(CommandHandler.class);
        ChannelPipeline pipeline = Channels.pipeline(this, handler, old.get(handlerType));
        Channel c = bootstrap.getFactory().newChannel(pipeline);
        c.getConfig().setOptions(bootstrap.getOptions());
        c.connect(serverAddress);

    }
}
