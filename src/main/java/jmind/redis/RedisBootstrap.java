package jmind.redis;

import jmind.core.lang.shard.LoadBalance.Balance;
import jmind.core.util.AddrUtil;
import jmind.redis.codec.Utf8Codec;
import jmind.redis.protocol.Command;
import jmind.redis.protocol.CommandHandler;
import jmind.redis.protocol.RedisWatchdog;
import jmind.redis.pubsub.PubSubCommandHandler;
import jmind.redis.pubsub.RedisPubSub;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.*;

/**
 * 
 * @author wbxie
 * 2014-1-17
 */
public class RedisBootstrap {
    private final ClientBootstrap bootstrap;
    private final Timer timer;
    private final ChannelGroup channels;

    private final List<InetSocketAddress> addrs;
    private final int timeout;

    public RedisBootstrap(String hosts) {
        this(hosts, 30);

    }

    public RedisBootstrap(String hosts, int timeout) {
       this.timeout = timeout;
        addrs = AddrUtil.getAddress(hosts);
        ExecutorService connectors = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        ExecutorService workers = Executors.newCachedThreadPool();
        ClientSocketChannelFactory factory = new NioClientSocketChannelFactory(connectors, workers);
        bootstrap = new ClientBootstrap(factory);
        bootstrap.setOption("connectTimeoutMillis", TimeUnit.SECONDS.toMillis(timeout));

        channels = new DefaultChannelGroup();
        timer = new HashedWheelTimer();

    }

    public RedisCmd<String, String> connectAsync() {
        return connectAsync(Balance.Hash);

    }

    public RedisCmd<String, String> connectAsync(Balance balance) {
        List<RedisHandler<String, String>> redis = new CopyOnWriteArrayList<RedisHandler<String, String>>();
        for (InetSocketAddress address : addrs) {
            RedisHandler<String, String> handler = connectAsync(address);
            if (handler != null)
                redis.add(handler);
        }
        if (redis.size() == 1) {
            return new SingleRedisCmd<String, String>(redis);

        }else if(balance==Balance.Time33){
            return new Time33RedisCmd<String,String>(redis);
        }
        return new RedisCmd<String, String>(redis);

    }

    /**
     * Open a new pub/sub connection to the redis server that treats
     * keys and values as UTF-8 strings.
     *
     * @return A new connection.
     */
    public RedisPubSub<String, String> connectPubSub() {
        Utf8Codec codec = new Utf8Codec();
        BlockingQueue<Command<String, String, ?>> queue = new LinkedBlockingQueue<Command<String, String, ?>>();
        PubSubCommandHandler<String, String> handler = new PubSubCommandHandler<String, String>(queue, codec);
        RedisPubSub<String, String> connection = new RedisPubSub<String, String>(queue, codec, timeout);

        try {
            RedisWatchdog watchdog = new RedisWatchdog(bootstrap, channels, timer, addrs.get(0), RedisHandler.class);
            ChannelPipeline pipeline = Channels.pipeline(watchdog, handler, connection);
            Channel channel = bootstrap.getFactory().newChannel(pipeline);

            ChannelFuture future = channel.connect(addrs.get(0));
            //            bootstrap.setPipeline(pipeline);
            //            ChannelFuture future = bootstrap.connect((SocketAddress) bootstrap.getOption("remoteAddress"));
            future.await();

            if (!future.isSuccess()) {
                throw future.getCause();
            }

            watchdog.setReconnect(true);
            this.channels.add(future.getChannel());
            return connection;
        } catch (Throwable e) {
            throw new RedisException("Unable to connect", e);
        }
    }

    private RedisHandler<String, String> connectAsync(InetSocketAddress address) {
        Utf8Codec codec = new Utf8Codec();
        BlockingQueue<Command<String, String, ?>> queue = new LinkedBlockingQueue<Command<String, String, ?>>();

        CommandHandler<String, String> handler = new CommandHandler<String, String>(queue);
        RedisHandler<String, String> redisHandler = new RedisHandler<String, String>(queue, codec, timeout);

        try {
            RedisWatchdog watchdog = new RedisWatchdog(bootstrap, channels, timer, address, RedisHandler.class);
            ChannelPipeline pipeline = Channels.pipeline(watchdog, handler, redisHandler);

            bootstrap.setPipeline(pipeline);
            ChannelFuture future = bootstrap.connect(address);
            future.await();
            if (!future.isSuccess()) {
                future.getCause().printStackTrace();
                return null;
            }

            watchdog.setReconnect(true);
            this.channels.add(future.getChannel());
            return redisHandler;
        } catch (Throwable e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Shutdown this client and close all open connections. The client should be
     * discarded after calling shutdown.
     */
    public void shutdown() {
        for (Channel c : channels) {
            ChannelPipeline pipeline = c.getPipeline();
            RedisHandler<?, ?> connection = pipeline.get(RedisHandler.class);
            connection.close();
        }
        ChannelGroupFuture future = channels.close();
        future.awaitUninterruptibly();
        bootstrap.releaseExternalResources();
    }

}
