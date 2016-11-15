package jmind.redis.pubsub;

/**
 * Interface for redis pub/sub listeners.
 *
 * @param <V> Value type.
 *
 * @author wbxie
 */
public interface RedisPubSubListener<K, V> {
    /**
     * Message received from a channel subscription.
     *
     * @param channel   Channel.
     * @param message   Message.
     */
    void message(K channel, V message);

    /**
     * Message received from a pattern subscription.
     *
     * @param pattern   Pattern.
     * @param channel   Channel.
     * @param message   Message.
     */
    void message(K pattern, K channel, V message);

    /**
     * Subscribed to a channel.
     *
     * @param channel   Channel
     * @param count     Subscription count.
     */
    void subscribed(K channel, long count);

    /**
     * Subscribed to a pattern.
     *
     * @param pattern   Pattern.
     * @param count     Subscription count.
     */
    void psubscribed(K pattern, long count);

    /**
     * Unsubscribed from a channel.
     *
     * @param channel   Channel
     * @param count     Subscription count.
     */
    void unsubscribed(K channel, long count);

    /**
     * Unsubscribed from a pattern.
     *
     * @param pattern   Channel
     * @param count     Subscription count.
     */
    void punsubscribed(K pattern, long count);
}
