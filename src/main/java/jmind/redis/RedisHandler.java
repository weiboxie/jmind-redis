package jmind.redis;

import static jmind.redis.protocol.CommandKeyword.AFTER;
import static jmind.redis.protocol.CommandKeyword.AND;
import static jmind.redis.protocol.CommandKeyword.BEFORE;
import static jmind.redis.protocol.CommandKeyword.ENCODING;
import static jmind.redis.protocol.CommandKeyword.FLUSH;
import static jmind.redis.protocol.CommandKeyword.GETNAME;
import static jmind.redis.protocol.CommandKeyword.IDLETIME;
import static jmind.redis.protocol.CommandKeyword.KILL;
import static jmind.redis.protocol.CommandKeyword.LEN;
import static jmind.redis.protocol.CommandKeyword.LIMIT;
import static jmind.redis.protocol.CommandKeyword.LIST;
import static jmind.redis.protocol.CommandKeyword.LOAD;
import static jmind.redis.protocol.CommandKeyword.NO;
import static jmind.redis.protocol.CommandKeyword.NOSAVE;
import static jmind.redis.protocol.CommandKeyword.NOT;
import static jmind.redis.protocol.CommandKeyword.ONE;
import static jmind.redis.protocol.CommandKeyword.OR;
import static jmind.redis.protocol.CommandKeyword.REFCOUNT;
import static jmind.redis.protocol.CommandKeyword.RESET;
import static jmind.redis.protocol.CommandKeyword.RESETSTAT;
import static jmind.redis.protocol.CommandKeyword.SETNAME;
import static jmind.redis.protocol.CommandKeyword.WITHSCORES;
import static jmind.redis.protocol.CommandKeyword.XOR;
import static jmind.redis.protocol.RedisCommand.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import jmind.base.lang.ScoreValue;
import jmind.redis.codec.Base16;
import jmind.redis.codec.RedisCodec;
import jmind.redis.out.BooleanListOutput;
import jmind.redis.out.BooleanOutput;
import jmind.redis.out.ByteArrayOutput;
import jmind.redis.out.CommandOut;
import jmind.redis.out.DateOutput;
import jmind.redis.out.DoubleOutput;
import jmind.redis.out.KeyListOutput;
import jmind.redis.out.KeyOutput;
import jmind.redis.out.KeySetOutput;
import jmind.redis.out.KeyValue;
import jmind.redis.out.KeyValueOutput;
import jmind.redis.out.LongOutput;
import jmind.redis.out.MapOutput;
import jmind.redis.out.MultiOutput;
import jmind.redis.out.NestedMultiOutput;
import jmind.redis.out.ScoredValueListOutput;
import jmind.redis.out.ScriptOutputType;
import jmind.redis.out.SortArgs;
import jmind.redis.out.StatusOutput;
import jmind.redis.out.StringListOutput;
import jmind.redis.out.ValueListOutput;
import jmind.redis.out.ValueOutput;
import jmind.redis.out.ValueSetOutput;
import jmind.redis.out.ZStoreArgs;
import jmind.redis.protocol.Command;
import jmind.redis.protocol.CommandArgs;
import jmind.redis.protocol.RedisCommand;
import jmind.redis.protocol.RedisWatchdog;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

public class RedisHandler<K, V> extends SimpleChannelUpstreamHandler {
    protected BlockingQueue<Command<K, V, ?>> queue;
    protected RedisCodec<K, V> codec;
    protected Channel channel;
    protected long timeout;

    protected MultiOutput<K, V> multi;
    private String password;
    private int db;
    private boolean closed;
    private boolean isConnect = false;

    /**
     * Initialize a new connection.
     *
     * @param queue   Command queue.
     * @param codec   Codec used to encode/decode keys and values.
     * @param timeout Maximum time to wait for a response.
     * @param unit    Unit of time for the timeout.
     */
    public RedisHandler(BlockingQueue<Command<K, V, ?>> queue, RedisCodec<K, V> codec, long timeout) {
        this.queue = queue;
        this.codec = codec;
        this.timeout = timeout;

    }

    public Future<Long> append(K key, V value) {
        return dispatch(APPEND, new LongOutput<K, V>(codec), key, value);
    }

    public String auth(String password) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(password);
        Command<K, V, String> cmd = dispatch(AUTH, new StatusOutput<K, V>(codec), args);
        String status = await(cmd);
        if ("OK".equals(status))
            this.password = password;
        return status;
    }

    public Future<String> bgrewriteaof() {
        return dispatch(BGREWRITEAOF, new StatusOutput<K, V>(codec));
    }

    public Future<String> bgsave() {
        return dispatch(BGSAVE, new StatusOutput<K, V>(codec));
    }

    public Future<Long> bitcount(K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key);
        return dispatch(BITCOUNT, new LongOutput<K, V>(codec), args);
    }

    public Future<Long> bitcount(K key, long start, long end) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(start).add(end);
        return dispatch(BITCOUNT, new LongOutput<K, V>(codec), args);
    }

    public Future<Long> bitopAnd(K destination, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.add(AND).addKey(destination).addKeys(keys);
        return dispatch(BITOP, new LongOutput<K, V>(codec), args);
    }

    public Future<Long> bitopNot(K destination, K source) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.add(NOT).addKey(destination).addKey(source);
        return dispatch(BITOP, new LongOutput<K, V>(codec), args);
    }

    public Future<Long> bitopOr(K destination, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.add(OR).addKey(destination).addKeys(keys);
        return dispatch(BITOP, new LongOutput<K, V>(codec), args);
    }

    public Future<Long> bitopXor(K destination, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.add(XOR).addKey(destination).addKeys(keys);
        return dispatch(BITOP, new LongOutput<K, V>(codec), args);
    }

    public Future<KeyValue<K, V>> blpop(long timeout, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys).add(timeout);
        return dispatch(BLPOP, new KeyValueOutput<K, V>(codec), args);
    }

    public Future<KeyValue<K, V>> brpop(long timeout, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys).add(timeout);
        return dispatch(BRPOP, new KeyValueOutput<K, V>(codec), args);
    }

    public Future<V> brpoplpush(long timeout, K source, K destination) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(source).addKey(destination).add(timeout);
        return dispatch(BRPOPLPUSH, new ValueOutput<K, V>(codec), args);
    }

    public Future<K> clientGetname() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(GETNAME);
        return dispatch(CLIENT, new KeyOutput<K, V>(codec), args);
    }

    public Future<String> clientSetname(K name) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(SETNAME).addKey(name);
        return dispatch(CLIENT, new StatusOutput<K, V>(codec), args);
    }

    public Future<String> clientKill(String addr) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(KILL).add(addr);
        return dispatch(CLIENT, new StatusOutput<K, V>(codec), args);
    }

    public Future<String> clientList() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(LIST);
        return dispatch(CLIENT, new StatusOutput<K, V>(codec), args);
    }

    public Future<List<String>> configGet(String parameter) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(GET).add(parameter);
        return dispatch(CONFIG, new StringListOutput<K, V>(codec), args);
    }

    public Future<String> configResetstat() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(RESETSTAT);
        return dispatch(CONFIG, new StatusOutput<K, V>(codec), args);
    }

    public Future<String> configSet(String parameter, String value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(SET).add(parameter).add(value);
        return dispatch(CONFIG, new StatusOutput<K, V>(codec), args);
    }

    public Future<Long> dbsize() {
        return dispatch(DBSIZE, new LongOutput<K, V>(codec));
    }

    public Future<String> debugObject(K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(OBJECT).addKey(key);
        return dispatch(DEBUG, new StatusOutput<K, V>(codec), args);
    }

    public Future<Long> decr(K key) {
        return dispatch(DECR, new LongOutput<K, V>(codec), key);
    }

    public Future<Long> decrby(K key, long amount) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(amount);
        return dispatch(DECRBY, new LongOutput<K, V>(codec), args);
    }

    public Future<Long> del(K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys);
        return dispatch(DEL, new LongOutput<K, V>(codec), args);
    }

    public Future<Long> del(K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key);
        return dispatch(DEL, new LongOutput<K, V>(codec), args);
    }

    public Future<String> discard() {
        if (multi != null) {
            multi.cancel();
            multi = null;
        }
        return dispatch(DISCARD, new StatusOutput<K, V>(codec));
    }

    public Future<byte[]> dump(K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key);
        return dispatch(DUMP, new ByteArrayOutput<K, V>(codec), args);
    }

    public Future<Boolean> exists(K key) {
        return dispatch(EXISTS, new BooleanOutput<K, V>(codec), key);
    }

    public Future<Boolean> expire(K key, long seconds) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(seconds);
        return dispatch(EXPIRE, new BooleanOutput<K, V>(codec), args);
    }

    public Future<Boolean> expireat(K key, Date timestamp) {
        return expireat(key, timestamp.getTime() / 1000);
    }

    public Future<Boolean> expireat(K key, long timestamp) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(timestamp);
        return dispatch(EXPIREAT, new BooleanOutput<K, V>(codec), args);
    }

    public Future<List<Object>> exec() {
        MultiOutput<K, V> multi = this.multi;
        this.multi = null;
        if (multi == null)
            multi = new MultiOutput<K, V>(codec);
        return dispatch(EXEC, multi);
    }

    public Future<String> flushall() throws Exception {
        return dispatch(FLUSHALL, new StatusOutput<K, V>(codec));
    }

    public Future<String> flushdb() throws Exception {
        return dispatch(FLUSHDB, new StatusOutput<K, V>(codec));
    }

    public Future<V> get(K key) {
        return dispatch(GET, new ValueOutput<K, V>(codec), key);
    }

    public Future<Long> getbit(K key, long offset) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(offset);
        return dispatch(GETBIT, new LongOutput<K, V>(codec), args);
    }

    public Future<V> getrange(K key, long start, long end) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(start).add(end);
        return dispatch(GETRANGE, new ValueOutput<K, V>(codec), args);
    }

    public Future<V> getset(K key, V value) {
        return dispatch(GETSET, new ValueOutput<K, V>(codec), key, value);
    }

    public Future<Long> hdel(K key, K... fields) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKeys(fields);
        return dispatch(HDEL, new LongOutput<K, V>(codec), args);
    }

    public Future<Boolean> hexists(K key, K field) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(field);
        return dispatch(HEXISTS, new BooleanOutput<K, V>(codec), args);
    }

    public Future<V> hget(K key, K field) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(field);
        return dispatch(HGET, new ValueOutput<K, V>(codec), args);
    }

    public Future<Long> hincrby(K key, K field, long amount) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(field).add(amount);
        return dispatch(HINCRBY, new LongOutput<K, V>(codec), args);
    }

    public Future<Double> hincrbyfloat(K key, K field, double amount) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(field).add(amount);
        return dispatch(HINCRBYFLOAT, new DoubleOutput<K, V>(codec), args);
    }

    public Future<Map<K, V>> hgetall(K key) {
        return dispatch(HGETALL, new MapOutput<K, V>(codec), key);
    }

    public Future<Set<K>> hkeys(K key) {
        return dispatch(HKEYS, new KeySetOutput<K, V>(codec), key);
    }

    public Future<Long> hlen(K key) {
        return dispatch(HLEN, new LongOutput<K, V>(codec), key);
    }

    public Future<List<V>> hmget(K key, K... fields) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKeys(fields);
        return dispatch(HMGET, new ValueListOutput<K, V>(codec), args);
    }

    public Future<String> hmset(K key, Map<K, V> map) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(map);
        return dispatch(HMSET, new StatusOutput<K, V>(codec), args);
    }

    public Future<Long> hset(K key, K field, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(field).addValue(value);
        return dispatch(HSET, new LongOutput<K, V>(codec), args);
    }

    public Future<Long> hsetnx(K key, K field, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(field).addValue(value);
        return dispatch(HSETNX, new LongOutput<K, V>(codec), args);
    }

    public Future<List<V>> hvals(K key) {
        return dispatch(HVALS, new ValueListOutput<K, V>(codec), key);
    }

    public Future<Long> incr(K key) {
        return dispatch(INCR, new LongOutput<K, V>(codec), key);
    }

    public Future<Long> incrby(K key, long amount) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(amount);
        return dispatch(INCRBY, new LongOutput<K, V>(codec), args);
    }

    public Future<Double> incrbyfloat(K key, double amount) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(amount);
        return dispatch(INCRBYFLOAT, new DoubleOutput<K, V>(codec), args);
    }

    public Future<String> info() {
        return dispatch(INFO, new StatusOutput<K, V>(codec));
    }

    public Future<String> info(String section) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(section);
        return dispatch(INFO, new StatusOutput<K, V>(codec), args);
    }

    public Future<List<K>> keys(K pattern) {
        return dispatch(KEYS, new KeyListOutput<K, V>(codec), pattern);
    }

    public Future<Date> lastsave() {
        return dispatch(LASTSAVE, new DateOutput<K, V>(codec));
    }

    public Future<V> lindex(K key, long index) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(index);
        return dispatch(LINDEX, new ValueOutput<K, V>(codec), args);
    }

    public Future<Long> linsert(K key, boolean before, V pivot, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(before ? BEFORE : AFTER).addValue(pivot).addValue(value);
        return dispatch(LINSERT, new LongOutput<K, V>(codec), args);
    }

    public Future<Long> llen(K key) {
        return dispatch(LLEN, new LongOutput<K, V>(codec), key);
    }

    public Future<V> lpop(K key) {
        return dispatch(LPOP, new ValueOutput<K, V>(codec), key);
    }

    public Future<Long> lpush(K key, V... values) {
        return dispatch(LPUSH, new LongOutput<K, V>(codec), key, values);
    }

    public Future<Long> lpushx(K key, V value) {
        return dispatch(LPUSHX, new LongOutput<K, V>(codec), key, value);
    }

    public Future<List<V>> lrange(K key, long start, long stop) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(start).add(stop);
        return dispatch(LRANGE, new ValueListOutput<K, V>(codec), args);
    }

    public Future<Long> lrem(K key, long count, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(count).addValue(value);
        return dispatch(LREM, new LongOutput<K, V>(codec), args);
    }

    public Future<String> lset(K key, long index, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(index).addValue(value);
        return dispatch(LSET, new StatusOutput<K, V>(codec), args);
    }

    public Future<String> ltrim(K key, long start, long stop) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(start).add(stop);
        return dispatch(LTRIM, new StatusOutput<K, V>(codec), args);
    }

    public Future<String> migrate(String host, int port, K key, int db, long timeout) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.add(host).add(port).addKey(key).add(db).add(timeout);
        return dispatch(MIGRATE, new StatusOutput<K, V>(codec), args);
    }

    public Future<List<V>> mget(K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys);
        return dispatch(MGET, new ValueListOutput<K, V>(codec), args);
    }

    public Future<Boolean> move(K key, int db) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(db);
        return dispatch(MOVE, new BooleanOutput<K, V>(codec), args);
    }

    public Future<String> multi() {
        Command<K, V, String> cmd = dispatch(MULTI, new StatusOutput<K, V>(codec));
        multi = (multi == null ? new MultiOutput<K, V>(codec) : multi);
        return cmd;
    }

    public Future<String> mset(Map<K, V> map) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(map);
        return dispatch(MSET, new StatusOutput<K, V>(codec), args);
    }

    public Future<Boolean> msetnx(Map<K, V> map) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(map);
        return dispatch(MSETNX, new BooleanOutput<K, V>(codec), args);
    }

    public Future<String> objectEncoding(K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(ENCODING).addKey(key);
        return dispatch(OBJECT, new StatusOutput<K, V>(codec), args);
    }

    public Future<Long> objectIdletime(K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(IDLETIME).addKey(key);
        return dispatch(OBJECT, new LongOutput<K, V>(codec), args);
    }

    public Future<Long> objectRefcount(K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(REFCOUNT).addKey(key);
        return dispatch(OBJECT, new LongOutput<K, V>(codec), args);
    }

    public Future<Boolean> persist(K key) {
        return dispatch(PERSIST, new BooleanOutput<K, V>(codec), key);
    }

    public Future<Boolean> pexpire(K key, long milliseconds) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(milliseconds);
        return dispatch(PEXPIRE, new BooleanOutput<K, V>(codec), args);
    }

    public Future<Boolean> pexpireat(K key, Date timestamp) {
        return pexpireat(key, timestamp.getTime());
    }

    public Future<Boolean> pexpireat(K key, long timestamp) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(timestamp);
        return dispatch(PEXPIREAT, new BooleanOutput<K, V>(codec), args);
    }

    public Future<Long> pttl(K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key);
        return dispatch(PTTL, new LongOutput<K, V>(codec), args);
    }

    public Future<Long> publish(K channel, V message) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(channel).addValue(message);
        return dispatch(PUBLISH, new LongOutput<K, V>(codec), args);
    }

    public Future<V> randomkey() {
        return dispatch(RANDOMKEY, new ValueOutput<K, V>(codec));
    }

    public Future<String> rename(K key, K newKey) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(newKey);
        return dispatch(RENAME, new StatusOutput<K, V>(codec), args);
    }

    public Future<Boolean> renamenx(K key, K newKey) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addKey(newKey);
        return dispatch(RENAMENX, new BooleanOutput<K, V>(codec), args);
    }

    public Future<String> restore(K key, long ttl, byte[] value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(ttl).add(value);
        return dispatch(RESTORE, new StatusOutput<K, V>(codec), args);
    }

    public Future<V> rpop(K key) {
        return dispatch(RPOP, new ValueOutput<K, V>(codec), key);
    }

    public Future<V> rpoplpush(K source, K destination) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(source).addKey(destination);
        return dispatch(RPOPLPUSH, new ValueOutput<K, V>(codec), args);
    }

    public Future<Long> rpush(K key, V... values) {
        return dispatch(RPUSH, new LongOutput<K, V>(codec), key, values);
    }

    public Future<Long> rpushx(K key, V value) {
        return dispatch(RPUSHX, new LongOutput<K, V>(codec), key, value);
    }

    public Future<Long> sadd(K key, V... members) {
        return dispatch(SADD, new LongOutput<K, V>(codec), key, members);
    }

    public Future<String> save() {
        return dispatch(SAVE, new StatusOutput<K, V>(codec));
    }

    public Future<Long> scard(K key) {
        return dispatch(SCARD, new LongOutput<K, V>(codec), key);
    }

    public Future<Set<V>> sdiff(K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys);
        return dispatch(SDIFF, new ValueSetOutput<K, V>(codec), args);
    }

    public Future<Long> sdiffstore(K destination, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(destination).addKeys(keys);
        return dispatch(SDIFFSTORE, new LongOutput<K, V>(codec), args);
    }

    public Future<String> set(K key, V value) {
        return dispatch(SET, new StatusOutput<K, V>(codec), key, value);
    }

    public Future<Long> setbit(K key, long offset, int value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(offset).add(value);
        return dispatch(SETBIT, new LongOutput<K, V>(codec), args);
    }

    public Future<String> setex(K key, long seconds, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(seconds).addValue(value);
        return dispatch(SETEX, new StatusOutput<K, V>(codec), args);
    }

    public Future<Long> setnx(K key, V value) {
        return dispatch(SETNX, new LongOutput<K, V>(codec), key, value);
    }

    public Future<Long> setrange(K key, long offset, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(offset).addValue(value);
        return dispatch(SETRANGE, new LongOutput<K, V>(codec), args);
    }

    public void shutdown(boolean save) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        dispatch(SHUTDOWN, new StatusOutput<K, V>(codec), save ? args.add(SAVE) : args.add(NOSAVE));
    }

    public Future<Set<V>> sinter(K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys);
        return dispatch(SINTER, new ValueSetOutput<K, V>(codec), args);
    }

    public Future<Long> sinterstore(K destination, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(destination).addKeys(keys);
        return dispatch(SINTERSTORE, new LongOutput<K, V>(codec), args);
    }

    public Future<Boolean> sismember(K key, V member) {
        return dispatch(SISMEMBER, new BooleanOutput<K, V>(codec), key, member);
    }

    public Future<Boolean> smove(K source, K destination, V member) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(source).addKey(destination).addValue(member);
        return dispatch(SMOVE, new BooleanOutput<K, V>(codec), args);
    }

    public Future<String> slaveof(String host, int port) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(host).add(port);
        return dispatch(SLAVEOF, new StatusOutput<K, V>(codec), args);
    }

    public Future<String> slaveofNoOne() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(NO).add(ONE);
        return dispatch(SLAVEOF, new StatusOutput<K, V>(codec), args);
    }

    public Future<List<Object>> slowlogGet() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(GET);
        return dispatch(SLOWLOG, new NestedMultiOutput<K, V>(codec), args);
    }

    public Future<List<Object>> slowlogGet(int count) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(GET).add(count);
        return dispatch(SLOWLOG, new NestedMultiOutput<K, V>(codec), args);
    }

    public Future<Long> slowlogLen() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(LEN);
        return dispatch(SLOWLOG, new LongOutput<K, V>(codec), args);
    }

    public Future<String> slowlogReset() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(RESET);
        return dispatch(SLOWLOG, new StatusOutput<K, V>(codec), args);
    }

    public Future<Set<V>> smembers(K key) {
        return dispatch(SMEMBERS, new ValueSetOutput<K, V>(codec), key);
    }

    public Future<List<V>> sort(K key) {
        return dispatch(SORT, new ValueListOutput<K, V>(codec), key);
    }

    public Future<List<V>> sort(K key, SortArgs sortArgs) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key);
        sortArgs.build(args, null);
        return dispatch(SORT, new ValueListOutput<K, V>(codec), args);
    }

    public Future<Long> sortStore(K key, SortArgs sortArgs, K destination) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key);
        sortArgs.build(args, destination);
        return dispatch(SORT, new LongOutput<K, V>(codec), args);
    }

    public Future<V> spop(K key) {
        return dispatch(SPOP, new ValueOutput<K, V>(codec), key);
    }

    public Future<V> srandmember(K key) {
        return dispatch(SRANDMEMBER, new ValueOutput<K, V>(codec), key);
    }

    public Future<Set<V>> srandmember(K key, long count) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(count);
        return dispatch(SRANDMEMBER, new ValueSetOutput<K, V>(codec), args);
    }

    public Future<Long> srem(K key, V... members) {
        return dispatch(SREM, new LongOutput<K, V>(codec), key, members);
    }

    public Future<Set<V>> sunion(K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys);
        return dispatch(SUNION, new ValueSetOutput<K, V>(codec), args);
    }

    public Future<Long> sunionstore(K destination, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(destination).addKeys(keys);
        return dispatch(SUNIONSTORE, new LongOutput<K, V>(codec), args);
    }

    public Future<String> sync() {
        return dispatch(SYNC, new StatusOutput<K, V>(codec));
    }

    public Future<Long> strlen(K key) {
        return dispatch(STRLEN, new LongOutput<K, V>(codec), key);
    }

    public Future<Long> ttl(K key) {
        return dispatch(TTL, new LongOutput<K, V>(codec), key);
    }

    public Future<String> type(K key) {
        return dispatch(TYPE, new StatusOutput<K, V>(codec), key);
    }

    public Future<String> watch(K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKeys(keys);
        return dispatch(WATCH, new StatusOutput<K, V>(codec), args);
    }

    public Future<String> unwatch() {
        return dispatch(UNWATCH, new StatusOutput<K, V>(codec));
    }

    public Future<Long> zadd(K key, double score, V member) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(score).addValue(member);
        return dispatch(ZADD, new LongOutput<K, V>(codec), args);
    }

    @SuppressWarnings("unchecked")
    public Future<Long> zadd(K key, Object... scoresAndValues) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key);
        for (int i = 0; i < scoresAndValues.length; i += 2) {
            args.add((Double) scoresAndValues[i]);
            args.addValue((V) scoresAndValues[i + 1]);
        }
        return dispatch(ZADD, new LongOutput<K, V>(codec), args);
    }

    public Future<Long> zcard(K key) {
        return dispatch(ZCARD, new LongOutput<K, V>(codec), key);
    }

    public Future<Long> zcount(K key, double min, double max) {
        return zcount(key, string(min), string(max));
    }

    public Future<Long> zcount(K key, String min, String max) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(min).add(max);
        return dispatch(ZCOUNT, new LongOutput<K, V>(codec), args);
    }

    public Future<Double> zincrby(K key, double amount, K member) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(amount).addKey(member);
        return dispatch(ZINCRBY, new DoubleOutput<K, V>(codec), args);
    }

    public Future<Long> zinterstore(K destination, K... keys) {
        return zinterstore(destination, new ZStoreArgs(), keys);
    }

    public Future<Long> zinterstore(K destination, ZStoreArgs storeArgs, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(destination).add(keys.length).addKeys(keys);
        storeArgs.build(args);
        return dispatch(ZINTERSTORE, new LongOutput<K, V>(codec), args);
    }

    public Future<List<V>> zrange(K key, long start, long stop) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(start).add(stop);
        return dispatch(ZRANGE, new ValueListOutput<K, V>(codec), args);
    }

    public Future<List<ScoreValue<V>>> zrangeWithScores(K key, long start, long stop) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(start).add(stop).add(WITHSCORES);
        return dispatch(ZRANGE, new ScoredValueListOutput<K, V>(codec), args);
    }

    public Future<List<V>> zrangebyscore(K key, double min, double max) {
        return zrangebyscore(key, string(min), string(max));
    }

    public Future<List<V>> zrangebyscore(K key, String min, String max) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(min).add(max);
        return dispatch(ZRANGEBYSCORE, new ValueListOutput<K, V>(codec), args);
    }

    public Future<List<V>> zrangebyscore(K key, double min, double max, long offset, long count) {
        return zrangebyscore(key, string(min), string(max), offset, count);
    }

    public Future<List<V>> zrangebyscore(K key, String min, String max, long offset, long count) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(min).add(max).add(LIMIT).add(offset).add(count);
        return dispatch(ZRANGEBYSCORE, new ValueListOutput<K, V>(codec), args);
    }

    public Future<List<ScoreValue<V>>> zrangebyscoreWithScores(K key, double min, double max) {
        return zrangebyscoreWithScores(key, string(min), string(max));
    }

    public Future<List<ScoreValue<V>>> zrangebyscoreWithScores(K key, String min, String max) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(min).add(max).add(WITHSCORES);
        return dispatch(ZRANGEBYSCORE, new ScoredValueListOutput<K, V>(codec), args);
    }

    public Future<List<ScoreValue<V>>> zrangebyscoreWithScores(K key, double min, double max, long offset, long count) {
        return zrangebyscoreWithScores(key, string(min), string(max), offset, count);
    }

    public Future<List<ScoreValue<V>>> zrangebyscoreWithScores(K key, String min, String max, long offset, long count) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(min).add(max).add(WITHSCORES).add(LIMIT).add(offset).add(count);
        return dispatch(ZRANGEBYSCORE, new ScoredValueListOutput<K, V>(codec), args);
    }

    public Future<Long> zrank(K key, V member) {
        return dispatch(ZRANK, new LongOutput<K, V>(codec), key, member);
    }

    public Future<Long> zrem(K key, V... members) {
        return dispatch(ZREM, new LongOutput<K, V>(codec), key, members);
    }

    public Future<Long> zremrangebyrank(K key, long start, long stop) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(start).add(stop);
        return dispatch(ZREMRANGEBYRANK, new LongOutput<K, V>(codec), args);
    }

    public Future<Long> zremrangebyscore(K key, double min, double max) {
        return zremrangebyscore(key, string(min), string(max));
    }

    public Future<Long> zremrangebyscore(K key, String min, String max) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(min).add(max);
        return dispatch(ZREMRANGEBYSCORE, new LongOutput<K, V>(codec), args);
    }

    public Future<List<V>> zrevrange(K key, long start, long stop) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(start).add(stop);
        return dispatch(ZREVRANGE, new ValueListOutput<K, V>(codec), args);
    }

    public Future<List<ScoreValue<V>>> zrevrangeWithScores(K key, long start, long stop) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(start).add(stop).add(WITHSCORES);
        return dispatch(ZREVRANGE, new ScoredValueListOutput<K, V>(codec), args);
    }

    public Future<Set<V>> zrevrangebyscore(K key, double max, double min) {
        return zrevrangebyscore(key, string(max), string(min));
    }

    public Future<Set<V>> zrevrangebyscore(K key, String max, String min) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).add(max).add(min);
        return dispatch(ZREVRANGEBYSCORE, new ValueSetOutput<K, V>(codec), args);
    }

    public Future<List<V>> zrevrangebyscore(K key, double max, double min, long offset, long count) {
        return zrevrangebyscore(key, string(max), string(min), offset, count);
    }

    public Future<List<V>> zrevrangebyscore(K key, String max, String min, long offset, long count) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(max).add(min).add(LIMIT).add(offset).add(count);
        return dispatch(ZREVRANGEBYSCORE, new ValueListOutput<K, V>(codec), args);
    }

    public Future<List<ScoreValue<V>>> zrevrangebyscoreWithScores(K key, double max, double min) {
        return zrevrangebyscoreWithScores(key, string(max), string(min));
    }

    public Future<List<ScoreValue<V>>> zrevrangebyscoreWithScores(K key, String max, String min) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(max).add(min).add(WITHSCORES);
        return dispatch(ZREVRANGEBYSCORE, new ScoredValueListOutput<K, V>(codec), args);
    }

    public Future<List<ScoreValue<V>>> zrevrangebyscoreWithScores(K key, double max, double min, long offset, long count) {
        return zrevrangebyscoreWithScores(key, string(max), string(min), offset, count);
    }

    public Future<List<ScoreValue<V>>> zrevrangebyscoreWithScores(K key, String max, String min, long offset, long count) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(key).add(max).add(min).add(WITHSCORES).add(LIMIT).add(offset).add(count);
        return dispatch(ZREVRANGEBYSCORE, new ScoredValueListOutput<K, V>(codec), args);
    }

    public Future<Long> zrevrank(K key, V member) {
        return dispatch(ZREVRANK, new LongOutput<K, V>(codec), key, member);
    }

    public Future<Double> zscore(K key, V member) {
        return dispatch(ZSCORE, new DoubleOutput<K, V>(codec), key, member);
    }

    public Future<Long> zunionstore(K destination, K... keys) {
        return zunionstore(destination, new ZStoreArgs(), keys);
    }

    public Future<Long> zunionstore(K destination, ZStoreArgs storeArgs, K... keys) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addKey(destination).add(keys.length).addKeys(keys);
        storeArgs.build(args);
        return dispatch(ZUNIONSTORE, new LongOutput<K, V>(codec), args);
    }

    /**************script**************/
    public <T> Future<T> eval(V script, ScriptOutputType type, K[] keys, V... values) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.addValue(script).add(keys.length).addKeys(keys).addValues(values);
        CommandOut<K, V, T> output = newScriptOutput(codec, type);
        return dispatch(EVAL, output, args);
    }

    public <T> Future<T> evalsha(String digest, ScriptOutputType type, K[] keys, V... values) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec);
        args.add(digest).add(keys.length).addKeys(keys).addValues(values);
        CommandOut<K, V, T> output = newScriptOutput(codec, type);
        return dispatch(EVALSHA, output, args);
    }

    public Future<List<Boolean>> scriptExists(String... digests) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(EXISTS);
        for (String sha : digests)
            args.add(sha);
        return dispatch(SCRIPT, new BooleanListOutput<K, V>(codec), args);
    }

    public Future<String> scriptFlush() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(FLUSH);
        return dispatch(SCRIPT, new StatusOutput<K, V>(codec), args);
    }

    public Future<String> scriptKill() {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(KILL);
        return dispatch(SCRIPT, new StatusOutput<K, V>(codec), args);
    }

    public Future<String> scriptLoad(V script) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(LOAD).addValue(script);
        return dispatch(SCRIPT, new StatusOutput<K, V>(codec), args);
    }

    /***********Connection(连接)****************/
    public Future<String> quit() {
        return dispatch(QUIT, new StatusOutput<K, V>(codec));
    }

    public Future<String> ping() {
        return dispatch(PING, new StatusOutput<K, V>(codec));
    }

    public Future<V> echo(V msg) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addValue(msg);
        return dispatch(ECHO, new ValueOutput<K, V>(codec), args);
    }

    public String select(int db) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(db);
        Command<K, V, String> cmd = dispatch(SELECT, new StatusOutput<K, V>(codec), args);
        String status = await(cmd);
        return status;
    }

    /**
     * Wait until commands are complete or the connection timeout is reached.
     *
     * @param futures   Futures to wait for.
     *
     * @return True if all futures complete in time.
     */
    public boolean awaitAll(Future<?>... futures) {
        return awaitAll(timeout, TimeUnit.SECONDS, futures);
    }

    /**
     * Wait until futures are complete or the supplied timeout is reached.
     *
     * @param timeout   Maximum time to wait for futures to complete.
     * @param unit      Unit of time for the timeout.
     * @param futures   Futures to wait for.
     *
     * @return True if all futures complete in time.
     */
    public boolean awaitAll(long timeout, TimeUnit unit, Future<?>... futures) {
        boolean complete;

        try {
            long nanos = unit.toNanos(timeout);
            long time = System.nanoTime();

            for (Future<?> f : futures) {
                if (nanos < 0)
                    return false;
                f.get(nanos, TimeUnit.NANOSECONDS);

                long now = System.nanoTime();
                nanos -= now - time;
                time = now;
            }

            complete = true;
        } catch (TimeoutException e) {
            complete = false;
        } catch (Exception e) {
            throw new RedisCmdInterruptedException(e);
        }

        return complete;
    }

    /**
     * Close the connection.
     */
    public synchronized void close() {
        if (!closed && channel != null) {
            RedisWatchdog watchdog = channel.getPipeline().get(RedisWatchdog.class);
            watchdog.setReconnect(false);
            closed = true;
            channel.close();
        }
    }

    public String digest(V script) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA1");
            md.update(codec.encodeValue(script));
            return new String(Base16.encode(md.digest(), false));
        } catch (NoSuchAlgorithmException e) {
            throw new RedisException("JVM does not support SHA1");
        }
    }

    @Override
    public synchronized void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        channel = ctx.getChannel();
        isConnect = true;
        List<Command<K, V, ?>> tmp = new ArrayList<Command<K, V, ?>>(queue.size() + 2);

        if (password != null) {
            CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(password);
            tmp.add(new Command<K, V, String>(AUTH, new StatusOutput<K, V>(codec), args, false));
        }

        if (db != 0) {
            CommandArgs<K, V> args = new CommandArgs<K, V>(codec).add(db);
            tmp.add(new Command<K, V, String>(SELECT, new StatusOutput<K, V>(codec), args, false));
        }

        tmp.addAll(queue);
        queue.clear();

        for (Command<K, V, ?> cmd : tmp) {
            if (!cmd.isCancelled()) {
                queue.add(cmd);
                channel.write(cmd);
            }
        }

        tmp.clear();
    }

    @Override
    public synchronized void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        isConnect = false;
        if (closed) {
            for (Command<K, V, ?> cmd : queue) {
                cmd.getOutput().setError("Connection closed");
                cmd.complete();
            }
            queue.clear();
            queue = null;
            channel = null;
        }
    }

    public <T> Command<K, V, T> dispatch(RedisCommand type, CommandOut<K, V, T> output) {
        return dispatch(type, output, (CommandArgs<K, V>) null);
    }

    public <T> Command<K, V, T> dispatch(RedisCommand type, CommandOut<K, V, T> output, K key) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key);
        return dispatch(type, output, args);
    }

    public <T> Command<K, V, T> dispatch(RedisCommand type, CommandOut<K, V, T> output, K key, V value) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addValue(value);
        return dispatch(type, output, args);
    }

    public <T> Command<K, V, T> dispatch(RedisCommand type, CommandOut<K, V, T> output, K key, V[] values) {
        CommandArgs<K, V> args = new CommandArgs<K, V>(codec).addKey(key).addValues(values);
        return dispatch(type, output, args);
    }

    public synchronized <T> Command<K, V, T> dispatch(RedisCommand type, CommandOut<K, V, T> output,
            CommandArgs<K, V> args) {
        Command<K, V, T> cmd = new Command<K, V, T>(type, output, args, multi != null);

        try {
            if (multi != null) {
                multi.add(cmd);
            }

            queue.put(cmd);

            if (channel != null) {
                channel.write(cmd);
            }
        } catch (NullPointerException e) {
            throw new RedisException("Connection is closed");
        } catch (InterruptedException e) {
            throw new RedisCmdInterruptedException(e);
        }

        return cmd;
    }

    public <T> T await(Command<K, V, T> cmd) {
        if (!cmd.await(timeout, TimeUnit.SECONDS)) {
            cmd.cancel(true);
            throw new RedisException("Command timed out");
        }
        CommandOut<K, V, T> output = cmd.getOutput();
        if (output.hasError())
            throw new RedisException(output.getError());
        return output.get();
    }

    @SuppressWarnings({ "unchecked", "hiding" })
    protected <K, V, T> CommandOut<K, V, T> newScriptOutput(RedisCodec<K, V> codec, ScriptOutputType type) {
        switch (type) {
        case BOOLEAN:
            return (CommandOut<K, V, T>) new BooleanOutput<K, V>(codec);
        case INTEGER:
            return (CommandOut<K, V, T>) new LongOutput<K, V>(codec);
        case STATUS:
            return (CommandOut<K, V, T>) new StatusOutput<K, V>(codec);
        case MULTI:
            return (CommandOut<K, V, T>) new NestedMultiOutput<K, V>(codec);
        case VALUE:
            return (CommandOut<K, V, T>) new ValueOutput<K, V>(codec);
        default:
            throw new RedisException("Unsupported script output type");
        }
    }

    public String string(double n) {
        if (Double.isInfinite(n)) {
            return (n > 0) ? "+inf" : "-inf";
        }
        return Double.toString(n);
    }

    public boolean isConnect() {
        return isConnect;
    }
}
