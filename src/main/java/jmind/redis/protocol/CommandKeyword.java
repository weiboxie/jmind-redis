package jmind.redis.protocol;

/**
 * Keyword modifiers for redis commands.
 *
 * @author wbxie
 */
public enum CommandKeyword {
    AFTER, AGGREGATE, ALPHA, AND, ASC, BEFORE, BY, COUNT, DESC, ENCODING, FLUSH, GETNAME, IDLETIME, KILL, LEN, LIMIT, LIST, LOAD, MAX, MIN, NO, NOSAVE, NOT, ONE, OR, REFCOUNT, RESET, RESETSTAT, SETNAME, STORE, SUM, WEIGHTS, WITHSCORES, XOR;

    public byte[] bytes;

    private CommandKeyword() {
        bytes = name().getBytes(Charsets.UTF8);
    }
}
