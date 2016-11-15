package jmind.redis.out;

/**
 * A Lua script returns one of the following types:
 *
 * <ul>
 *  <li>{@link #BOOLEAN} boolean</li>
 *  <li>{@link #INTEGER} 64-bit integer</li>
 *  <li>{@link #STATUS}  status string</li>
 *  <li>{@link #VALUE}   value</li>
 *  <li>{@link #MULTI}   of these types</li>.
 * </ul>
 *
 * @author wbxie
 */
public enum ScriptOutputType {
    BOOLEAN, INTEGER, MULTI, STATUS, VALUE
}
