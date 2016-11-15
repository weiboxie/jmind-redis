package jmind.redis.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * {@link Charset}-related utilities.
 *
 * @author wbxie
 */
public class Charsets {
    public static final Charset UTF8 = Charset.forName("UTF-8");

    public static ByteBuffer buffer(String s) {
        return ByteBuffer.wrap(s.getBytes(UTF8));
    }
}
