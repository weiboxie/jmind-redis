package jmind.redis.codec;

import static java.nio.charset.CoderResult.OVERFLOW;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

/**
 * A {@link RedisCodec} that handles UTF-8 encoded keys and values.
 *
 * @author wbxie
 */
public class Utf8Codec extends RedisCodec<String, String> {
    private Charset charset;
    private CharsetDecoder decoder;
    private CharBuffer chars;

    /**
     * Initialize a new instance that encodes and decodes strings using
     * the UTF-8 charset;
     */
    public Utf8Codec() {
        charset = Charset.forName("UTF-8");
        decoder = charset.newDecoder();
        chars = CharBuffer.allocate(1024);
    }

    @Override
    public String decodeKey(ByteBuffer bytes) {
        return decode(bytes);
    }

    @Override
    public String decodeValue(ByteBuffer bytes) {
        return decode(bytes);
    }

    @Override
    public byte[] encodeKey(String key) {
        return encode(key);
    }

    @Override
    public byte[] encodeValue(String value) {
        return encode(value);
    }

    private String decode(ByteBuffer bytes) {
        chars.clear();
        bytes.mark();

        decoder.reset();
        while (decoder.decode(bytes, chars, true) == OVERFLOW || decoder.flush(chars) == OVERFLOW) {
            chars = CharBuffer.allocate(chars.capacity() * 2);
            bytes.reset();
        }

        return chars.flip().toString();
    }

    private byte[] encode(String string) {
        return string.getBytes(charset);
    }
}
