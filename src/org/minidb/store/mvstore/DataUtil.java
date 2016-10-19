package org.minidb.store.mvstore;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by gxh on 2016/6/29.
 */
public class DataUtil {

    public static final int COMPRESSED_VAR_INT_MAX = 0x1fffff;
    public static final long COMPRESSED_VAR_LONG_MAX = 0x1ffffffffffffL;

    public static final int PAGE_TYPE_LEAF = 0;
    public static final int PAGE_TYPE_NODE = 1;

    public static final Charset LATIN = Charset.forName("ISO-8859-1");

    public static void copyExcept(Object src, Object dst, int oldSize,
                                  int removeIndex) {
        if (removeIndex > 0 && oldSize > 0) {
            System.arraycopy(src, 0, dst, 0, removeIndex);
        }
        if (removeIndex < oldSize) {
            System.arraycopy(src, removeIndex + 1, dst, removeIndex, oldSize
                    - removeIndex - 1);
        }
    }

    public static void copyWithGap(Object src, Object dst, int oldSize,
                                   int gapIndex) {
        if (gapIndex > 0) {
            System.arraycopy(src, 0, dst, 0, gapIndex);
        }
        if (gapIndex < oldSize) {
            System.arraycopy(src, gapIndex, dst, gapIndex + 1, oldSize
                    - gapIndex);
        }
    }

    public static int readHexInt(HashMap<String, ? extends Object> map,
                                 String key, int defaultValue) {
        Object v = map.get(key);
        if (v == null) {
            return defaultValue;
        } else if (v instanceof Integer) {
            return (Integer) v;
        }
        return (int) Long.parseLong((String) v, 16);
    }

    public static long readHexLong(Map<String, ? extends Object> map,
                                   String key, long defaultValue) {
        Object v = map.get(key);
        if (v == null) {
            return defaultValue;
        } else if (v instanceof Long) {
            return (Long) v;
        }
        return parseHexLong((String) v);
    }

    public static long parseHexLong(String x) {
        if (x.length() == 16) {
            // avoid problems with overflow
            // in Java 8, this special case is not needed
            return (Long.parseLong(x.substring(0, 8), 16) << 32) |
                    Long.parseLong(x.substring(8, 16), 16);
        }
        return Long.parseLong(x, 16);
    }

    public static StringBuilder appendMap(StringBuilder buff,
                                          HashMap<String, ?> map) {
        ArrayList<String> list = new ArrayList(map.keySet());
        Collections.sort(list);
        for (String k : list) {
            appendMap(buff, k, map.get(k));
        }
        return buff;
    }


    private static final byte[] EMPTY_BYTES = {};


    public static byte[] newBytes(int len) {
        if (len == 0) {
            return EMPTY_BYTES;
        }
        try {
            return new byte[len];
        } catch (OutOfMemoryError e) {
            Error e2 = new OutOfMemoryError("Requested memory: " + len);
            e2.initCause(e);
            throw e2;
        }
    }

    public static void appendMap(StringBuilder buff, String key, Object value) {
        if (buff.length() > 0) {
            buff.append(',');
        }
        buff.append(key).append(':');
        String v;
        if (value instanceof Long) {
            v = Long.toHexString((Long) value);
        } else if (value instanceof Integer) {
            v = Integer.toHexString((Integer) value);
        } else {
            v = value.toString();
        }
        if (v.indexOf(',') < 0 && v.indexOf('\"') < 0) {
            buff.append(v);
        } else {
            buff.append('\"');
            for (int i = 0, size = v.length(); i < size; i++) {
                char c = v.charAt(i);
                if (c == '\"') {
                    buff.append('\\');
                }
                buff.append(c);
            }
            buff.append('\"');
        }
    }

    public static HashMap<String, String> parseMap(String s) {
        HashMap<String, String> map = new HashMap();
        for (int i = 0, size = s.length(); i < size;) {
            int startKey = i;
            i = s.indexOf(':', i);
            String key = s.substring(startKey, i++);
            StringBuilder buff = new StringBuilder();
            while (i < size) {
                char c = s.charAt(i++);
                if (c == ',') {
                    break;
                } else if (c == '\"') {
                    while (i < size) {
                        c = s.charAt(i++);
                        if (c == '\\') {
                            c = s.charAt(i++);
                        } else if (c == '\"') {
                            break;
                        }
                        buff.append(c);
                    }
                } else {
                    buff.append(c);
                }
            }
            map.put(key, buff.toString());
        }
        return map;
    }

    public static int getFletcher32(byte[] bytes, int length) {
        int s1 = 0xffff, s2 = 0xffff;
        int i = 0, evenLength = length / 2 * 2;
        while (i < evenLength) {
            // reduce after 360 words (each word is two bytes)
            for (int end = Math.min(i + 720, evenLength); i < end;) {
                int x = ((bytes[i++] & 0xff) << 8) | (bytes[i++] & 0xff);
                s2 += s1 += x;
            }
            s1 = (s1 & 0xffff) + (s1 >>> 16);
            s2 = (s2 & 0xffff) + (s2 >>> 16);
        }
        if (i < length) {
            // odd length: append 0
            int x = (bytes[i] & 0xff) << 8;
            s2 += s1 += x;
        }
        s1 = (s1 & 0xffff) + (s1 >>> 16);
        s2 = (s2 & 0xffff) + (s2 >>> 16);
        return (s2 << 16) | s1;
    }

    public static void writeVarInt(ByteBuffer buff, int x) {
        while ((x & ~0x7f) != 0) {
            buff.put((byte) (0x80 | (x & 0x7f)));
            x >>>= 7;
        }
        buff.put((byte) x);
    }

    public static void writeVarLong(ByteBuffer buff, long x) {
        while ((x & ~0x7f) != 0) {
            buff.put((byte) (0x80 | (x & 0x7f)));
            x >>>= 7;
        }
        buff.put((byte) x);
    }

    public static int readVarInt(ByteBuffer buff) {
        int b = buff.get();
        if (b >= 0) {
            return b;
        }
        // a separate function so that this one can be inlined
        return readVarIntRest(buff, b);
    }

    private static int readVarIntRest(ByteBuffer buff, int b) {
        int x = b & 0x7f;
        b = buff.get();
        if (b >= 0) {
            return x | (b << 7);
        }
        x |= (b & 0x7f) << 7;
        b = buff.get();
        if (b >= 0) {
            return x | (b << 14);
        }
        x |= (b & 0x7f) << 14;
        b = buff.get();
        if (b >= 0) {
            return x | b << 21;
        }
        x |= ((b & 0x7f) << 21) | (buff.get() << 28);
        return x;
    }

    public static long readVarLong(ByteBuffer buff) {
        long x = buff.get();
        if (x >= 0) {
            return x;
        }
        x &= 0x7f;
        for (int s = 7; s < 64; s += 7) {
            long b = buff.get();
            x |= (b & 0x7f) << s;
            if (b >= 0) {
                break;
            }
        }
        return x;
    }

    public static String readString(ByteBuffer buff, int len) {
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            int x = buff.get() & 0xff;
            if (x < 0x80) {
                chars[i] = (char) x;
            } else if (x >= 0xe0) {
                chars[i] = (char) (((x & 0xf) << 12)
                        + ((buff.get() & 0x3f) << 6) + (buff.get() & 0x3f));
            } else {
                chars[i] = (char) (((x & 0x1f) << 6) + (buff.get() & 0x3f));
            }
        }
        return new String(chars);
    }

    public static long getPagePos(int chunkId, int offset,
                                  int length, int type) {
        long pos = (long) chunkId << 38;
        pos |= (long) offset << 6;
        pos |= encodeLength(length) << 1;
        pos |= type;
        return pos;
    }

    /**
     * Convert the length to a length code 0..31. 31 means more than 1 MB.
     *
     * @param len the length
     * @return the length code
     */
    public static int encodeLength(int len) {
        if (len <= 32) {
            return 0;
        }
        int code = Integer.numberOfLeadingZeros(len);
        int remaining = len << (code + 1);
        code += code;
        if ((remaining & (1 << 31)) != 0) {
            code--;
        }
        if ((remaining << 1) != 0) {
            code--;
        }
        code = Math.min(31, 52 - code);
        // alternative code (slower):
        // int x = len;
        // int shift = 0;
        // while (x > 3) {
        //    shift++;
        //    x = (x >>> 1) + (x & 1);
        // }
        // shift = Math.max(0,  shift - 4);
        // int code = (shift << 1) + (x & 1);
        // code = Math.min(31, code);
        return code;
    }

    public static short getCheckValue(int x) {
        return (short) ((x >> 16) ^ x);
    }

    public static int getPageOffset(long pos) {
        return (int) (pos >> 6);
    }

    public static int getPageChunkId(long pos) {
        return (int) (pos >>> 38);
    }

    public static int getPageMaxLength(long pos) {
        int code = (int) ((pos >> 1) & 31);
        return (2 + (code & 1)) << ((code >> 1) + 4);
    }

    /**
     * An entry of a map.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    public static class MapEntry<K, V> implements Map.Entry<K, V> {

        private final K key;
        private V value;

        public MapEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            throw new RuntimeException("Updating the value is not supported");
        }

    }
}
