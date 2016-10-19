/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.engine;

import org.minidb.store.mvstore.DataUtil;
import org.minidb.store.mvstore.WriteBuffer;
import org.minidb.store.mvstore.type.DataType;
import org.minidb.value.*;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

/**
 * A row type.
 */
public class ValueDataType implements DataType {

    private static final int INT_0_15 = 32;
    private static final int LONG_0_7 = 48;
    private static final int DECIMAL_0_1 = 56;
    private static final int DECIMAL_SMALL_0 = 58;
    private static final int DECIMAL_SMALL = 59;
    private static final int DOUBLE_0_1 = 60;
    private static final int FLOAT_0_1 = 62;
    private static final int BOOLEAN_FALSE = 64;
    private static final int BOOLEAN_TRUE = 65;
    private static final int INT_NEG = 66;
    private static final int LONG_NEG = 67;
    private static final int STRING_0_31 = 68;
    private static final int BYTES_0_31 = 100;
    private static final int SPATIAL_KEY_2D = 132;

    public static final int ASCENDING = 0;
    public static final int DESCENDING = 1;

    final int[] sortTypes;

    public ValueDataType(int[] sortTypes) {
        this.sortTypes = sortTypes;
    }

    @Override
    public int compare(Object a, Object b) {
        if (a == b) {
            return 0;
        }
        if (a instanceof ValueArray && b instanceof ValueArray) {
            Value[] ax = ((ValueArray) a).getList();
            Value[] bx = ((ValueArray) b).getList();
            int al = ax.length;
            int bl = bx.length;
            int len = Math.min(al, bl);
            for (int i = 0; i < len; i++) {
                int sortType = sortTypes[i];
                int comp = compareValues(ax[i], bx[i], sortType);
                if (comp != 0) {
                    return comp;
                }
            }
            if (len < al) {
                return -1;
            } else if (len < bl) {
                return 1;
            }
            return 0;
        }
        return compareValues((Value) a, (Value) b, ASCENDING);
    }

    private int compareValues(Value a, Value b, int sortType) {
        if (a == b) {
            return 0;
        }
        // null is never stored;
        // comparison with null is used to retrieve all entries
        // in which case null is always lower than all entries
        // (even for descending ordered indexes)
        if (a == null) {
            return -1;
        } else if (b == null) {
            return 1;
        }
//        throw new RuntimeException("compareValues ERROR");
        boolean aNull = a == ValueNull.INSTANCE;
        boolean bNull = b == ValueNull.INSTANCE;
        if (aNull || bNull) {
            throw new RuntimeException("compareValues ERROR");
//            return SortOrder.compareNull(aNull, sortType);
        }
        int comp = a.compareTypeSafe(b);
        if ((sortType & DESCENDING) != 0) {
            comp = -comp;
        }
        return comp;
    }


    @Override
    public int getMemory(Object obj) {
        return getMemory((Value) obj);
    }

    private static int getMemory(Value v) {
//        return v == null ? 0 : v.getMemory();
//        throw new RuntimeException("getMemory ERROR");
        System.out.println("getMemory 没写");
        return 0;
    }

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            obj[i] = read(buff);
        }
    }

    @Override
    public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }

    @Override
    public Object read(ByteBuffer buff) {
        return readValue(buff);
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        Value x = (Value) obj;
        writeValue(buff, x);
    }

    private void writeValue(WriteBuffer buff, Value v) {
        int type = v.getType();
        switch (type) {
            case Value.INT: {
                int x = v.getInt();
                if (x < 0) {
                    buff.put((byte) INT_NEG).putVarInt(-x);
                } else if (x < 16) {
                    buff.put((byte) (INT_0_15 + x));
                } else {
                    buff.put((byte) type).putVarInt(x);
                }
                break;
            }
            case Value.LONG: {
                long x = v.getLong();
                if (x < 0) {
                    buff.put((byte) LONG_NEG).putVarLong(-x);
                } else if (x < 8) {
                    buff.put((byte) (LONG_0_7 + x));
                } else {
                    buff.put((byte) type).putVarLong(x);
                }
                break;
            }
            case Value.STRING: {
                String s = v.getString();
                int len = s.length();
                if (len < 32) {
                    buff.put((byte) (STRING_0_31 + len)).
                            putStringData(s, len);
                } else {
                    buff.put((byte) type);
                    writeString(buff, s);
                }
                break;
            }
            case Value.ARRAY: {
                Value[] list = ((ValueArray) v).getList();
                buff.put((byte) type).putVarInt(list.length);
                for (Value x : list) {
                    writeValue(buff, x);
                }
                break;
            }
            default:
                throw new RuntimeException("writeValue ERROR");
        }
    }

    private static void writeString(WriteBuffer buff, String s) {
        int len = s.length();
        buff.putVarInt(len).putStringData(s, len);
    }

    /**
     * Read a value.
     *
     * @return the value
     */
    private Object readValue(ByteBuffer buff) {
        int type = buff.get() & 255;
        switch (type) {
            case INT_NEG:
                return ValueInt.get(-readVarInt(buff));
            case Value.INT:
                return ValueInt.get(readVarInt(buff));
            case LONG_NEG:
                return ValueLong.get(-readVarLong(buff));
            case Value.LONG:
                return ValueLong.get(readVarLong(buff));
            case Value.STRING:
                return ValueString.get(readString(buff));
            case DECIMAL_0_1:
                return ValueDecimal.ZERO;
            case DECIMAL_0_1 + 1:
                return ValueDecimal.ONE;
            case DECIMAL_SMALL_0:
                return ValueDecimal.get(BigDecimal.valueOf(
                        readVarLong(buff)));
            case DECIMAL_SMALL: {
                int scale = readVarInt(buff);
                return ValueDecimal.get(BigDecimal.valueOf(
                        readVarLong(buff), scale));
            }
            case Value.ARRAY: {
                int len = readVarInt(buff);
                Value[] list = new Value[len];
                for (int i = 0; i < len; i++) {
                    list[i] = (Value) readValue(buff);
                }
                return ValueArray.get(list);
            }
            default:
                if (type >= INT_0_15 && type < INT_0_15 + 16) {
                    return ValueInt.get(type - INT_0_15);
                } else if (type >= LONG_0_7 && type < LONG_0_7 + 8) {
                    return ValueLong.get(type - LONG_0_7);
                }
                throw new RuntimeException("readValue ERROR");
        }
    }

    private static int readVarInt(ByteBuffer buff) {
        return DataUtil.readVarInt(buff);
    }

    private static long readVarLong(ByteBuffer buff) {
        return DataUtil.readVarLong(buff);
    }

    private static String readString(ByteBuffer buff, int len) {
        return DataUtil.readString(buff, len);
    }

    private static String readString(ByteBuffer buff) {
        int len = readVarInt(buff);
        return DataUtil.readString(buff, len);
    }

    @Override
    public int hashCode() {
        throw new RuntimeException("hashCode ERROR");
//        return compareMode.hashCode() ^ Arrays.hashCode(sortTypes);
    }

    @Override
    public boolean equals(Object obj) {
        throw new RuntimeException("equals ERROR");
//        if (obj == this) {
//            return true;
//        } else if (!(obj instanceof ValueDataType)) {
//            return false;
//        }
//        ValueDataType v = (ValueDataType) obj;
//        if (!compareMode.equals(v.compareMode)) {
//            return false;
//        }
//        return Arrays.equals(sortTypes, v.sortTypes);
    }

}
