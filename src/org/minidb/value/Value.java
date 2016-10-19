package org.minidb.value;

import org.minidb.util.Utils;

import java.math.BigDecimal;
import java.sql.Types;

/**
 * Created by gxh on 2016/6/11.
 */
public abstract class Value {

    public static final int UNKNOWN = -1;
    public static final int NULL = 0;
    public static final int BOOLEAN = 1;
    public static final int BYTE = 2;
    public static final int SHORT = 3;
    public static final int INT = 4;
    public static final int LONG = 5;
    public static final int DOUBLE = 6;
    public static final int FLOAT = 7;
    public static final int BYTES = 8;
    public static final int STRING = 9;
    public static final int ARRAY = 10;

    public Boolean getBoolean() {
        return ((ValueBoolean) convertTo(Value.BOOLEAN)).getBoolean();
    }

    public int getInt(){
        return ((ValueInt)convertTo(Value.INT)).getInt();
    }

    public long getLong(){
        return ((ValueLong)convertTo(Value.LONG)).getLong();
    }

    public String getString(){
        return ((ValueString)convertTo(Value.STRING)).getString();
    }

    public abstract int getType();

//    public Value convertTo(int targetType) {
//        if (getType() == targetType) {
//            return this;
//        }
//        return null;
//    }

    public Value convertTo(int targetType) {
        if (getType() == targetType) {
            return this;
        }
        try {
            // decimal conversion
            switch (targetType) {
                case BOOLEAN: {
                    switch (getType()) {
                        case INT:
                        case LONG:
                            return ValueBoolean.get(getSignum() != 0);
                    }
                    break;
                }
                case INT: {
                    switch (getType()) {
                        case BOOLEAN:
                            return ValueInt.get(getBoolean().booleanValue() ? 1 : 0);
                        case LONG:
                            return ValueInt.get(convertToInt(getLong()));
                        case BYTES:
                            return ValueInt.get((int) Long.parseLong(getString(), 16));
                    }
                    break;
                }
                case LONG: {
                    switch (getType()) {
                        case BOOLEAN:
                            return ValueLong.get(getBoolean().booleanValue() ? 1 : 0);
                        case INT:
                            return ValueLong.get(getInt());
                    }
                    break;
                }
            }
            // conversion by parsing the string value
            String s = getString();
            switch (targetType) {
                case NULL:
                    return ValueNull.INSTANCE;
                case BOOLEAN: {
                    if (s.equalsIgnoreCase("true") ||
                            s.equalsIgnoreCase("t") ||
                            s.equalsIgnoreCase("yes") ||
                            s.equalsIgnoreCase("y")) {
                        return ValueBoolean.get(true);
                    } else if (s.equalsIgnoreCase("false") ||
                            s.equalsIgnoreCase("f") ||
                            s.equalsIgnoreCase("no") ||
                            s.equalsIgnoreCase("n")) {
                        return ValueBoolean.get(false);
                    } else {
                        // convert to a number, and if it is not 0 then it is true
                        return ValueBoolean.get(new BigDecimal(s).signum() != 0);
                    }
                }
                case INT:
                    return ValueInt.get(Integer.parseInt(s.trim()));
                case LONG:
                    return ValueLong.get(Long.parseLong(s.trim()));
                case STRING:
                    return ValueString.get(s);
                case ARRAY:
                    return ValueArray.get(new Value[]{ValueString.get(s)});
                default:
                    throw new RuntimeException("convertTo ERROR");
            }
        } catch (NumberFormatException e) {
            throw new RuntimeException("convertTo ERROR");
        }
    }

    private static int convertToInt(long x) {
        if (x > Integer.MAX_VALUE || x < Integer.MIN_VALUE) {
            throw new RuntimeException("convertToInt ERROR");
        }
        return (int) x;
    }

    public int getSignum() {
        throw new RuntimeException("getSignum ERROR");
    }

    public final int compareTypeSafe(Value v) {
        if (this == v) {
            return 0;
        } else if (this == ValueNull.INSTANCE) {
            return -1;
        } else if (v == ValueNull.INSTANCE) {
            return 1;
        }
        return compareSecure(v);
    }

    protected abstract int compareSecure(Value v);

    public final int compareTo(Value v) {
        if (this == v) {
            return 0;
        }
        if (this == ValueNull.INSTANCE) {
            return v == ValueNull.INSTANCE ? 0 : -1;
        } else if (v == ValueNull.INSTANCE) {
            return 1;
        }
        if (getType() == v.getType()) {
            return compareSecure(v);
        }
        int t2 = Value.getHigherOrder(getType(), v.getType());
        return convertTo(t2).compareSecure(v.convertTo(t2));
    }

    /**
     * Get the higher value order type of two value types. If values need to be
     * converted to match the other operands value type, the value with the
     * lower order is converted to the value with the higher order.
     *
     * @param t1 the first value type
     * @param t2 the second value type
     * @return the higher value type of the two
     */
    public static int getHigherOrder(int t1, int t2) {
        if (t1 == Value.UNKNOWN || t2 == Value.UNKNOWN) {
            if (t1 == t2) {
                throw new RuntimeException("getHigherOrder ERROR");
            } else if (t1 == Value.NULL) {
                throw new RuntimeException("getHigherOrder ERROR");
            } else if (t2 == Value.NULL) {
                throw new RuntimeException("getHigherOrder ERROR");
            }
        }
        if (t1 == t2) {
            return t1;
        }
        int o1 = getOrder(t1);
        int o2 = getOrder(t2);
        return o1 > o2 ? t1 : t2;
    }

    /**
     * Get the order of this value type.
     *
     * @param type the value type
     * @return the order number
     */
    static int getOrder(int type) {
        switch (type) {
            case UNKNOWN:
                return 1;
            case NULL:
                return 2;
            case STRING:
                return 10;
            case BOOLEAN:
                return 20;
            case BYTE:
                return 21;
            case SHORT:
                return 22;
            case INT:
                return 23;
            case LONG:
                return 24;
            case FLOAT:
                return 26;
            case DOUBLE:
                return 27;
            case BYTES:
                return 40;
            case ARRAY:
                return 50;
            default:
                throw new RuntimeException("getOrder ERROR");
        }
    }

    public abstract String getSQL();
}
