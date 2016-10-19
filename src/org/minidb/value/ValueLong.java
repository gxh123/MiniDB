package org.minidb.value;

import org.minidb.util.MathUtils;

import java.math.BigInteger;

/**
 * Created by gxh on 2016/6/24.
 */
public class ValueLong extends Value{

    public static final BigInteger MAX = BigInteger.valueOf(Long.MAX_VALUE);

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 19;

    /**
     * The maximum display size of a long.
     * Example: 9223372036854775808 //是 -9223372036854775808，"-9223372036854775808"这个算负号在内总长度是20
     */
    public static final int DISPLAY_SIZE = 20;

    private final long value;

    public ValueLong(long value){
        this.value = value;
    }

    public int getType() {
        return LONG;
    }

    public static ValueLong get(long i) {
        return new ValueLong(i);
    }

    @Override
    public long getLong() {
        return value;
    }

    @Override
    protected int compareSecure(Value o) {
        ValueLong v = (ValueLong) o;
        return MathUtils.compareLong(value, v.value);
    }

    public int getSignum() {
        return Long.signum(value);
    }

    @Override
    public String getSQL() {
        return getString();
    }

    @Override
    public String getString() {
        return String.valueOf(value);
    }

}
