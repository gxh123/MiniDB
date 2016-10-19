package org.minidb.value;

import org.minidb.util.MathUtils;

/**
 * Created by gxh on 2016/6/24.
 */
public class ValueInt extends Value{

    public static final int PRECISION = 10;

    public static final int DISPLAY_SIZE = 11;

    private int value;

    public ValueInt(int value){
        this.value = value;
    }

    public int getType() {
        return INT;
    }

    public static ValueInt get(int i) {
        return new ValueInt(i);
    }

    public int getInt(){
        return value;
    }

    @Override
    public long getLong() {
        return value;
    }

    protected int compareSecure(Value o) {
        ValueInt v = (ValueInt) o;
        return MathUtils.compareInt(value, v.value);
    }

    public int getSignum() {
        return Integer.signum(value);
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
