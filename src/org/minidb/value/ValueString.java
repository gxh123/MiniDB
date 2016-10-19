package org.minidb.value;

import org.minidb.util.StringUtils;

/**
 * Created by gxh on 2016/6/24.
 */
public class ValueString extends Value{

    private String value;

    public ValueString(String value){
        this.value = value;
    }

    public int getType() {
        return STRING;
    }

    public static ValueString get(String value){
        return new ValueString(value);
    }

    public String getString(){
        return value;
    }

    @Override
    protected int compareSecure(Value o) {
        ValueString v = (ValueString) o;
        return value.compareTo(v.value);
    }

    @Override
    public String getSQL() {
        return StringUtils.quoteStringSQL(value);
    }
}
