package org.minidb.value;

import org.minidb.util.StatementBuilder;

/**
 * Created by gxh on 2016/6/24.
 */
public class ValueArray extends Value{

    private final Value[] values;

    private ValueArray(Value[] list){
        this.values = list;
    }

    public Value[] getList(){
        return values;
    }

    public int getType() {
        return ARRAY;
    }

    public static ValueArray get(Value[] list) {
        return new ValueArray(list);
    }

    @Override
    protected int compareSecure(Value o) {
        ValueArray v = (ValueArray) o;
        if (values == v.values) {
            return 0;
        }
        int l = values.length;
        int ol = v.values.length;
        int len = Math.min(l, ol);
        for (int i = 0; i < len; i++) {
            Value v1 = values[i];
            Value v2 = v.values[i];
            int comp = v1.compareTo(v2);
            if (comp != 0) {
                return comp;
            }
        }
        return l > ol ? 1 : l == ol ? 0 : -1;
    }

    @Override
    public String getSQL() {
        StatementBuilder buff = new StatementBuilder("(");
        for (Value v : values) {
            buff.appendExceptFirst(", ");
            buff.append(v.getSQL());
        }
        if (values.length == 1) {
            buff.append(',');
        }
        return buff.append(')').toString();
    }
}
