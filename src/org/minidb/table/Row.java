package org.minidb.table;

import org.minidb.value.Value;

/**
 * Created by gxh on 2016/6/13.
 */
public class Row {
    private long key;
    private final Value[] data;

    public Row(Value[] data){
        this.data = data;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public long getKey() {
        return key;
    }

    public Value getValue(int i){
        return data[i];
    }

    public Value[] getValueList() {
        return data;
    }

    public void setValue(int i, Value v){
        data[i] = v;
    }
}
