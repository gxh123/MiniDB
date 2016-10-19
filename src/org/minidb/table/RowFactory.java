package org.minidb.table;

import org.minidb.value.Value;

/**
 * Created by gxh on 2016/6/24.
 */
public class RowFactory {

    public Row createRow(Value[] data){
        return new Row(data);
    }

}
