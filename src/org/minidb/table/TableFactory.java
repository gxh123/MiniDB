package org.minidb.table;

/**
 * Created by gxh on 2016/6/11.
 */
public interface TableFactory {
    public Table createTable(CreateTableData data);
}
