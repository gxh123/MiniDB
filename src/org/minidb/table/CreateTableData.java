package org.minidb.table;

import org.minidb.engine.Session;
import org.minidb.schema.Schema;
import org.minidb.table.Column;

import java.util.ArrayList;

/**
 * Created by gxh on 2016/6/11.
 */
public class CreateTableData {

    public int id;
    public String tableName;
    public Session session;
    public Schema schema;
    public ArrayList<Column> columns = new ArrayList<Column>();    //因为不知道有多少列
}
