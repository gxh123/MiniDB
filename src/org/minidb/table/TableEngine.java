package org.minidb.table;

import org.minidb.engine.Database;
import org.minidb.engine.Session;
import org.minidb.store.Store;

/**
 * Created by gxh on 2016/6/11.
 */
public class TableEngine implements TableFactory {

    public Table createTable(CreateTableData data) {
        Database database = data.session.getDatabase();
        Store store = database.openStore();
        Table table = new Table(data, database);
//        table.init(data.session);
        return table;
    }
}
