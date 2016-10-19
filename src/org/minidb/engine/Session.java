package org.minidb.engine;

import org.minidb.command.Command;
import org.minidb.command.Parser;
import org.minidb.command.Prepared;
import org.minidb.store.mvstore.TransactionStore.Transaction;
import org.minidb.table.Row;
import org.minidb.value.Value;

/**
 * Created by gxh on 2016/6/10.
 */
public class Session {
    private Database database;
    private User user;
    private Transaction transaction;

    public Session(Database database, User user){
        this.database = database;
        this.user = user;
    }

    public Database getDatabase(){
        return database;
    }

    public Row createRow(Value[] data){
        return database.createRow(data);
    }

    public Transaction getTransaction() {
        if (transaction == null) {
            transaction = database.getStore().getTransactionStore().begin();
        }
        return transaction;
    }

    public Command prepareCommand(String sql) {
        Command command;
        Parser parser = new Parser(this);
        command = parser.prepareCommand(sql);
        return command;
    }

    public String getCurrentSchemaName() {
        return "PUBLIC";
    }

    public Prepared prepare(String sql) {
        Parser parser = new Parser(this);
        return parser.prepare(sql);
    }

    public void commit(boolean ddl) {
        if (transaction != null) {
            // increment the data mod count, so that other sessions
            // see the changes
            // TODO should not rely on locking
//            if (locks.size() > 0) {    //先注释！！！
//                for (int i = 0, size = locks.size(); i < size; i++) {
//                    Table t = locks.get(i);
//                    if (t instanceof MVTable) {
//                        ((MVTable) t).commit();
//                    }
//                }
//            }
            transaction.commit();
            transaction = null;
        }
//        if (containsUncommitted()) {
            // need to commit even if rollback is not possible
            // (create/drop table and so on)
//            database.commit(this);
//        }
//        if (undoLog.size() > 0) {                //先不考虑回滚
//            // commit the rows when using MVCC
//            if (database.isMultiVersion()) {
//                ArrayList<Row> rows = New.arrayList();
//                synchronized (database) {
//                    while (undoLog.size() > 0) {
//                        UndoLogRecord entry = undoLog.getLast();
//                        entry.commit();
//                        rows.add(entry.getRow());
//                        undoLog.removeLast(false);
//                    }
//                    for (int i = 0, size = rows.size(); i < size; i++) {
//                        Row r = rows.get(i);
//                        r.commit();
//                    }
//                }
//            }
//            undoLog.clear();
//        }
//        if (!ddl) {
            // do not clean the temp tables if the last command was a
            // create/drop
//            cleanTempTables(false);
//        }
//        endTransaction();
    }
}
