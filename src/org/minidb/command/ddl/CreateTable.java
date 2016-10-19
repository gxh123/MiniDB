package org.minidb.command.ddl;

import org.minidb.command.CommandInterface;
import org.minidb.command.Prepared;
import org.minidb.engine.Database;
import org.minidb.schema.Schema;
import org.minidb.engine.Session;
import org.minidb.table.Column;
import org.minidb.table.CreateTableData;
import org.minidb.table.IndexColumn;
import org.minidb.table.Table;

import java.util.ArrayList;

/**
 * Created by gxh on 2016/7/12.
 */
public class CreateTable extends Prepared{

    private final CreateTableData data = new CreateTableData();
    protected Schema schema;
    private final ArrayList<Prepared> constraintCommands = new ArrayList();
    private IndexColumn[] pkColumns;


    public CreateTable(Session session, Schema schema) {
        super(session);
        this.schema = schema;
    }

    public void setTableName(String tableName) {
        data.tableName = tableName;
    }

    public void addColumn(Column column) {
        data.columns.add(column);
    }

    @Override
    public int update() {
        Database db = session.getDatabase();
//        db.lockMeta(session);
        if (schema.findTableOrView(session, data.tableName) != null) {
            throw new RuntimeException("TABLE_OR_VIEW_ALREADY_EXISTS ERROR");
        }
        data.id = getObjectId(); //第一次新建时会分配一个id
        data.session = session;
        Table table = schema.createTable(data);

//        db.lockMeta(session);
        db.addSchemaObject(session, table);

        for (Prepared command : constraintCommands) {
            command.update();
        }

        return 0;
    }

    public void addConstraintCommand(Prepared command) {
//        if (command instanceof CreateIndex) {
//            constraintCommands.add(command);
//        } else
        {
            AlterTableAddConstraint con = (AlterTableAddConstraint) command;
            boolean alreadySet;
            if (con.getType() == CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY) {
                alreadySet = setPrimaryKeyColumns(con.getIndexColumns());
            } else {
                alreadySet = false;
            }
            if (!alreadySet) {
                constraintCommands.add(command);
            }
        }
    }

    private boolean setPrimaryKeyColumns(IndexColumn[] columns) {
        if (pkColumns != null) {
            int len = columns.length;
            if (len != pkColumns.length) {
                throw new RuntimeException("SECOND_PRIMARY_KEY ");
            }
            for (int i = 0; i < len; i++) {
                if (!columns[i].columnName.equals(pkColumns[i].columnName)) {
                    throw new RuntimeException("SECOND_PRIMARY_KEY ");
                }
            }
            return true;
        }
        this.pkColumns = columns;
        return false;
    }
}

