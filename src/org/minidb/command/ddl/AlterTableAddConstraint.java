/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.command.ddl;

import org.minidb.command.CommandInterface;
import org.minidb.command.Prepared;
import org.minidb.engine.Database;
import org.minidb.engine.Session;
import org.minidb.expression.Expression;
import org.minidb.index.BaseIndex;
import org.minidb.index.IndexType;
import org.minidb.schema.Schema;
import org.minidb.table.Column;
import org.minidb.table.IndexColumn;
import org.minidb.table.Table;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * This class represents the statement
 * ALTER TABLE ADD CONSTRAINT
 */
public class AlterTableAddConstraint extends Prepared {

    private int type;
    private String constraintName;
    private BaseIndex index;
    private boolean ifTableExists;
    private final boolean ifNotExists;
    private Schema schema;
    private IndexColumn[] indexColumns;
    private String tableName;

    public AlterTableAddConstraint(Session session, Schema schema,
                                   boolean ifNotExists) {
        super(session);
        this.schema = schema;
        this.ifNotExists = ifNotExists;
    }

    @Override
    public int update() {
        return tryUpdate();
    }

    /**
     * Try to execute the statement.
     *
     * @return the update count
     */
    private int tryUpdate() {
        session.commit(true);
        Database db = session.getDatabase();
        Table table = schema.findTableOrView(session, tableName);
        if (table == null) {
            if (ifTableExists) {
                return 0;
            }
            throw new RuntimeException("tryUpdate ERROR");
        }
//        db.lockMeta(session);
//        table.lock(session, true, true);
        switch (type) {
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY: {
            IndexColumn.mapColumns(indexColumns, table);
            index = table.findPrimaryKey();
            if (index == null) {
                IndexType indexType = IndexType.createPrimaryKey(
                        true, false);
                String indexName = table.getSchema().getUniqueIndexName(
                        session, table, "PRIMARY_KEY_");
                int id = getObjectId();
                try {
                    index = table.addIndex(session, indexName, id,
                            indexColumns[0].column, indexType);
                } finally {
                    schema.freeUniqueName(indexName);
                }
            }
            break;
        }
        default:
            throw new RuntimeException("tryUpdate error");
        }
        return 0;
    }

    public void setIndexColumns(IndexColumn[] indexColumns) {
        this.indexColumns = indexColumns;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }


    public void setType(int type) {
        this.type = type;
    }


    public int getType() {
        return type;
    }


    public void setIndex(BaseIndex index) {
        this.index = index;
    }

    public IndexColumn[] getIndexColumns() {
        return indexColumns;
    }

}
