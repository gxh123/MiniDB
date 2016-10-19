/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.command.ddl;

import org.minidb.command.CommandInterface;
import org.minidb.command.Prepared;
import org.minidb.engine.Constants;
import org.minidb.engine.Database;
import org.minidb.engine.Session;
import org.minidb.index.IndexType;
import org.minidb.schema.Schema;
import org.minidb.table.IndexColumn;
import org.minidb.table.Table;

/**
 * This class represents the statement
 * CREATE INDEX
 */
public class CreateIndex extends Prepared {

    private String tableName;
    private String indexName;
    private IndexColumn[] indexColumns;
    private boolean primaryKey, unique, hash, spatial;
    private boolean ifTableExists;
    private boolean ifNotExists;
    private String comment;
    private Schema schema;

    public CreateIndex(Session session, Schema schema) {
        super(session);
        this.schema = schema;
    }

    public void setIfTableExists(boolean b) {
        this.ifTableExists = b;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public void setIndexColumns(IndexColumn[] columns) {
        this.indexColumns = columns;
    }

    @Override
    public int update() {
        session.commit(true);
        Database db = session.getDatabase();
        Table table = schema.findTableOrView(session, tableName);
        if (table == null) {
            if (ifTableExists) {
                return 0;
            }
            throw new RuntimeException("TABLE_OR_VIEW_NOT_FOUND");
        }
        if (schema.findIndex(session, indexName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw new RuntimeException("INDEX_ALREADY_EXISTS");
        }
        int id = getObjectId();
        if (indexName == null) {
            if (primaryKey) {
                indexName = table.getSchema().getUniqueIndexName(session,
                        table, Constants.PREFIX_PRIMARY_KEY);
            } else {
                indexName = table.getSchema().getUniqueIndexName(session,
                        table, Constants.PREFIX_INDEX);
            }
        }
        IndexType indexType;
        if (primaryKey) {
            if (table.findPrimaryKey() != null) {
                throw new RuntimeException("SECOND_PRIMARY_KEY");
            }
            indexType = IndexType.createPrimaryKey(true, hash);
        } else if (unique) {
            indexType = IndexType.createUnique(true, hash);
        } else {
            indexType = IndexType.createNonUnique(true, hash, spatial);
        }
        IndexColumn.mapColumns(indexColumns, table);
        table.addIndex(session, indexName, id, indexColumns[0].column, indexType);
        return 0;
    }

    public void setPrimaryKey(boolean b) {
        this.primaryKey = b;
    }

    public void setUnique(boolean b) {
        this.unique = b;
    }

    public void setHash(boolean b) {
        this.hash = b;
    }

    public void setSpatial(boolean b) {
        this.spatial = b;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public int getType() {
        return CommandInterface.CREATE_INDEX;
    }

}
