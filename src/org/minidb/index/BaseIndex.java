package org.minidb.index;

import org.minidb.engine.*;
import org.minidb.schema.SchemaObjectBase;
import org.minidb.table.*;
import org.minidb.util.StatementBuilder;
import org.minidb.value.ValueLong;

import java.util.HashSet;

/**
 * Created by gxh on 2016/6/10.
 */
public abstract class BaseIndex extends SchemaObjectBase {

    protected Table table;
    protected IndexType indexType;
    protected Column column;
    protected IndexColumn[] indexColumns;

    protected void initBaseIndex(Table newTable, int id, String name,
                                 Column column, IndexType newIndexType) {
        initSchemaObjectBase(newTable.getSchema(), id, name);
//        this.indexColumns = column;
        this.table = newTable;
        this.column = column;
        this.indexType = newIndexType;
    }

    public abstract Cursor find(Session session, Row start, Row end);

    public abstract void add(Session session, Row row);

    @Override
    public int getType() {
        return DbObject.INDEX;
    }

    public abstract boolean needRebuild();

    public String getCreateSQLForCopy(Table targetTable, String quotedName) {
        StringBuilder buff = new StringBuilder("CREATE ");
        buff.append(indexType.getSQL());
        buff.append(' ');
        if (table.isHidden()) {
            buff.append("IF NOT EXISTS ");
        }
        buff.append(quotedName);
        buff.append(" ON ").append(targetTable.getSQL());
        buff.append('(').append(getColumnListSQL()).append(')');
        return buff.toString();
    }

    @Override
    public String getCreateSQL() {
        return getCreateSQLForCopy(table, getSQL());
    }

    private String getColumnListSQL() {
        StatementBuilder buff = new StatementBuilder();
//        for (Column c : columns) {
//            buff.appendExceptFirst(", ");
//            buff.append(c.getSQL());
//        }
        buff.appendExceptFirst(", ");
        buff.append(column.getSQL());   //先考虑单列
        return buff.toString();
    }

    public Table getTable() {
        return table;
    }

    public IndexColumn[] getIndexColumns() {
        return indexColumns;
    }

    protected final long getCostRangeIndex(int[] masks, long rowCount, TableFilter filter,
                                           boolean isScanIndex, HashSet<Column> allColumnsSet) {
        int totalSelectivity = 0;
        long rowsCost = rowCount;
        if (masks != null) {
            int index = column.getColumnId();
            int mask = masks[index];
            if ((mask & IndexCondition.EQUALITY) == IndexCondition.EQUALITY) {
                totalSelectivity = 50;    //暂时先给50
                long distinctRows = rowCount * totalSelectivity / 100;
                if (distinctRows <= 0) {
                    distinctRows = 1;
                }
                rowsCost = 2 + Math.max(rowCount / distinctRows, 1);
            } else if ((mask & IndexCondition.RANGE) == IndexCondition.RANGE) {
                rowsCost = 2 + rowCount / 4;
            } else if ((mask & IndexCondition.START) == IndexCondition.START) {
                rowsCost = 2 + rowCount / 3;
            } else if ((mask & IndexCondition.END) == IndexCondition.END) {
                rowsCost = rowCount / 3;
            }
        }
        return rowsCost;
    }

    public Cursor find(TableFilter filter, Row first, Row last) {
        return find(filter.getSession(), first, last);
    }

    public abstract double getCost(Session session, int[] masks, TableFilter filter,HashSet<Column> allColumnsSet);

    public IndexType getIndexType() {
        return indexType;
    }
}
