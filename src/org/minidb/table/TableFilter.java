/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.table;

import org.minidb.command.dml.Select;
import org.minidb.engine.Session;
import org.minidb.expression.Comparison;
import org.minidb.expression.ConditionAndOr;
import org.minidb.expression.Expression;
import org.minidb.expression.ExpressionColumn;
import org.minidb.index.BaseIndex;
import org.minidb.index.IndexCondition;
import org.minidb.index.IndexCursor;
import org.minidb.value.Value;
import org.minidb.value.ValueLong;
import org.minidb.value.ValueNull;

import java.util.ArrayList;
import java.util.HashSet;

public class TableFilter implements ColumnResolver {

    private static final int BEFORE_FIRST = 0, FOUND = 1, AFTER_LAST = 2,
            NULL_ROW = 3;

    private Session session;
    private final Table table;
//    private final Select select;
    private BaseIndex index;
    private final IndexCursor cursor;
    private final ArrayList<IndexCondition> indexConditions = new ArrayList();
    private Expression filterCondition;

    private Row currentSearchRow;
    private Row current;
    private int state;
    private boolean foundOne;
    private int[] masks;

    public TableFilter(Session session, Table table) {
        this.session = session;
        this.table = table;
        this.cursor = new IndexCursor(this);
    }

    public void setIndex(BaseIndex index) {
        this.index = index;
        cursor.setIndex(index);
    }

    public Table getTable() {
        return table;
    }

    public Session getSession() {
        return session;
    }


    /**
     * Get the best plan item (index, cost) to use use for the current join
     * order.
     *
     * @param s the session
     * @param allColumnsSet the set of all columns
     * @return the best plan item
     */
    public PlanItem getBestPlanItem(Session s, TableFilter tableFilter, HashSet<Column> allColumnsSet) {
        int len = table.getColumns().length;
        int[] masks = new int[len];
        for (IndexCondition condition : indexConditions) {
            int id = condition.getColumn().getColumnId();
            if (id >= 0) {
                masks[id] |= condition.getMask(indexConditions);
            }
        }
        PlanItem item = table.getBestPlanItem(s, masks, tableFilter, allColumnsSet);
        item.setMasks(masks);
        return item;
    }

    /**
     * Reset to the current position.
     */
    public void reset() {
        state = BEFORE_FIRST;
        foundOne = false;
    }

    /**
     * Check if there are more rows to read.
     *
     * @return true if there are
     */
    public boolean next() {
        if (state == AFTER_LAST) {
            return false;
        } else if (state == BEFORE_FIRST) {
            cursor.find(session, indexConditions);
        } else {
        }
        while (true) {
            if (cursor.next()) {
                currentSearchRow = cursor.getSearchRow();
                current = null;
                state = FOUND;
            } else {
                state = AFTER_LAST;
                break;
            }

            if (state == FOUND) {
                foundOne = true;
                return true;
            }
        }
        state = AFTER_LAST;
        return false;
    }

    public void setPlanItem(PlanItem item) {
        if (item == null) {
            // invalid plan, most likely because a column wasn't found
            // this will result in an exception later on
            return;
        }
        setIndex(item.getIndex());
        masks = item.getMasks();
    }

    public void addIndexCondition(IndexCondition condition) {
        indexConditions.add(condition);
    }

    @Override
    public TableFilter getTableFilter() {
        return this;
    }

    @Override
    public Column[] getColumns() {
        return table.getColumns();
    }

    @Override
    public Value getValue(Column column) {
        if (currentSearchRow == null) {
            return null;
        }
        int columnId = column.getColumnId();
        if (columnId == -1) {
            return ValueLong.get(currentSearchRow.getKey());
        }
        if (current == null) {
            Value v = currentSearchRow.getValue(columnId);
            if (v != null) {
                return v;
            }
            current = cursor.get();
            if (current == null) {
                return ValueNull.INSTANCE;
            }
        }
        return current.getValue(columnId);
    }
}
