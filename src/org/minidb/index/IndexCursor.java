/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.index;

import org.minidb.engine.Session;
import org.minidb.expression.Comparison;
import org.minidb.result.ResultInterface;
import org.minidb.table.*;
import org.minidb.value.Value;
import org.minidb.value.ValueLong;
import org.minidb.value.ValueNull;

import java.util.ArrayList;
import java.util.HashSet;

import static org.minidb.table.IndexColumn.DESCENDING;

/**
 * The filter used to walk through an index. This class supports IN(..)
 * and IN(SELECT ...) optimizations.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class IndexCursor implements Cursor {

    private Session session;
    private final TableFilter tableFilter;
    private BaseIndex index;
    private Table table;
    private IndexColumn[] indexColumns;
    private boolean alwaysFalse;

    private Row start, end;
    private Cursor cursor;
    private Column inColumn;
    private int inListIndex;
    private Value[] inList;
    private ResultInterface inResult;
    private HashSet<Value> inResultTested;

    public IndexCursor(TableFilter filter) {
        this.tableFilter = filter;
    }

    public void setIndex(BaseIndex index) {
        this.index = index;
        this.table = index.getTable();
        Column[] columns = table.getColumns();
        indexColumns = new IndexColumn[columns.length];
        IndexColumn[] idxCols = index.getIndexColumns();
        if (idxCols != null) {
            for (int i = 0, len = columns.length; i < len; i++) {
                throw new RuntimeException("ERROR");
//                int idx = index.getColumnIndex(columns[i]);
//                if (idx >= 0) {
//                    indexColumns[i] = idxCols[idx];
//                }
            }
        }
    }

    /**
     * Prepare this index cursor to make a lookup in index.
     *
     * @param s Session.
     * @param indexConditions Index conditions.
     */
    public void prepare(Session s, ArrayList<IndexCondition> indexConditions) {
        this.session = s;
        alwaysFalse = false;
        start = end = null;
        inList = null;
        inColumn = null;
        inResult = null;
        inResultTested = null;
        // don't use enhanced for loop to avoid creating objects
        for (int i = 0, size = indexConditions.size(); i < size; i++) {
            IndexCondition condition = indexConditions.get(i);
            if (condition.isAlwaysFalse()) {
                alwaysFalse = true;
                break;
            }
            Column column = condition.getColumn();
            if (condition.getCompareType() == Comparison.IN_LIST) {
                if (start == null && end == null) {
                    if (canUseIndexForIn(column)) {
                        this.inColumn = column;
                        inList = condition.getCurrentValueList(s);
                        inListIndex = 0;
                    }
                }
            } else if (condition.getCompareType() == Comparison.IN_QUERY) {
                if (start == null && end == null) {
                    if (canUseIndexForIn(column)) {
                        this.inColumn = column;
                        inResult = condition.getCurrentResult();
                    }
                }
            } else {
                Value v = condition.getCurrentValue(s);
                boolean isStart = condition.isStart();
                boolean isEnd = condition.isEnd();
                boolean isIntersects = condition.isSpatialIntersects();
                int columnId = column.getColumnId();
                if (columnId >= 0) {
                    IndexColumn idxCol = indexColumns[columnId];
                    if (idxCol != null && (idxCol.sortType & DESCENDING) != 0) {
                        // if the index column is sorted the other way, we swap
                        // end and start NULLS_FIRST / NULLS_LAST is not a
                        // problem, as nulls never match anyway
                        boolean temp = isStart;
                        isStart = isEnd;
                        isEnd = temp;
                    }
                }
                if (isStart) {
                    start = getSearchRow(start, columnId, v, true);
                }
                if (isEnd) {
                    end = getSearchRow(end, columnId, v, false);
                }
                if (isIntersects) {
//                    intersects = getSpatialSearchRow(intersects, columnId, v);
                }
                if (isStart || isEnd) {
                    // an X=? condition will produce less rows than
                    // an X IN(..) condition
                    inColumn = null;
                    inList = null;
                    inResult = null;
                }
//                if (!session.getDatabase().getSettings().optimizeIsNull) {
//                    if (isStart && isEnd) {
//                        if (v == ValueNull.INSTANCE) {
//                            // join on a column=NULL is always false
//                            alwaysFalse = true;
//                        }
//                    }
//                }
            }
        }
    }

    /**
     * Re-evaluate the start and end values of the index search for rows.
     *
     * @param s the session
     * @param indexConditions the index conditions
     */
    public void find(Session s, ArrayList<IndexCondition> indexConditions) {
        prepare(s, indexConditions);
        if (inColumn != null) {
            return;
        }
        if (!alwaysFalse) {
//            if (intersects != null && index instanceof SpatialIndex) {
//                cursor = ((SpatialIndex) index).findByGeometry(tableFilter,
//                        start, end, intersects);
//            } else
            {
                cursor = index.find(tableFilter, start, end);
            }
        }
    }

    private boolean canUseIndexForIn(Column column) {
        if (inColumn != null) {
            // only one IN(..) condition can be used at the same time
            return false;
        }
        // The first column of the index must match this column,
        // or it must be a VIEW index (where the column is null).
        // Multiple IN conditions with views are not supported, see
        // IndexCondition.getMask.
        IndexColumn[] cols = index.getIndexColumns();
        if (cols == null) {
            return true;
        }
        IndexColumn idxCol = cols[0];
        return idxCol == null || idxCol.column == column;
    }


    private Row getSearchRow(Row row, int columnId, Value v,
            boolean max) {
        if (row == null) {
            row = table.getTemplateRow();
        } else {
            v = getMax(row.getValue(columnId), v, max);
        }
        if (columnId < 0) {
            row.setKey(v.getLong());
        } else {
            row.setValue(columnId, v);
        }
        return row;
    }

    private Value getMax(Value a, Value b, boolean bigger) {
        if (a == null) {
            return b;
        } else if (b == null) {
            return a;
        }
//        if (session.getDatabase().getSettings().optimizeIsNull) {
//            // IS NULL must be checked later
//            if (a == ValueNull.INSTANCE) {
//                return b;
//            } else if (b == ValueNull.INSTANCE) {
//                return a;
//            }
//        }
        int comp = a.compareTo(b);
        if (comp == 0) {
            return a;
        }
        if (a == ValueNull.INSTANCE || b == ValueNull.INSTANCE) {
//            if (session.getDatabase().getSettings().optimizeIsNull) {
//                // column IS NULL AND column <op> <not null> is always false
//                return null;
//            }
        }
        if (!bigger) {
            comp = -comp;
        }
        return comp > 0 ? a : b;
    }

    /**
     * Check if the result is empty for sure.
     *
     * @return true if it is
     */
    public boolean isAlwaysFalse() {
        return alwaysFalse;
    }

    /**
     * Get start search row.
     *
     * @return search row
     */
    public Row getStart() {
        return start;
    }

    /**
     * Get end search row.
     *
     * @return search row
     */
    public Row getEnd() {
        return end;
    }

    @Override
    public Row get() {
        if (cursor == null) {
            return null;
        }
        return cursor.get();
    }

    @Override
    public Row getSearchRow() {
        return cursor.getSearchRow();
    }

    @Override
    public boolean next() {
        while (true) {
            if (cursor == null) {
                nextCursor();
                if (cursor == null) {
                    return false;
                }
            }
            if (cursor.next()) {
                return true;
            }
            cursor = null;
        }
    }

    private void nextCursor() {
//        throw new RuntimeException("ERROR");
        if (inList != null) {
            while (inListIndex < inList.length) {
                Value v = inList[inListIndex++];
                if (v != ValueNull.INSTANCE) {
                    find(v);
                    break;
                }
            }
        } else if (inResult != null) {
            while (inResult.next()) {
                Value v = inResult.currentRow()[0];
                if (v != ValueNull.INSTANCE) {
                    v = inColumn.convert(v);
                    if (inResultTested == null) {
                        inResultTested = new HashSet<Value>();
                    }
                    if (inResultTested.add(v)) {
                        find(v);
                        break;
                    }
                }
            }
        }
    }

    private void find(Value v) {
//        throw new RuntimeException("ERROR");
        v = inColumn.convert(v);
        int id = inColumn.getColumnId();
        if (start == null) {
            start = table.getTemplateRow();
        }
        start.setValue(id, v);
        cursor = index.find(tableFilter, start, start);
    }

    @Override
    public boolean previous() {
//        throw DbException.throwInternalError();
        throw new RuntimeException("ERROR");
    }

}
