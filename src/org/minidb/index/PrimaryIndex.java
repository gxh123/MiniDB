package org.minidb.index;

import org.minidb.engine.Session;
import org.minidb.table.Column;
import org.minidb.table.Row;
import org.minidb.table.Table;
import org.minidb.table.TableFilter;
import org.minidb.value.ValueLong;

import java.util.HashSet;

/**
 * Created by gxh on 2016/6/12.
 */
public class PrimaryIndex extends BaseIndex{

    static final ValueLong MIN = ValueLong.get(Long.MIN_VALUE);
    static final ValueLong MAX = ValueLong.get(Long.MAX_VALUE);

    public PrimaryIndex(Table table, int id, String name, Column column, IndexType newIndexType) {
        initBaseIndex(table, id, name,
                column, newIndexType);
    }

    @Override
    public Cursor find(Session session, Row first, Row last) {
        ValueLong min = table.getKey(first, MIN, MIN);
        ValueLong max = table.getKey(last, MAX, MIN);
        return table.find(session, min, max);
    }

    public void add(Session session, Row row) {
        //nothing to do
    }

    public boolean needRebuild() {
        return false;
    }

    @Override
    public double getCost(Session session, int[] masks, TableFilter filter, HashSet<Column> allColumnsSet) {
        return getCostRangeIndex(masks, table.getRowCount(session),
                filter, true, allColumnsSet);
    }
}
