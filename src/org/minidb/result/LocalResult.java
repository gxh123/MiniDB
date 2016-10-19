package org.minidb.result;


import org.minidb.engine.Database;
import org.minidb.engine.Session;
import org.minidb.expression.Expression;
import org.minidb.value.Value;

import java.util.ArrayList;

public class LocalResult implements ResultInterface, ResultTarget {

    private Session session;
    private int visibleColumnCount;
    private Expression[] expressions;
    private ArrayList<Value[]> rows;
    private int rowId, rowCount;
    private Value[] currentRow;

    public LocalResult(Session session, Expression[] expressions,
                       int visibleColumnCount) {
        this.session = session;
        rows = new ArrayList();
        this.visibleColumnCount = visibleColumnCount;
        rowId = -1;
        this.expressions = expressions;
    }

    public void addRow(Value[] values) {
        rows.add(values);
        rowCount++;
    }

    @Override
    public int getRowCount() {
        return rowCount;
    }

    @Override
    public Value[] currentRow() {
        return currentRow;
    }

    @Override
    public boolean next() {
        if (rowId < rowCount) {
            rowId++;
            if (rowId < rowCount) {
                currentRow = rows.get(rowId);
                return true;
            }
            currentRow = null;
        }
        return false;
    }

    public String getAlias(int i) {
        return expressions[i].getAlias();
    }

    public int getVisibleColumnCount() {
        return visibleColumnCount;
    }
}
