/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.command.dml;


import org.minidb.command.Command;
import org.minidb.command.CommandInterface;
import org.minidb.command.Prepared;
import org.minidb.engine.Session;
import org.minidb.expression.Comparison;
import org.minidb.expression.Expression;
import org.minidb.expression.ExpressionColumn;
import org.minidb.result.ResultInterface;
import org.minidb.result.ResultTarget;
import org.minidb.table.Column;
import org.minidb.table.Row;
import org.minidb.table.Table;
import org.minidb.util.StatementBuilder;
import org.minidb.value.Value;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * This class represents the statement
 * INSERT
 */
public class Insert extends Prepared implements ResultTarget {

    private Table table;
    private Column[] columns;
    private final ArrayList<Expression[]> list = new ArrayList();
    private Query query;
    private int rowNumber;

    public Insert(Session session) {
        super(session);
    }

//    public void setCommand(Command command) {
//        super.setCommand(command);
//        if (query != null) {
//            query.setCommand(command);
//        }
//    }

    public void setTable(Table table) {
        this.table = table;
    }

    public void setColumns(Column[] columns) {
        this.columns = columns;
    }

    public void setQuery(Query query) {
        this.query = query;
    }


    /**
     * Add a row to this merge statement.
     *
     * @param expr the list of values
     */
    public void addRow(Expression[] expr) {
        list.add(expr);
    }

    @Override
    public int update() {
        return insertRows();
    }

    private int insertRows() {
        setCurrentRowNumber(0);
        rowNumber = 0;
        int listSize = list.size();
        if (listSize > 0) {
            int columnLen = columns.length;
            for (int x = 0; x < listSize; x++) {
                Row newRow = table.getTemplateRow();
                Expression[] expr = list.get(x);
                setCurrentRowNumber(x + 1);
                for (int i = 0; i < columnLen; i++) {
                    Column c = columns[i];
                    int index = c.getColumnId();
                    Expression e = expr[i];
                    if (e != null) {
                        // e can be null (DEFAULT)
                        e = e.optimize(session);
                        Value v = c.convert(e.getValue(session));
                        newRow.setValue(index, v);
                    }
                }
                rowNumber++;
//                table.lock(session, true, false);
                table.addRow(session, newRow);
//                session.log(table, UndoLogRecord.INSERT, newRow);
            }
        }
        return rowNumber;
    }

    @Override
    public void addRow(Value[] values) {
        throw new RuntimeException("addRow ERROR");
//        Row newRow = table.getTemplateRow();
//        setCurrentRowNumber(++rowNumber);
//        for (int j = 0, len = columns.length; j < len; j++) {
//            Column c = columns[j];
//            int index = c.getColumnId();
//            try {
//                Value v = c.convert(values[j]);
//                newRow.setValue(index, v);
//            } catch (DbException ex) {
//                throw setRow(ex, rowNumber, getSQL(values));
//            }
//        }
//        table.validateConvertUpdateSequence(session, newRow);
//        boolean done = table.fireBeforeRow(session, null, newRow);
//        if (!done) {
//            table.addRow(session, newRow);
//            session.log(table, UndoLogRecord.INSERT, newRow);
//            table.fireAfterRow(session, null, newRow, false);
//        }
    }

    @Override
    public int getRowCount() {
        return rowNumber;
    }

    @Override
    public void prepare() {
        if (columns == null) {
            if (list.size() > 0 && list.get(0).length == 0) {
                // special case where table is used as a sequence
                columns = new Column[0];
            } else {
                columns = table.getColumns();
            }
        }
        if (list.size() > 0) {
            for (Expression[] expr : list) {
                for (int i = 0, len = expr.length; i < len; i++) {
                    Expression e = expr[i];
                    if (e != null) {
                        e = e.optimize(session);
                        expr[i] = e;
                    }
                }
            }
        }
    }

    public int getType() {
        return CommandInterface.INSERT;
    }


}
