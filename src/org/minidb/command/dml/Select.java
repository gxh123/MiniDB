/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.command.dml;

import org.minidb.command.CommandInterface;
import org.minidb.engine.Database;
import org.minidb.engine.Session;
import org.minidb.expression.ConditionAndOr;
import org.minidb.expression.Expression;
import org.minidb.expression.ExpressionColumn;
import org.minidb.result.LocalResult;
import org.minidb.result.ResultTarget;
import org.minidb.table.*;
import org.minidb.value.Value;

import java.util.ArrayList;


public class Select extends Query {
    private TableFilter tableFilter;
    private final ArrayList<TableFilter> topFilters = new ArrayList();
    private ArrayList<Expression> expressions;
    private Expression[] expressionArray;
    private Expression having;
    private Expression condition;
    private ArrayList<Expression> group;
    private int[] groupIndex;
    private boolean[] groupByExpression;
    private int havingIndex;
    private double cost;
    private int visibleColumnCount;

    public Select(Session session) {
        super(session);
    }

    public void setTableFilter(TableFilter tableFilter) {
        this.tableFilter = tableFilter;
    }

    public void setExpressions(ArrayList<Expression> expressions) {
        this.expressions = expressions;
    }

    /**
     * Add a condition to the list of conditions.
     *
     * @param cond the condition to add
     */
    public void addCondition(Expression cond) {
        if (condition == null) {
            condition = cond;
        } else {
            condition = new ConditionAndOr(ConditionAndOr.AND, cond, condition);
        }
    }

    private void queryFlat(int columnCount, ResultTarget result, long limitRows) {
        while (tableFilter.next()) {
            if (condition == null ||
                    Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                Value[] row = new Value[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    Expression expr = expressions.get(i);
                    row[i] = expr.getValue(session);
                }
                result.addRow(row);
            }
        }
    }

    @Override
    protected LocalResult queryWithoutCache( ResultTarget target) {
        int columnCount = expressions.size();
        LocalResult result = null;
        if (target == null ) {
            result = createLocalResult(result);
        }
//        tableFilter.startQuery(session);
        tableFilter.reset();
//        topTableFilter.lock(session, exclusive, exclusive);
        ResultTarget to = result != null ? result : target;

        queryFlat(columnCount, result, 1000);

        if (result != null) {
            return result;
        }
        return null;
    }

    private LocalResult createLocalResult(LocalResult old) {
        return old != null ? old : new LocalResult(session, expressionArray,
                visibleColumnCount);
    }

//    private void expandColumnList() {
//        Database db = session.getDatabase();
//
//        // the expressions may change within the loop
//        for (int i = 0; i < expressions.size(); i++) {
//            Expression expr = expressions.get(i);
//            if (!expr.isWildcard()) {
//                continue;
//            }
//            String schemaName = expr.getSchemaName();
//            String tableAlias = expr.getTableAlias();
//            if (tableAlias == null) {
//                expressions.remove(i);
//                for (TableFilter filter : filters) {
//                    i = expandColumnList(filter, i);
//                }
//                i--;
//            } else {
//                TableFilter filter = null;
//                for (TableFilter f : filters) {
//                    if (db.equalsIdentifiers(tableAlias, f.getTableAlias())) {
//                        if (schemaName == null ||
//                                db.equalsIdentifiers(schemaName,
//                                        f.getSchemaName())) {
//                            filter = f;
//                            break;
//                        }
//                    }
//                }
//                if (filter == null) {
//                    throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1,
//                            tableAlias);
//                }
//                expressions.remove(i);
//                i = expandColumnList(filter, i);
//                i--;
//            }
//        }
//    }

//    private int expandColumnList(TableFilter filter, int index) {
//        Table t = filter.getTable();
//        String alias = filter.getTableAlias();
//        Column[] columns = t.getColumns();
//        for (Column c : columns) {
//            if (filter.isNaturalJoinColumn(c)) {
//                continue;
//            }
//            ExpressionColumn ec = new ExpressionColumn(
//                    session.getDatabase(), null, alias, c.getName());
//            expressions.add(index++, ec);
//        }
//        return index;
//    }

    @Override
    public void init() {
//        expandColumnList();

        visibleColumnCount = expressions.size();

        mapColumns(tableFilter, 0);
    }

    @Override
    public void prepare() {
        if (condition != null) {
            condition.createIndexConditions(session, tableFilter);
        }
        cost = preparePlan();
        expressionArray = new Expression[expressions.size()];
        expressions.toArray(expressionArray);
    }

    private double preparePlan() {
        Optimizer optimizer = new Optimizer(tableFilter, condition, session);
        optimizer.optimize();
        return optimizer.getCost();
    }


    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        for (Expression e : expressions) {
            e.mapColumns(resolver, level);
        }
        if (condition != null) {
            condition.mapColumns(resolver, level);
        }
    }

    public int getType() {
        return CommandInterface.SELECT;
    }

//    public SortOrder getSortOrder() {
//        return sort;
//    }

}
