/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.table;

import org.minidb.engine.Session;
import org.minidb.expression.Expression;
import org.minidb.expression.ExpressionVisitor;
import java.util.HashSet;

public class Plan {

    private final TableFilter tableFilter;
    private PlanItem planItem = null;
    private final Expression condition;

    public Plan(TableFilter filter, Expression condition) {
        this.tableFilter = filter;
        this.condition = condition;
    }

    public PlanItem getItem() {
        return planItem;
    }

    public TableFilter getTableFilter() {
        return tableFilter;
    }

    public double calculateCost(Session session) {
        final HashSet<Column> allColumnsSet = ExpressionVisitor
                .allColumnsForTableFilters(tableFilter);
        planItem = tableFilter.getBestPlanItem(session, tableFilter, allColumnsSet);
        return planItem.cost;
    }
}
