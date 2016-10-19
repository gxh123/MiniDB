/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.command.dml;

import org.minidb.engine.Session;
import org.minidb.expression.Expression;
import org.minidb.table.Plan;
import org.minidb.table.PlanItem;
import org.minidb.table.TableFilter;
import org.minidb.util.BitField;

import java.util.Random;

/**
 * The optimizer is responsible to find the best execution plan
 * for a given query.
 */
class Optimizer {

    private final TableFilter tableFilter;
    private final Expression condition;
    private final Session session;

    private Plan bestPlan;
    private double cost;

    Optimizer(TableFilter filter, Expression condition, Session session) {
        this.tableFilter = filter;
        this.condition = condition;
        this.session = session;
    }

    private void calculateBestPlan() {
        cost = -1;
        Plan p = new Plan(tableFilter, condition);
        double costNow = p.calculateCost(session);
        if (cost < 0 || costNow < cost) {
            cost = costNow;
            bestPlan = p;
        }
    }

    void optimize() {
        calculateBestPlan();
        PlanItem item = bestPlan.getItem();
        bestPlan.getTableFilter().setPlanItem(item);
    }

    public TableFilter getTableFilter() {
        return tableFilter;
    }

    double getCost() {
        return cost;
    }

}
