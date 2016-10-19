/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.expression;

import org.minidb.engine.Session;
import org.minidb.table.ColumnResolver;
import org.minidb.table.TableFilter;
import org.minidb.value.Value;

/**
 * An 'and' or 'or' condition as in WHERE ID=1 AND NAME=?
 */
public class ConditionAndOr extends Condition {

    /**
     * The AND condition type as in ID=1 AND NAME='Hello'.
     */
    public static final int AND = 0;

    /**
     * The OR condition type as in ID=1 OR NAME='Hello'.
     */
    public static final int OR = 1;

    private final int andOrType;
    private Expression left, right;

    public ConditionAndOr(int andOrType, Expression left, Expression right) {
        this.andOrType = andOrType;
        this.left = left;
        this.right = right;
    }

    @Override
    public Value getValue(Session session) {
        throw new RuntimeException("getValue ERROR");
    }

    @Override
    public Expression optimize(Session session) {
        throw new RuntimeException("optimize ERROR");
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        throw new RuntimeException("ERROR");
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean value) {
        throw new RuntimeException("ERROR");
    }

    public String getSQL() {
        String sql;
        switch (andOrType) {
            case AND:
                sql = left.getSQL() + "\n    AND " + right.getSQL();
                break;
            case OR:
                sql = left.getSQL() + "\n    OR " + right.getSQL();
                break;
            default:
                throw new RuntimeException("getSQL");
        }
        return "(" + sql + ")";
    }
}
