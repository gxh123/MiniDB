/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.expression;

import org.minidb.engine.Database;
import org.minidb.engine.Session;
import org.minidb.index.IndexCondition;
import org.minidb.table.ColumnResolver;
import org.minidb.table.TableFilter;
import org.minidb.value.Value;
import org.minidb.value.ValueBoolean;
import org.minidb.value.ValueNull;

import java.util.Arrays;

/**
 * Example comparison expressions are ID=1, NAME=NAME, NAME IS NULL.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class Comparison extends Condition {

    /**
     * This is a flag meaning the comparison is null safe (meaning never returns
     * NULL even if one operand is NULL). Only EQUAL and NOT_EQUAL are supported
     * currently.
     */
    public static final int NULL_SAFE = 16;

    /**
     * The comparison type meaning = as in ID=1.
     */
    public static final int EQUAL = 0;

    /**
     * The comparison type meaning ID IS 1 (ID IS NOT DISTINCT FROM 1).
     */
    public static final int EQUAL_NULL_SAFE = EQUAL | NULL_SAFE;

    /**
     * The comparison type meaning &gt;= as in ID&gt;=1.
     */
    public static final int BIGGER_EQUAL = 1;

    /**
     * The comparison type meaning &gt; as in ID&gt;1.
     */
    public static final int BIGGER = 2;

    /**
     * The comparison type meaning &lt;= as in ID&lt;=1.
     */
    public static final int SMALLER_EQUAL = 3;

    /**
     * The comparison type meaning &lt; as in ID&lt;1.
     */
    public static final int SMALLER = 4;

    /**
     * The comparison type meaning &lt;&gt; as in ID&lt;&gt;1.
     */
    public static final int NOT_EQUAL = 5;

    /**
     * The comparison type meaning ID IS NOT 1 (ID IS DISTINCT FROM 1).
     */
    public static final int NOT_EQUAL_NULL_SAFE = NOT_EQUAL | NULL_SAFE;

    /**
     * The comparison type meaning IS NULL as in NAME IS NULL.
     */
    public static final int IS_NULL = 6;

    /**
     * The comparison type meaning IS NOT NULL as in NAME IS NOT NULL.
     */
    public static final int IS_NOT_NULL = 7;

    /**
     * This is a pseudo comparison type that is only used for index conditions.
     * It means the comparison will always yield FALSE. Example: 1=0.
     */
    public static final int FALSE = 8;

    /**
     * This is a pseudo comparison type that is only used for index conditions.
     * It means equals any value of a list. Example: IN(1, 2, 3).
     */
    public static final int IN_LIST = 9;

    /**
     * This is a pseudo comparison type that is only used for index conditions.
     * It means equals any value of a list. Example: IN(SELECT ...).
     */
    public static final int IN_QUERY = 10;

    /**
     * This is a comparison type that is only used for spatial index
     * conditions (operator "&&").
     */
    public static final int SPATIAL_INTERSECTS = 11;

    private final Database database;
    private int compareType;
    private Expression left;
    private Expression right;

    public Comparison(Session session, int compareType, Expression left,
                      Expression right) {
        this.database = session.getDatabase();
        this.left = left;
        this.right = right;
        this.compareType = compareType;
    }

    @Override
    public Value getValue(Session session) {
        Value l = left.getValue(session);
        if (right == null) {
            boolean result;
            switch (compareType) {
                case IS_NULL:
                    result = l == ValueNull.INSTANCE;
                    break;
                case IS_NOT_NULL:
                    result = !(l == ValueNull.INSTANCE);
                    break;
                default:
                    throw new RuntimeException("getValue ERROR");
            }
            return ValueBoolean.get(result);
        }
        if (l == ValueNull.INSTANCE) {
            if ((compareType & NULL_SAFE) == 0) {
                return ValueNull.INSTANCE;
            }
        }
        Value r = right.getValue(session);
        if (r == ValueNull.INSTANCE) {
            if ((compareType & NULL_SAFE) == 0) {
                return ValueNull.INSTANCE;
            }
        }
        int dataType = Value.getHigherOrder(left.getType(), right.getType());
        l = l.convertTo(dataType);
        r = r.convertTo(dataType);
        boolean result = compareNotNull(database, l, r, compareType);
        return ValueBoolean.get(result);
    }

    static boolean compareNotNull(Database database, Value l, Value r,
                                  int compareType) {
        boolean result;
        switch (compareType) {
            case EQUAL:
            case EQUAL_NULL_SAFE:
                result = database.areEqual(l, r);
                break;
            case NOT_EQUAL:
            case NOT_EQUAL_NULL_SAFE:
                result = !database.areEqual(l, r);
                break;
            case BIGGER_EQUAL:
                result = database.compare(l, r) >= 0;
                break;
            case BIGGER:
                result = database.compare(l, r) > 0;
                break;
            case SMALLER_EQUAL:
                result = database.compare(l, r) <= 0;
                break;
            case SMALLER:
                result = database.compare(l, r) < 0;
                break;
            default:
                throw new RuntimeException("compareNotNull ERROR");
        }
        return result;
    }

    @Override
    public Expression optimize(Session session) {
        return null;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        left.mapColumns(resolver, level);
        if (right != null) {
            right.mapColumns(resolver, level);
        }
    }

    @Override
    public void createIndexConditions(Session session, TableFilter filter) {
//        if (!filter.getTable().isQueryComparable()) {
//            return;
//        }
        ExpressionColumn l = null;
        if (left instanceof ExpressionColumn) {
            l = (ExpressionColumn) left;
            if (filter != l.getTableFilter()) {
                l = null;
            }
        }
        if (right == null) {
            if (l != null) {
                switch (compareType) {
                    case IS_NULL:
                        throw new RuntimeException("ERROR");
//                        if (session.getDatabase().getSettings().optimizeIsNull) {
//                            filter.addIndexCondition(
//                                    IndexCondition.get(
//                                            Comparison.EQUAL_NULL_SAFE, l,
//                                            ValueExpression.getNull()));
//                        }
                }
            }
            return;
        }
        ExpressionColumn r = null;
        if (right instanceof ExpressionColumn) {
            r = (ExpressionColumn) right;
            if (filter != r.getTableFilter()) {
                r = null;
            }
        }
        // one side must be from the current filter
        if (l == null && r == null) {
            return;
        }
        if (l != null && r != null) {
            return;
        }
        if (l == null) {
            ExpressionVisitor visitor =
                    ExpressionVisitor.getNotFromResolverVisitor(filter);
//            throw new RuntimeException("ERROR");
//            if (!left.isEverything(visitor)) {
//                return;
//            }
        } else if (r == null) {
            ExpressionVisitor visitor =
                    ExpressionVisitor.getNotFromResolverVisitor(filter);
//            throw new RuntimeException("ERROR");
//            if (!right.isEverything(visitor)) {
//                return;
//            }
        } else {
            // if both sides are part of the same filter, it can't be used for
            // index lookup
            return;
        }
        boolean addIndex;
        switch (compareType) {
            case NOT_EQUAL:
            case NOT_EQUAL_NULL_SAFE:
                addIndex = false;
                break;
            case EQUAL:
            case EQUAL_NULL_SAFE:
            case BIGGER:
            case BIGGER_EQUAL:
            case SMALLER_EQUAL:
            case SMALLER:
            case SPATIAL_INTERSECTS:
                addIndex = true;
                break;
            default:
                throw new RuntimeException("ERROR");
//                throw DbException.throwInternalError("type=" + compareType);
        }
        if (addIndex) {
            if (l != null) {
                filter.addIndexCondition(
                        IndexCondition.get(compareType, l, right));
            } else if (r != null) {
                throw new RuntimeException("ERROR");
//                int compareRev = getReversedCompareType(compareType);
//                filter.addIndexCondition(
//                        IndexCondition.get(compareRev, r, left));
            }
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean value) {
        throw new RuntimeException("ERROR");
    }

    public String getSQL() {
        String sql;
        switch (compareType) {
            case IS_NULL:
                sql = left.getSQL() + " IS NULL";
                break;
            case IS_NOT_NULL:
                sql = left.getSQL() + " IS NOT NULL";
                break;
            case SPATIAL_INTERSECTS:
                sql = "INTERSECTS(" + left.getSQL() + ", " + right.getSQL() + ")";
                break;
            default:
                sql = left.getSQL() + " " + getCompareOperator(compareType) +
                        " " + right.getSQL();
        }
        return "(" + sql + ")";
    }

    static String getCompareOperator(int compareType) {
        switch (compareType) {
            case EQUAL:
                return "=";
            case EQUAL_NULL_SAFE:
                return "IS";
            case BIGGER_EQUAL:
                return ">=";
            case BIGGER:
                return ">";
            case SMALLER_EQUAL:
                return "<=";
            case SMALLER:
                return "<";
            case NOT_EQUAL:
                return "<>";
            case NOT_EQUAL_NULL_SAFE:
                return "IS NOT";
            case SPATIAL_INTERSECTS:
                return "&&";
            default:
                throw new RuntimeException("getCompareOperator");
        }
    }

}
