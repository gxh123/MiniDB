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
import org.minidb.value.ValueNull;

/**
 * An expression representing a constant value.
 */
public class ValueExpression extends Expression {
    /**
     * The expression represents ValueNull.INSTANCE.
     */
    private static final Object NULL = new ValueExpression(ValueNull.INSTANCE);

    /**
     * This special expression represents the default value. It is used for
     * UPDATE statements of the form SET COLUMN = DEFAULT. The value is
     * ValueNull.INSTANCE, but should never be accessed.
     */
    private static final Object DEFAULT = new ValueExpression(ValueNull.INSTANCE);

    private final Value value;

    private ValueExpression(Value value) {
        this.value = value;
    }

    /**
     * Get the NULL expression.
     *
     * @return the NULL expression
     */
    public static ValueExpression getNull() {
        return (ValueExpression) NULL;
    }

    /**
     * Get the DEFAULT expression.
     *
     * @return the DEFAULT expression
     */
    public static ValueExpression getDefault() {
        return (ValueExpression) DEFAULT;
    }

    /**
     * Create a new expression with the given value.
     *
     * @param value the value
     * @return the expression
     */
    public static ValueExpression get(Value value) {
        if (value == ValueNull.INSTANCE) {
            return getNull();
        }
        return new ValueExpression(value);
    }

    @Override
    public Value getValue(Session session) {
        return value;
    }

    @Override
    public int getType() {
        return value.getType();
    }

    @Override
    public Expression optimize(Session session) {
        return this;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {

    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean value) {
        throw new RuntimeException("ERROR");
    }

    @Override
    public String getSQL() {
        if (this == DEFAULT) {
            return "DEFAULT";
        }
        return value.getSQL();
    }

}
