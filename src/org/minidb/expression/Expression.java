/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.expression;

import org.minidb.engine.Session;
import org.minidb.table.ColumnResolver;
import org.minidb.table.TableFilter;
import org.minidb.util.StringUtils;
import org.minidb.value.Value;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * An expression is a operation, a value, or a function in a query.
 */
public abstract class Expression {

    /**
     * Return the resulting value for the current row.
     *
     * @param session the session
     * @return the result
     */
    public abstract Value getValue(Session session);

    /**
     * Return the data type. The data type may not be known before the
     * optimization phase.
     *
     * @return the type
     */
    public abstract int getType();

    /**
     * Try to optimize the expression.
     *
     * @param session the session
     * @return the optimized expression
     */
    public abstract Expression optimize(Session session);

    public abstract void mapColumns(ColumnResolver resolver, int level);

    /**
     * Create index conditions if possible and attach them to the table filter.
     *
     * @param session the session
     * @param filter the table filter
     */
    public void createIndexConditions(Session session, TableFilter filter) {
        // default is do nothing
    }

    public Boolean getBooleanValue(Session session) {
        return getValue(session).getBoolean();
    }

    public abstract void setEvaluatable(TableFilter tableFilter, boolean value);

    public String getAlias() {
        return StringUtils.unEnclose(getSQL());
    }

    public abstract String getSQL();

}
