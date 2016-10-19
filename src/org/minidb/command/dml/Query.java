/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.command.dml;

import org.minidb.command.Prepared;
import org.minidb.engine.Database;
import org.minidb.engine.Session;
import org.minidb.expression.Expression;
import org.minidb.result.LocalResult;
import org.minidb.result.ResultTarget;
import org.minidb.table.ColumnResolver;
import org.minidb.table.Table;
import org.minidb.table.TableFilter;
import org.minidb.value.Value;
import org.minidb.value.ValueNull;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * Represents a SELECT statement (simple, or union).
 */
public abstract class Query extends Prepared {


    Query(Session session) {
        super(session);
    }

    protected abstract LocalResult queryWithoutCache(ResultTarget target);

    /**
     * Initialize the query.
     */
    public abstract void init();

    /**
     * Map the columns to the given column resolver.
     *
     * @param resolver
     *            the resolver
     * @param level
     *            the subquery level (0 is the top level query, 1 is the first
     *            subquery level)
     */
    public abstract void mapColumns(ColumnResolver resolver, int level);

    @Override
    public boolean isQuery() {
        return true;
    }

    public LocalResult query(int maxrows) {
        return query(null);
    }


    LocalResult query(ResultTarget target) {
        LocalResult r = queryWithoutCache(target);
        return r;
    }

}
