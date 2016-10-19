/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.table;

import org.minidb.command.dml.Select;
import org.minidb.expression.Expression;
import org.minidb.expression.ExpressionColumn;
import org.minidb.value.Value;

/**
 * A column resolver is list of column (for example, a table) that can map a
 * column name to an actual column.
 */
public interface ColumnResolver {

    TableFilter getTableFilter();

    Column[] getColumns();

    Value getValue(Column column);
}
