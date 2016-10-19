/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.expression;


import org.minidb.command.Parser;
import org.minidb.command.dml.Select;
import org.minidb.engine.Database;
import org.minidb.engine.Session;
import org.minidb.schema.Schema;
import org.minidb.table.Column;
import org.minidb.table.ColumnResolver;
import org.minidb.table.Table;
import org.minidb.table.TableFilter;
import org.minidb.value.Value;

import java.util.HashMap;

/**
 * A expression that represents a column of a table or view.
 */
public class ExpressionColumn extends Expression {

    private final Database database;
    private final String schemaName;
    private final String tableAlias;
    private final String columnName;
    private ColumnResolver columnResolver;
    private int queryLevel;
    private Column column;
    private boolean evaluatable;

    public ExpressionColumn(Database database, Column column) {
        this.database = database;
        this.column = column;
        this.schemaName = null;
        this.tableAlias = null;
        this.columnName = null;
    }

    public ExpressionColumn(Database database, String schemaName,
                            String tableAlias, String columnName) {
        this.database = database;
        this.schemaName = schemaName;
        this.tableAlias = tableAlias;
        this.columnName = columnName;
    }

    public String getSQL() {
        String sql;
//        boolean quote = database.getSettings().databaseToUpper;
        boolean quote = true;
        if (column != null) {
            sql = column.getSQL();
        } else {
            sql = quote ? Parser.quoteIdentifier(columnName) : columnName;
        }
        if (tableAlias != null) {
            String a = quote ? Parser.quoteIdentifier(tableAlias) : tableAlias;
            sql = a + "." + sql;
        }
        if (schemaName != null) {
            String s = quote ? Parser.quoteIdentifier(schemaName) : schemaName;
            sql = s + "." + sql;
        }
        return sql;
    }

    public TableFilter getTableFilter() {
        return columnResolver == null ? null : columnResolver.getTableFilter();
    }


    public void mapColumns(ColumnResolver resolver, int level) {
        for (Column col : resolver.getColumns()) {
            String n = col.getName();
            if (database.equalsIdentifiers(columnName, n)) {
                mapColumn(resolver, col, level);
                return;
            }
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean value) {

    }

    private void mapColumn(ColumnResolver resolver, Column col, int level) {
        if (this.columnResolver == null) {
            queryLevel = level;
            column = col;
            this.columnResolver = resolver;
        } else if (queryLevel == level && this.columnResolver != resolver) {
            throw new RuntimeException("mapColumn ERROR");
        }
    }

    @Override
    public Expression optimize(Session session) {
        throw new RuntimeException("ERROR");
//        if (columnResolver == null) {
//            Schema schema = session.getDatabase().findSchema(
//                    tableAlias == null ? session.getCurrentSchemaName() : tableAlias);
//            if (schema != null) {
//                throw new RuntimeException("ERROR");
////                Constant constant = schema.findConstant(columnName);
////                if (constant != null) {
////                    return constant.getValue();
////                }
//            }
//            String name = columnName;
//            if (tableAlias != null) {
//                name = tableAlias + "." + name;
//                if (schemaName != null) {
//                    name = schemaName + "." + name;
//                }
//            }
////            throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, name);
//            throw new RuntimeException("ERROR");
//        }
//        return columnResolver.optimize(this, column);
    }


    @Override
    public Value getValue(Session session) {
        Value value = columnResolver.getValue(column);
        return value;
    }

    @Override
    public int getType() {
        return column.getType();
    }

    public Column getColumn() {
        return column;
    }

}
