/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.value;
/**
 * Implementation of NULL. NULL is not a regular data type.
 */
public class ValueNull extends Value {

    public static final ValueNull INSTANCE = new ValueNull();

    @Override
    public int getType() {
        return NULL;
    }

    protected int compareSecure(Value v) {
        throw new RuntimeException("compare null");
    }

    @Override
    public String getSQL() {
        return "NULL";
    }

}
