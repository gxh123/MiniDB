/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.schema;

import org.minidb.engine.DbObjectBase;

public abstract class SchemaObjectBase extends DbObjectBase implements
        SchemaObject{

    private Schema schema;

    protected void initSchemaObjectBase(Schema newSchema, int id, String name) {
        initDbObjectBase(newSchema.getDatabase(), id, name);
        this.schema = newSchema;
    }

    public Schema getSchema() {
        return schema;
    }

    public String getSQL() {
        return schema.getSQL() + "." + super.getSQL();
    }

    public boolean isHidden() {
        return false;
    }

}
