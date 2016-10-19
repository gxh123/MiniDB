/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.engine;

import org.minidb.command.Parser;

import java.util.ArrayList;

/**
 * The base class for all database objects.
 */
public abstract class DbObjectBase implements DbObject {

    protected Database database;
    private int id;
    private String objectName;

    /**
     * Initialize some attributes of this object.
     *
     * @param db the database
     * @param objectId the object id
     * @param name the name
     */
    protected void initDbObjectBase(Database db, int objectId, String name) {
        this.database = db;
        this.id = objectId;
        this.objectName = name;
    }

    /**
     * Build a SQL statement to re-create this object.
     *
     * @return the SQL statement
     */
    @Override
    public abstract String getCreateSQL();

    protected void setObjectName(String name) {
        objectName = name;
    }

    public String getSQL() {
        return Parser.quoteIdentifier(objectName);
    }

    public Database getDatabase() {
        return database;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public String getName() {
        return objectName;
    }

    @Override
    public String toString() {
        return objectName + ":" + id + ":" + super.toString();
    }

}
