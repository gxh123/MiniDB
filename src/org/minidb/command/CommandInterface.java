/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.command;

import org.minidb.result.ResultInterface;

import java.util.ArrayList;

/**
 * Represents a SQL statement.
 */
public interface CommandInterface {

    int ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY = 6;

    int CREATE_USER = 32;
    int SELECT = 66;
    int CREATE_INDEX = 25;
    int INSERT = 61;

    boolean isQuery();

    ResultInterface executeQuery();

    int executeUpdate();

}
