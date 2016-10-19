/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.result;

import org.minidb.value.Value;

public interface ResultInterface {

    /**
     * Get the current row.
     *
     * @return the row
     */
    Value[] currentRow();

    /**
     * Go to the next row.
     *
     * @return true if a row exists
     */
    boolean next();
}
