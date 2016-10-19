package org.minidb.index;

import org.minidb.table.Row;

/**
 * Created by gxh on 2016/6/13.
 */
public interface Cursor {

    /**
     * Get the complete current row.
     * All column are available.
     *
     * @return the complete row
     */
    Row get();

    /**
     * Get the current row.
     * Only the data for indexed columns is available in this row.
     *
     * @return the search row
     */
    Row getSearchRow();

    /**
     * Skip to the next row if one is available.
     *
     * @return true if another row is available
     */
    boolean next();

    /**
     * Skip to the previous row if one is available.
     * No filtering is made here.
     *
     * @return true if another row is available
     */
    boolean previous();
}
