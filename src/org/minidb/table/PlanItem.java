/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.table;

import org.minidb.index.BaseIndex;

/**
 * The plan item describes the index to be used, and the estimated cost when
 * using it.
 */
public class PlanItem {

    double cost;
    private int[] masks;
    private BaseIndex index;

    void setMasks(int[] masks) {
        this.masks = masks;
    }

    int[] getMasks() {
        return masks;
    }

    void setIndex(BaseIndex index) {
        this.index = index;
    }

    public BaseIndex getIndex() {
        return index;
    }

}
