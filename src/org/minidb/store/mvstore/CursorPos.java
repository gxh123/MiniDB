/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.store.mvstore;

/**
 * A position in a cursor
 */
public class CursorPos { //遍历B-Tree时通过它能向上或向右指定节点

    /**
     * The current page.
     */
    public Page page;

    /**
     * The current index.
     */
    public int index;

    /**
     * The position in the parent page, if any.
     */
    public final CursorPos parent;

    public CursorPos(Page page, int index, CursorPos parent) {
        this.page = page;
        this.index = index;
        this.parent = parent;
    }

}

