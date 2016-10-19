/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.store.mvstore;

import java.util.Iterator;

/**
 * A cursor to iterate over elements in ascending order.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class Cursor<K, V> implements Iterator<K> { //深度优先，然后到右边的同节点中的记录，接着过渡到父节点，父节点的右节点...

    private final MVMap<K, ?> map;
    private final K from;
    private CursorPos pos;
    private K current, last;
    private V currentValue, lastValue;
    private Page lastPage;
    private final Page root;
    private boolean initialized;

    Cursor(MVMap<K, ?> map, Page root, K from) {
        this.map = map;
        this.root = root;
        this.from = from;
    }

    @Override
    public boolean hasNext() {
        if (!initialized) {
            min(root, from);
            initialized = true;
            fetchNext();
        }
        return current != null;
    }

    @Override
    public K next() {
        hasNext();
        K c = current;
        last = current;
        lastValue = currentValue;
        lastPage = pos == null ? null : pos.page;
        fetchNext();
        return c;
    }

    /**
     * Get the last read key if there was one.
     *
     * @return the key or null
     */
    public K getKey() {
        return last;
    }

    /**
     * Get the last read value if there was one.
     *
     * @return the value or null
     */
    public V getValue() {
        return lastValue;
    }

    Page getPage() {
        return lastPage;
    }

    @Override
    public void remove() {
        throw new RuntimeException("Removing is not supported");
    }

    /**
     * Fetch the next entry that is equal or larger than the given key, starting
     * from the given page. This method retains the stack.
     *
     * @param p the page to start
     * @param from the key to search
     */
    private void min(Page p, K from) {
        while (true) {
            if (p.isLeaf()) {
                int x = from == null ? 0 : p.binarySearch(from);
                if (x < 0) {
                    x = -x - 1;
                }
                pos = new CursorPos(p, x, pos);
                break;
            }
            int x = from == null ? -1 : p.binarySearch(from);
            if (x < 0) {
                x = -x - 1;
            } else {
                x++;
            }
            //遍历完当前leaf page后，就转到parent page，然后就到右边的第一个兄弟page，
            //所以要x+1
            pos = new CursorPos(p, x + 1, pos);
            p = p.getChildPage(x);
        }
    }

    /**
     * Fetch the next entry if there is one.
     */
    @SuppressWarnings("unchecked")
    private void fetchNext() {
        while (pos != null) {
            if (pos.index < pos.page.getKeyCount()) {
                int index = pos.index++;
                current = (K) pos.page.getKey(index);
                currentValue = (V) pos.page.getValue(index);
                return;
            }
            pos = pos.parent;
            if (pos == null) {
                break;
            }
            if (pos.index < map.getChildPageCount(pos.page)) {
                min(pos.page.getChildPage(pos.index++), null);
            }
        }
        current = null;
    }

}
