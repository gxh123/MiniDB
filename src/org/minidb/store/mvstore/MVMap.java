/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.store.mvstore;

import org.minidb.store.mvstore.type.DataType;
import org.minidb.store.mvstore.type.ObjectDataType;

import java.util.*;

/*
MVMap主要维护了一棵b- tree，对MVMap插入键值对实际上就是对这棵b- tree进行插入，MVMap通过将b- tree的根节点作为自己的属性，从而可以访问树的各个节点，进行操作
 */
public class MVMap<K, V> {

    public static final int PAGE_SIZE = 16 * 1024;
//public static final int PAGE_SIZE = 1000;
    /**
     * The store.
     */
    protected MVStore store;
    protected volatile Page root;
    protected volatile long writeVersion;   //受store的currentVersion控制，每次store commit，currentVersion都会加1，同时会更新所有map的writeVersion，会控制要修改的page的version
    private int id;
    private long createVersion;             //创建这个map时的store的currentVersion
    private final DataType keyType;
    private final DataType valueType;

    private ArrayList<Page> oldRoots =
            new ArrayList<Page>();

    protected MVMap(DataType keyType, DataType valueType) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.root = Page.createEmpty(this,  -1);
    }

    protected void init(MVStore store, HashMap<String, Object> config) {
        this.store = store;
        this.id = DataUtil.readHexInt(config, "id", 0);
        this.createVersion = DataUtil.readHexLong(config, "createVersion", 0);
        this.writeVersion = store.getCurrentVersion();
    }

    //插入操作，先看root是否需要分裂，需要就分裂，不需要就 根据key二分查找往下走，再判断要不要分裂，不断重复直到叶子节点
    //由于root可能因为自身的分裂发生改变，newRoot函数更新root
    public synchronized V put(K key, V value) {
        long v = writeVersion;
        Page p = root.copy(v);
        p = splitRootIfNeeded(p, v);
        Object result = put(p, v, key, value);
        newRoot(p);
        return (V) result;
    }

    //通过memory属性判断是否需要分裂
    protected Page splitRootIfNeeded(Page p, long writeVersion) {
        if (p.getMemory() <= PAGE_SIZE || p.getKeyCount() <= 1) {    //大于4k则分裂
//            System.out.println("p.getMemory(): " + p.getMemory());
            return p;
        }
//        System.out.println("splitRoot");
        int at = p.getKeyCount() / 2;
        long totalCount = p.getTotalCount();
        Object k = p.getKey(at);
        Page split = p.split(at); //分裂后返回的右节点，左节点为原来的page p
        Object[] keys = { k };
        Page.PageReference[] children = {
                new Page.PageReference(p, p.getPos() ,p.getTotalCount()),
                new Page.PageReference(split, split.getPos() , split.getTotalCount()),
        };
        p = Page.create(this, writeVersion,   //构造一个新的root page，并将之前分裂产生的左右节点作为子节点
                keys, null,
                children,
                totalCount, 0);
        return p;
    }

    //二分查找往下走到叶子节点，中间遇到需要分裂的则分裂，在子节点更新（原来有这个key）或者插入（原来没有这个key）新的value
    protected Object put(Page p, long writeVersion, Object key, Object value) {
        int index = p.binarySearch(key);
        if (p.isLeaf()) {
            if (index < 0) {
                index = -index - 1;
                p.insertLeaf(index, key, value);
                return null;
            }
            return p.setValue(index, value);
        }
        // p is a node
        if (index < 0) {
            index = -index - 1;
        } else {
            index++; //大于等于split key的在右边节点，所以要加1
        }
        Page c = p.getChildPage(index).copy(writeVersion);
        //如果在这里发生split，可能是树叶也可能是非树叶节点
        if (c.getMemory() > PAGE_SIZE && c.getKeyCount() > 1) {
            // split on the way down
            int at = c.getKeyCount() / 2;
            Object k = c.getKey(at);
            Page split = c.split(at);
            p.setChild(index, split); //这里把右边节点替换原来的
            p.insertNode(index, k, c); //这里把左边节点插入，同时在keys数组中加入新的k
            // now we are not sure where to add
            return put(p, writeVersion, key, value);
        }
        Object result = put(c, writeVersion, key, value);
        p.setChild(index, c);
        return result;
    }

    //更新root，同时将旧版本的root放入oldRoots
    protected void newRoot(Page newRoot) {
        if (root != newRoot) {
            removeUnusedOldVersions();
            if (root.getVersion() != newRoot.getVersion()) {
                Page last = oldRoots.size() == 0 ? null : oldRoots.get(oldRoots.size() - 1);
                if (last == null || last.getVersion() != root.getVersion()) {
                    oldRoots.add(root);
                }
            }
            root = newRoot;
        }
    }

    public V get(Object key) {
        return (V) binarySearch(root, key);
    }

    //采用二分查找不断往下到叶子节点，查找到value
    protected Object binarySearch(Page p, Object key) {
        int x = p.binarySearch(key);
        if (!p.isLeaf()) {
            if (x < 0) {
                x = -x - 1;
            } else {
                x++;
            }
            p = p.getChildPage(x);
            return binarySearch(p, key);
        }
        if (x >= 0) {
            return p.getValue(x);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        V result = get(key);
        if (result == null) {
            return null;
        }
        long v = writeVersion;
        synchronized (this) {
            Page p = root.copy(v);
            result = (V) remove(p, v, key);
            if (!p.isLeaf() && p.getTotalCount() == 0) {
                p = Page.createEmpty(this,  p.getVersion());
            }
            newRoot(p);
        }
        return result;
    }

    //这个remove再看看！！
    protected Object remove(Page p, long writeVersion, Object key) {
        int index = p.binarySearch(key);
        Object result = null;
        if (p.isLeaf()) {         //如果是叶子节点，就删掉key value
            if (index >= 0) {
                result = p.getValue(index);
                p.remove(index);
            }
            return result;
        }
        // node
        if (index < 0) {
            index = -index - 1;
        } else {
            index++;
        }
        Page cOld = p.getChildPage(index);
        Page c = cOld.copy(writeVersion);
        result = remove(c, writeVersion, key);    //如果不是叶子节点，就获取子节点，递归调用remove
        if (result == null || c.getTotalCount() != 0) {
            // no change, or
            // there are more nodes
            p.setChild(index, c);
        } else {
            // this child was deleted
            if (p.getKeyCount() == 0) {
                p.setChild(index, c);
//                c.removePage(); //直接删除最后一个子节点，父节点在remove(Object)那里删除
            } else {
                p.remove(index); //删除没有记录的子节点
            }
        }
        return result;
    }

    /*---------------------------------------------------------------------------------------------------------------*/
    //rollback就是从oldRoots里找到所要版本的root，版本更加新的那些全部删掉
    //H2 rollbackTo主要是对内存数据库起作用，会保留5个老版本，对于使用硬盘存储的时候不会保留，只要这个版本在磁盘上存了，oldRoots里面就会删掉
    void rollbackTo(long version) {
        if (root.getVersion() >= version) {
            while (true) {
                Page last = oldRoots.size() == 0 ? null : oldRoots.get(oldRoots.size()-1); //不断从oldRoots里找，不是就删，直到小于所要的版本的root
                if (last == null) {
                    break;
                }
                oldRoots.remove(last);
                root = last;
                if (root.getVersion() < version) {
                    break;
                }
            }
        }
    }

    public MVMap<K, V> openVersion(long version) {
        Page newest = null;
        // need to copy because it can change
        Page r = root;
        if (version >= r.getVersion() &&                  //这些判断条件很重要！！！
                (version == writeVersion ||
                r.getVersion() >= 0 ||
                version <= createVersion||
                store.getFileStore() == null)) {
            newest = r;
        } else {
            Page last = oldRoots.size() == 0 ? null : oldRoots.get(0);
            if (last == null || version < last.getVersion()) {
                // smaller than all in-memory versions
                return store.openMapVersion(version, id, this);
            }
            Iterator<Page> it = oldRoots.iterator();
            while (it.hasNext()) {                     //找到小于等于所要版本的root
                Page p = it.next();
                if (p.getVersion() > version) {
                    break;
                }
                last = p;
            }
            newest = last;
        }
        MVMap<K, V> m = openReadOnly();             //new一个新的map，设置为只读，这里暂时去掉，没有设置
        m.root = newest;
        return m;
    }

    MVMap<K, V> openReadOnly() {
        MVMap<K, V> m = new MVMap<K, V>(keyType, valueType);
        HashMap<String, Object> config = new HashMap();
        config.put("id", id);
        config.put("createVersion", createVersion);
        m.init(store ,config);
        m.root = root;
        return m;
    }

    public synchronized void clear() {
        newRoot(Page.createEmpty(this, writeVersion));
    }

    String asString(String name) {
        StringBuilder buff = new StringBuilder();
        if (name != null) {
            DataUtil.appendMap(buff, "name", name);
        }
        if (createVersion != 0) {
            DataUtil.appendMap(buff, "createVersion", createVersion);
        }
        String type = getType();
        if (type != null) {
            DataUtil.appendMap(buff, "type", type);
        }
        return buff.toString();
    }

    long getOldestVersionToKeep() {
        long v = store.currentVersion;
        if (store.fileStore == null) {
            return v - store.versionsToKeep;
        }
//        long storeVersion = store.currentStoreVersion;
//        if (storeVersion > -1) {
//            v = Math.min(v, storeVersion);
//        }
        return v;
    }

    //主要是对内存数据库起作用，会保留5个老版本，对于使用硬盘存储的时候不会保留，只要这个版本在磁盘上存了，oldRoots里面就会删掉
    void removeUnusedOldVersions() {
        long oldest = getOldestVersionToKeep();
        if (oldest == -1) {
            return;
        }
        Page last = oldRoots.size() == 0 ? null : oldRoots.get(oldRoots.size() - 1); //后面的是最近加入的
        if(last == null) return;
        while (true) {
            Page p = oldRoots.get(0);
            if (p == null || p.getVersion() >= oldest || p == last) {
                break;
            }
            oldRoots.remove(p);
        }
    }

    void setRootPos(long rootPos, long version) {
        root = rootPos == 0 ? Page.createEmpty(this, -1) : readPage(rootPos);
        root.setVersion(version);
    }

    Page readPage(long pos) {
        return store.readPage(this, pos);
    }



    /*---------------------------------------------------------------------------------------------------------------*/

    void setWriteVersion(long writeVersion) {
        this.writeVersion = writeVersion;
    }

    int compare(Object a, Object b) {
        return keyType.compare(a, b);
    }

    public DataType getKeyType() {
        return keyType;
    }

    public DataType getValueType() {
        return valueType;
    }

    public int hashCode() {
        return id;
    }

    public boolean equals(Object o) {
        return this == o;
    }

    public int getId() {
        return id;
    }

    public long getCreateVersion() {
        return createVersion;
    }

    public String getType() {
        return null;
    }

    static String getMapRootKey(int mapId) {
        return "root." + Integer.toHexString(mapId);
    }

    protected int getChildPageCount(Page p) {
        return p.getRawChildPageCount();
    }

    protected void removePage(long pos, int memory) {
        store.removePage(this, pos, memory);
    }

    static String getMapKey(int mapId) {
        return "map." + Integer.toHexString(mapId);
    }

    public long getVersion() {
        return root.getVersion();
    }

    public Page getRoot() {
        return root;
    }

    public Iterator<K> keyIterator(K from) {
        return new Cursor<K, V>(this, root, from);
    }

    public String getName() {
        return store.getMapName(id);
    }

    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    public int size() {
        long size = sizeAsLong();
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
    }

    /**
     * Get the number of entries, as a long.
     *
     * @return the number of entries
     */
    public long sizeAsLong() {
        return root.getTotalCount();
    }

    public boolean isEmpty() {
        // could also use (sizeAsLong() == 0)
        return root.isLeaf() && root.getKeyCount() == 0;
    }

    public Cursor<K, V> cursor(K from) {
        return new Cursor<K, V>(this, root, from);
    }

    public synchronized V putIfAbsent(K key, V value) {
        V old = get(key);
        if (old == null) {
            put(key, value);
        }
        return old;
    }

    public synchronized boolean replace(K key, V oldValue, V newValue) {
        V old = get(key);
        if (areValuesEqual(old, oldValue)) {
            put(key, newValue);
            return true;
        }
        return false;
    }

    public boolean areValuesEqual(Object a, Object b) {
        if (a == b) {
            return true;
        } else if (a == null || b == null) {
            return false;
        }
        return valueType.compare(a, b) == 0;
    }

    public K lastKey() {
        return getFirstLast(false);
    }

    protected K getFirstLast(boolean first) {
        if (size() == 0) {
            return null;
        }
        Page p = root;
        while (true) {
            if (p.isLeaf()) {
                return (K) p.getKey(first ? 0 : p.getKeyCount() - 1);
            }
            p = p.getChildPage(first ? 0 : getChildPageCount(p) - 1);
        }
    }

    public K lowerKey(K key) {
        return getMinMax(key, true, true);
    }

    public Set<Map.Entry<K, V>> entrySet() {
        final MVMap<K, V> map = this;
        final Page root = this.root;
        return new AbstractSet<Map.Entry<K, V>>() {

            @Override
            public Iterator<Map.Entry<K, V>> iterator() {
                final Cursor<K, V> cursor = new Cursor<K, V>(map, root, null);
                return new Iterator<Map.Entry<K, V>>() {

                    @Override
                    public boolean hasNext() {
                        return cursor.hasNext();
                    }

                    @Override
                    public Map.Entry<K, V> next() {
                        K k = cursor.next();
                        return new DataUtil.MapEntry<K, V>(k, cursor.getValue());
                    }

                    @Override
                    public void remove() {
                        throw new RuntimeException("Removing is not supported");
                    }
                };

            }

            @Override
            public int size() {
                return MVMap.this.size();
            }

            @Override
            public boolean contains(Object o) {
                return MVMap.this.containsKey(o);
            }

        };

    }

    /**
     * Get the smallest or largest key using the given bounds.
     *
     * @param key the key
     * @param min whether to retrieve the smallest key
     * @param excluding if the given upper/lower bound is exclusive
     * @return the key, or null if no such key exists
     */
    protected K getMinMax(K key, boolean min, boolean excluding) {
        return getMinMax(root, key, min, excluding);
    }

    @SuppressWarnings("unchecked")
    private K getMinMax(Page p, K key, boolean min, boolean excluding) {
        if (p.isLeaf()) {
            int x = p.binarySearch(key);
            if (x < 0) {
                x = -x - (min ? 2 : 1);
            } else if (excluding) {
                x += min ? -1 : 1;
            }
            if (x < 0 || x >= p.getKeyCount()) {
                return null;
            }
            return (K) p.getKey(x);
        }
        int x = p.binarySearch(key);
        if (x < 0) {
            x = -x - 1;
        } else {
            x++;
        }
        while (true) {
            if (x < 0 || x >= getChildPageCount(p)) {
                return null;
            }
            K k = getMinMax(p.getChildPage(x), key, min, excluding);
            if (k != null) {
                return k;
            }
            x += min ? -1 : 1;
        }
    }

    /*---------------------------------------------------------------------------------------------------------------*/

    public interface MapBuilder<M extends MVMap<K, V>, K, V> {
        M create();
    }


    public static class Builder<K, V> implements MapBuilder<MVMap<K, V>, K, V> {

        protected DataType keyType;
        protected DataType valueType;

        /**
         * Create a new builder with the default key and value data types.
         */
        public Builder() {
            // ignore
        }

        /**
         * Set the key data type.
         *
         * @param keyType the key type
         * @return this
         */
        public Builder<K, V> keyType(DataType keyType) {
            this.keyType = keyType;
            return this;
        }

        public DataType getKeyType() {
            return keyType;
        }

        public DataType getValueType() {
            return valueType;
        }

        /**
         * Set the value data type.
         *
         * @param valueType the value type
         * @return this
         */
        public Builder<K, V> valueType(DataType valueType) {
            this.valueType = valueType;
            return this;
        }

        public MVMap<K, V> create() {
            if (keyType == null) {
                keyType = new ObjectDataType();
            }
            if (valueType == null) {
                valueType = new ObjectDataType();
            }
            return new MVMap<K, V>(keyType, valueType);
        }

    }
}
