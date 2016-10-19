/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.store.mvstore;

import org.minidb.store.mvstore.type.DataType;
import org.minidb.store.mvstore.type.ObjectDataType;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A store that supports concurrent MVCC read-committed transactions.
 */
public class TransactionStore {

    /**
     * The store.
     */
    final MVStore store;
    /**
     * The undo log.
     * <p>
     * Log entries are written before the data is changed
     * (write-ahead).
     * <p>
     * Key: opId, value: [ mapId, key, oldValue ].
     */
    final MVMap<Long, Object[]> undoLog;

    /**
     * The map of maps.
     */
    private HashMap<Integer, MVMap<Object, VersionedValue>> maps = new HashMap();

    private final DataType dataType;

    private final BitSet openTransactions = new BitSet();

    private int nextTempMapId;

    /**
     * Create a new transaction store.
     *
     * @param store the store
     */
    public TransactionStore(MVStore store) {
        this(store, new ObjectDataType());
    }

    /**
     * Create a new transaction store.
     *
     * @param store the store
     * @param dataType the data type for map keys and values
     */
    public TransactionStore(MVStore store, DataType dataType) {
        this.store = store;
        this.dataType = dataType;
        VersionedValueType oldValueType = new VersionedValueType(dataType);
        ArrayType undoLogValueType = new ArrayType(new DataType[]{
                new ObjectDataType(), dataType, oldValueType
        });
        MVMap.Builder<Long, Object[]> builder =
                new MVMap.Builder<Long, Object[]>().
                valueType(undoLogValueType);
        undoLog = store.openMap("undoLog", builder);
        //TransactionStore 主要维护了一个 undoLog的 MVMap
        //undoLog的Key: opId, value: [ mapId, key, oldValue ].
        //opId为transactionId和logId共同构成，通过opId就能确定是哪一个transaction，是哪一个修改（通过logid）
        //存放到MVMap里的数据的键值对的值的类型VersionedValue都含有属性operationId（即opId），不为0表示还未提交，为0表示已提交
        //这里的提交，未提交是指是否已经调用commit方法，将undoLog里面对应的项去掉，同时将map里的值更新为一个operationId的值，然后会调用MVStore里的commit方法
    }

    /**
     * Combine the transaction id and the log id to an operation id.
     *
     * @param transactionId the transaction id
     * @param logId the log id
     * @return the operation id
     */
    static long getOperationId(int transactionId, long logId) { //transactionId占前面24位，logId占后面40位
        return ((long) transactionId << 40) | logId;
    }

    /**
     * Get the transaction id for the given operation id.
     *
     * @param operationId the operation id
     * @return the transaction id
     */
    static int getTransactionId(long operationId) {
        return (int) (operationId >>> 40);
    }

    /**
     * Get the log id for the given operation id.
     *
     * @param operationId the operation id
     * @return the log id
     */
    static long getLogId(long operationId) {
        return operationId & ((1L << 40) - 1);
    }

    /**
     * Close the transaction store.
     */
    public synchronized void close() {
        store.commit();
    }

    /**
     * Begin a new transaction.
     *
     * @return the transaction
     */
    public synchronized Transaction begin() {
        int transactionId;
        int status;
        transactionId = openTransactions.nextClearBit(1);
        openTransactions.set(transactionId);
        status = Transaction.STATUS_OPEN;
        return new Transaction(this, transactionId, status, null, 0);
    }

    /**
     * Log an entry.
     *
     * @param t the transaction
     * @param logId the log id
     * @param mapId the map id
     * @param key the key
     * @param oldValue the old value
     */
    void log(Transaction t, long logId, int mapId,
            Object key, Object oldValue) {
        Long undoKey = getOperationId(t.getId(), logId);
        Object[] log = new Object[] { mapId, key, oldValue };
        synchronized (undoLog) {
            undoLog.put(undoKey, log);
        }
    }

    /**
     * Remove a log entry.
     *
     * @param t the transaction
     * @param logId the log id
     */
    public void logUndo(Transaction t, long logId) {
        Long undoKey = getOperationId(t.getId(), logId);
        synchronized (undoLog) {
            Object[] old = undoLog.remove(undoKey);
        }
    }

    /**
     * Remove the given map.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param map the map
     */
    synchronized <K, V> void removeMap(TransactionMap<K, V> map) {
        maps.remove(map.mapId);
        store.removeMap(map.map);
    }

    /**
     * Commit a transaction.
     *
     * @param t the transaction
     * @param maxLogId the last log id
     */
    void commit(Transaction t, long maxLogId) {
        synchronized (undoLog) {
            t.setStatus(Transaction.STATUS_COMMITTING);
            for (long logId = 0; logId < maxLogId; logId++) {
                Long undoKey = getOperationId(t.getId(), logId);
                Object[] op = undoLog.get(undoKey);
                int mapId = (Integer) op[0];
                MVMap<Object, VersionedValue> map = openMap(mapId);
                if (map == null) {
                    // map was later removed
                } else {
                    Object key = op[1];
                    VersionedValue value = map.get(key);
                    if (value == null) {
                        // nothing to do
                    } else if (value.value == null) {
                        // remove the value
                        map.remove(key);
                    } else {
                        VersionedValue v2 = new VersionedValue();
                        v2.value = value.value;
                        map.put(key, v2);
                    }
                }
                undoLog.remove(undoKey);
            }
        }
        endTransaction(t);
    }

    /**
     * Open the map with the given name.
     *
     * @param <K> the key type
     * @param name the map name
     * @param keyType the key type
     * @param valueType the value type
     * @return the map
     */
    synchronized <K> MVMap<K, VersionedValue> openMap(String name,
                                                      DataType keyType, DataType valueType) {
        if (keyType == null) {
            keyType = new ObjectDataType();
        }
        if (valueType == null) {
            valueType = new ObjectDataType();
        }
        VersionedValueType vt = new VersionedValueType(valueType);
        MVMap<K, VersionedValue> map;
        MVMap.Builder<K, VersionedValue> builder =
                new MVMap.Builder<K, VersionedValue>().
                keyType(keyType).valueType(vt);
        map = store.openMap(name, builder);
        @SuppressWarnings("unchecked")
        MVMap<Object, VersionedValue> m = (MVMap<Object, VersionedValue>) map;
        maps.put(map.getId(), m);
        return map;
    }

    /**
     * Open the map with the given id.
     *
     * @param mapId the id
     * @return the map
     */
    synchronized MVMap<Object, VersionedValue> openMap(int mapId) {
        MVMap<Object, VersionedValue> map = maps.get(mapId);
        if (map != null) {
            return map;
        }
        String mapName = store.getMapName(mapId);
        if (mapName == null) {
            // the map was removed later on
            return null;
        }
        VersionedValueType vt = new VersionedValueType(dataType);
        MVMap.Builder<Object, VersionedValue> mapBuilder =
                new MVMap.Builder<Object, VersionedValue>().
                keyType(dataType).valueType(vt);
        map = store.openMap(mapName, mapBuilder);
        maps.put(mapId, map);
        return map;
    }

    /**
     * End this transaction
     *
     * @param t the transaction
     */
    synchronized void endTransaction(Transaction t) {
        t.setStatus(Transaction.STATUS_CLOSED);
        openTransactions.clear(t.transactionId);
        store.commit();
    }

    /**
     * Rollback to an old savepoint.
     *
     * @param t the transaction
     * @param maxLogId the last log id
     * @param toLogId the log id to roll back to
     */
    //rollbackTo就是将这个Transaction的想要会到的LogId之后所有的logId的影响都去掉，包括去掉undoLog里面的项，在map里删掉或者放入原先的值
    void rollbackTo(Transaction t, long maxLogId, long toLogId) {
        synchronized (undoLog) {
            for (long logId = maxLogId - 1; logId >= toLogId; logId--) {
                Long undoKey = getOperationId(t.getId(), logId);
                Object[] op = undoLog.get(undoKey);
                int mapId = ((Integer) op[0]).intValue();
                MVMap<Object, VersionedValue> map = openMap(mapId);
                if (map != null) {
                    Object key = op[1];
                    VersionedValue oldValue = (VersionedValue) op[2];
                    if (oldValue == null) {
                        // this transaction added the value
                        map.remove(key);
                    } else {
                        // this transaction updated the value
                        map.put(key, oldValue);
                    }
                }
                undoLog.remove(undoKey);
            }
        }
    }

    /**
     * Create a temporary map. Such maps are removed when opening the store.
     *
     * @return the map
     */
    synchronized MVMap<Object, Integer> createTempMap() {
        String mapName = "temp." + nextTempMapId++;
        return openTempMap(mapName);
    }

    /**
     * Open a temporary map.
     *
     * @param mapName the map name
     * @return the map
     */
    MVMap<Object, Integer> openTempMap(String mapName) {
        MVMap.Builder<Object, Integer> mapBuilder =
                new MVMap.Builder<Object, Integer>().
                        keyType(dataType);
        return store.openMap(mapName, mapBuilder);
    }

    /**
     * A transaction.
     */
    //Transaction主要就是调用store.log记录更新或者删除的变化，维护logId, 每次更新或者删除成功就增加。
    public static class Transaction {

        /**
         * The status of a closed transaction (committed or rolled back).
         */
        public static final int STATUS_CLOSED = 0;

        /**
         * The status of an open transaction.
         */
        public static final int STATUS_OPEN = 1;

        /**
         * The status of a prepared transaction.
         */
        public static final int STATUS_PREPARED = 2;

        /**
         * The status of a transaction that is being committed, but possibly not
         * yet finished. A transactions can go into this state when the store is
         * closed while the transaction is committing. When opening a store,
         * such transactions should be committed.
         */
        public static final int STATUS_COMMITTING = 3;

        /**
         * The transaction store.
         */
        final TransactionStore store;

        /**
         * The transaction id.
         */
        final int transactionId;

        /**
         * The log id of the last entry in the undo log map.
         */
        long logId;

        private int status;

        private String name;

        Transaction(TransactionStore store, int transactionId, int status,
                String name, long logId) {
            this.store = store;
            this.transactionId = transactionId;
            this.status = status;
            this.name = name;
            this.logId = logId;
        }

        public int getId() {
            return transactionId;
        }

        public int getStatus() {
            return status;
        }

        void setStatus(int status) {
            this.status = status;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        /**
         * Create a new savepoint.
         *
         * @return the savepoint id
         */
        public long setSavepoint() {
            return logId;
        }

        /**
         * Add a log entry.
         *
         * @param mapId the map id
         * @param key the key
         * @param oldValue the old value
         */
        void log(int mapId, Object key, Object oldValue) {
            store.log(this, logId, mapId, key, oldValue);
            // only increment the log id if logging was successful
            logId++;
        }

        /**
         * Remove the last log entry.
         */
        void logUndo() {
            store.logUndo(this, --logId);
        }

        /**
         * Open a data map.
         *
         * @param <K> the key type
         * @param <V> the value type
         * @param name the name of the map
         * @return the transaction map
         */
        public <K, V> TransactionMap<K, V> openMap(String name) {
            return openMap(name, null, null);
        }

        /**
         * Open the map to store the data.
         *
         * @param <K> the key type
         * @param <V> the value type
         * @param name the name of the map
         * @param keyType the key data type
         * @param valueType the value data type
         * @return the transaction map
         */
        public <K, V> TransactionMap<K, V> openMap(String name,
                                                   DataType keyType, DataType valueType) {
            MVMap<K, VersionedValue> map = store.openMap(name, keyType,
                    valueType);
            int mapId = map.getId();
//            System.out.println("name: " + name + "mapId:" + mapId);
            return new TransactionMap<K, V>(this, map, mapId);
        }

        /**
         * Open the transactional version of the given map.
         *
         * @param <K> the key type
         * @param <V> the value type
         * @param map the base map
         * @return the transactional map
         */
        public <K, V> TransactionMap<K, V> openMap(
                MVMap<K, VersionedValue> map) {
            int mapId = map.getId();
            return new TransactionMap<K, V>(this, map, mapId);
        }

        /**
         * Prepare the transaction. Afterwards, the transaction can only be
         * committed or rolled back.
         */
        public void prepare() {
            status = STATUS_PREPARED;
        }

        /**
         * Commit the transaction. Afterwards, this transaction is closed.
         */
        public void commit() {
            store.commit(this, logId);
        }

        /**
         * Roll back to the given savepoint. This is only allowed if the
         * transaction is open.
         *
         * @param savepointId the savepoint id
         */
        public void rollbackToSavepoint(long savepointId) {
            store.rollbackTo(this, logId, savepointId);
            logId = savepointId;
        }

        /**
         * Roll the transaction back. Afterwards, this transaction is closed.
         */
        public void rollback() {
            store.rollbackTo(this, logId, 0);
            store.endTransaction(this);
        }

        /**
         * Remove the map.
         *
         * @param map the map
         */
        public <K, V> void removeMap(TransactionMap<K, V> map) {
            store.removeMap(map);
        }

        @Override
        public String toString() {
            return "" + transactionId;
        }

    }

    /**
     * A map that supports transactions.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    //TransactionMap本质是在MVMap的基础上加了一些操作使之能支持： 读已提交，不允许脏写 这些功能

    public static class TransactionMap<K, V> {

        /**
         * The map id.
         */
        final int mapId;

        /**
         * If a record was read that was updated by this transaction, and the
         * update occurred before this log id, the older version is read. This
         * is so that changes are not immediately visible, to support statement
         * processing (for example "update test set id = id + 1").
         */
        long readLogId = Long.MAX_VALUE;

        /**
         * The map used for writing (the latest version).
         * <p>
         * Key: key the key of the data.
         * Value: { transactionId, oldVersion, value }
         */
        final MVMap<K, VersionedValue> map;

        private Transaction transaction;

        TransactionMap(Transaction transaction, MVMap<K, VersionedValue> map,
                int mapId) {
            this.transaction = transaction;
            this.map = map;
            this.mapId = mapId;
        }

        /**
         * Set the savepoint. Afterwards, reads are based on the specified
         * savepoint.
         *
         * @param savepoint the savepoint
         */
        public void setSavepoint(long savepoint) {
            this.readLogId = savepoint;
        }

        /**
         * Get a clone of this map for the given transaction.
         *
         * @param transaction the transaction
         * @param savepoint the savepoint
         * @return the map
         */
        public TransactionMap<K, V> getInstance(Transaction transaction,
                long savepoint) {
            TransactionMap<K, V> m =
                    new TransactionMap<K, V>(transaction, map, mapId);
            m.setSavepoint(savepoint);
            return m;
        }

        public long sizeAsLongMax() {
            return map.sizeAsLong();
        }

        /**
         * Get the size of the map as seen by this transaction.
         *
         * @return the size
         */
        public long sizeAsLong() {
//            throw new RuntimeException("sizeAsLong 没写");
            long sizeRaw = map.sizeAsLong();
            MVMap<Long, Object[]> undo = transaction.store.undoLog;
            long undoLogSize;
            synchronized (undo) {
                undoLogSize = undo.sizeAsLong();
            }
            if (undoLogSize == 0) {
                return sizeRaw;
            }
            if (undoLogSize > sizeRaw) {
                // the undo log is larger than the map -
                // count the entries of the map
                long size = 0;
                Cursor<K, VersionedValue> cursor = map.cursor(null);
                while (cursor.hasNext()) {
                    VersionedValue data;
                    synchronized (transaction.store.undoLog) {
                        K key = cursor.next();
                        data = getValue(key, readLogId, cursor.getValue());
                    }
                    if (data != null && data.value != null) {
                        size++;
                    }
                }
                return size;
            }
            // the undo log is smaller than the map -
            // scan the undo log and subtract invisible entries
            synchronized (undo) {
                // re-fetch in case any transaction was committed now
                long size = map.sizeAsLong();
                MVMap<Object, Integer> temp = transaction.store.createTempMap();
                try {
                    for (Map.Entry<Long, Object[]> e : undo.entrySet()) {
                        Object[] op = e.getValue();
                        int m = (Integer) op[0];
                        if (m != mapId) {
                            // a different map - ignore
                            continue;
                        }
                        @SuppressWarnings("unchecked")
                        K key = (K) op[1];
                        if (get(key) == null) {
                            Integer old = temp.put(key, 1);
                            // count each key only once (there might be multiple
                            // changes for the same key)
                            if (old == null) {
                                size--;
                            }
                        }
                    }
                } finally {
                    transaction.store.store.removeMap(temp);
                }
                return size;
            }
        }

        /**
         * Remove an entry.
         * <p>
         * If the row is locked, this method will retry until the row could be
         * updated or until a lock timeout.
         *
         * @param key the key
         * @throws IllegalStateException if a lock timeout occurs
         */
        public V remove(K key) {
            return set(key, null);
        }

        /**
         * Update the value for the given key.
         * <p>
         * If the row is locked, this method will retry until the row could be
         * updated or until a lock timeout.
         *
         * @param key the key
         * @param value the new value (not null)
         * @return the old value
         * @throws IllegalStateException if a lock timeout occurs
         */
        public V put(K key, V value) {
//            DataUtils.checkArgument(value != null, "The value may not be null");
            return set(key, value);
        }

        /**
         * Update the value for the given key, without adding an undo log entry.
         *
         * @param key the key
         * @param value the value
         * @return the old value
         */
        @SuppressWarnings("unchecked")
        public V putCommitted(K key, V value) {
//            DataUtils.checkArgument(value != null, "The value may not be null");
            VersionedValue newValue = new VersionedValue();
            newValue.value = value;
            VersionedValue oldValue = map.put(key, newValue);
            System.out.println("putCommitted:" + "oldValue:" + oldValue + ",newValue:"+ newValue);
            return (V) (oldValue == null ? null : oldValue.value);
        }

        private V set(K key, V value) {
            V old = get(key);
            boolean ok = trySet(key, value, false);
            if (ok) {
                return old;
            }
            throw new RuntimeException("Entry is locked");
        }

        /**
         * Try to remove the value for the given key.
         * <p>
         * This will fail if the row is locked by another transaction (that
         * means, if another open transaction changed the row).
         *
         * @param key the key
         * @return whether the entry could be removed
         */
        public boolean tryRemove(K key) {
            return trySet(key, null, false);
        }

        /**
         * Try to update the value for the given key.
         * <p>
         * This will fail if the row is locked by another transaction (that
         * means, if another open transaction changed the row).
         *
         * @param key the key
         * @param value the new value
         * @return whether the entry could be updated
         */
        public boolean tryPut(K key, V value) {
//            DataUtils.checkArgument(value != null, "The value may not be null");
            return trySet(key, value, false);
        }



        /**
         * Try to set or remove the value. When updating only unchanged entries,
         * then the value is only changed if it was not changed after opening
         * the map.
         *
         * @param key the key
         * @param value the new value (null to remove the value)
         * @param onlyIfUnchanged only set the value if it was not changed (by
         *            this or another transaction) since the map was opened
         * @return true if the value was set, false if there was a concurrent
         *         update
         */
        //不允许脏写，只允许提交以后或者是同一个事务的才能修改
        public boolean trySet(K key, V value, boolean onlyIfUnchanged) { //思考时要注意一点: put和remove都会调用它
            VersionedValue current = map.get(key);
            VersionedValue newValue = new VersionedValue();
            newValue.operationId = getOperationId(
                    transaction.transactionId, transaction.logId);
            newValue.value = value;
            if (current == null) {   //如果本来没有值，后来要放的时候发现有值了，就不放了（我感觉也可以放，判断下operationId）
                // a new value
                transaction.log(mapId, key, current);
                VersionedValue old = map.putIfAbsent(key, newValue);  //putIfAbsent 原子操作
                if (old != null) {
                    transaction.logUndo();
                    return false;
                }
                return true;
            }
            long id = current.operationId;
            if (id == 0) {               //如果已经提交了，则这个值对其他事务是可见的，则可以修改它
                // committed             //这里对提交的值进行修改，但是如果提交的值还未写入磁盘，这样修改会不会有问题？
                transaction.log(mapId, key, current);
                // the transaction is committed:
                // overwrite the value
                if (!map.replace(key, current, newValue)) {
                    // somebody else was faster
                    transaction.logUndo();
                    return false;
                }
                return true;
            }
            int tx = getTransactionId(current.operationId);
            if (tx == transaction.transactionId) {          //如果没有提交，但这个值是同一个事务修改的，则可以修改它
                // added or updated by this transaction
                transaction.log(mapId, key, current);
                if (!map.replace(key, current, newValue)) {
                    // strange, somebody overwrote the value
                    // even though the change was not committed
                    transaction.logUndo();
                    return false;
                }
                return true;
            }
            // the transaction is not yet committed
            return false;
        }

        /**
         * Get the value for the given key at the time when this map was opened.
         *
         * @param key the key
         * @return the value or null
         */
        public V get(K key) {
            return get(key, readLogId);
        }

        public K lastKey() {
            K k = map.lastKey();
            while (true) {
                if (k == null) {
                    return null;
                }
                if (get(k) != null) {
                    return k;
                }
                k = map.lowerKey(k);
            }
        }

        /**
         * Whether the map contains the key.
         *
         * @param key the key
         * @return true if the map contains an entry for this key
         */
        public boolean containsKey(K key) {
            return get(key) != null;
        }

        /**
         * Get the value for the given key.
         *
         * @param key the key
         * @param maxLogId the maximum log id
         * @return the value or null
         */
        @SuppressWarnings("unchecked")
        public V get(K key, long maxLogId) {
            VersionedValue data = getValue(key, maxLogId);
            return data == null ? null : (V) data.value;
        }

        private VersionedValue getValue(K key, long maxLog) {
            synchronized (getUndoLog()) {
                VersionedValue data = map.get(key);
                return getValue(key, maxLog, data);
            }
        }

        Object getUndoLog() {
            return transaction.store.undoLog;
        }

        /**
         * Get the versioned value for the given key.
         *
         * @param key the key
         * @param maxLog the maximum log id of the entry
         * @param data the value stored in the main map
         * @return the value
         */
        VersionedValue getValue(K key, long maxLog, VersionedValue data) {
            //基本思路是: data最先是从map中取出的值，如果为null，说明在map中没有了，如果有且operationId是0，说明是已提交的。
            //不满足这两条件，再从undo log中按operationId找
            while (true) {
                if (data == null) {
                    // doesn't exist or deleted by a committed transaction
                    return null;
                }
                long id = data.operationId;
                if (id == 0) {
                    // it is committed
                    return data;              //读已提交
                }
                int tx = getTransactionId(id);
                if (tx == transaction.transactionId) {   //未提交，但是是同一个事务也可以读
                    // added by this transaction
                    if (getLogId(id) < maxLog) {
                        return data;
                    }
                }
                // get the value before the uncommitted transaction
                //如果要获取的值还未提交，则从undoLog去获取老的值，这个老的值已经提交了。
                //如果没有提交，一般情况下undoLog会存在着一项
                Object[] d;
                d = transaction.store.undoLog.get(id);
                if (d == null) {
                    // this entry should be committed or rolled back
                    // in the meantime (the transaction might still be open)
                    // or it might be changed again in a different
                    // transaction (possibly one with the same id)
                    data = map.get(key);
                } else {
                    data = (VersionedValue) d[2];     //读已提交
                }
            }
        }

        /**
         * Iterate over keys.
         *
         * @param from the first key to return
         * @return the iterator
         */
        public Iterator<K> keyIterator(K from) {
            return keyIterator(from, false);
        }

        /**
         * Iterate over keys.
         *
         * @param from the first key to return
         * @param includeUncommitted whether uncommitted entries should be
         *            included
         * @return the iterator
         */
        public Iterator<K> keyIterator(final K from, final boolean includeUncommitted) {
            return new Iterator<K>() {
                private K currentKey = from;
                private Cursor<K, VersionedValue> cursor = map.cursor(currentKey);

                {
                    fetchNext();
                }

                private void fetchNext() {
                    while (cursor.hasNext()) {
                        K k = null;
                        try {
                            k = cursor.next();
                        } catch (IllegalStateException e) {
                            // TODO this is a bit ugly
                            System.out.println("keyIterator error");
                        }
                        currentKey = k;
                        if (includeUncommitted) {
                            return;
                        }
                        if (containsKey(k)) {
                            return;
                        }
                    }
                    currentKey = null;
                }

                @Override
                public boolean hasNext() {
                    return currentKey != null;
                }

                @Override
                public K next() {
                    K result = currentKey;
                    fetchNext();
                    return result;
                }

                @Override
                public void remove() {
                    System.out.println("Removing is not supported");
                }
            };
        }

        public Iterator<Map.Entry<K, V>> entryIterator(final K from) {
            return new Iterator<Map.Entry<K, V>>() {
                private Map.Entry<K, V> current;
                private K currentKey = from;
                private Cursor<K, VersionedValue> cursor = map.cursor(currentKey);

                {
                    fetchNext();
                }

                private void fetchNext() {
                    while (cursor.hasNext()) {
                        synchronized (getUndoLog()) {
                            K k;
                            try {
                                k = cursor.next();
                            } catch (IllegalStateException e) {
                                throw new RuntimeException("fetchNext ERROR");
                            }
                            final K key = k;
                            VersionedValue data = cursor.getValue();
                            data = getValue(key, readLogId, data);
                            if (data != null && data.value != null) {
                                @SuppressWarnings("unchecked")
                                final V value = (V) data.value;
                                current = new DataUtil.MapEntry<K, V>(key, value);
                                currentKey = key;
                                return;
                            }
                        }
                    }
                    current = null;
                    currentKey = null;
                }

                @Override
                public boolean hasNext() {
                    return current != null;
                }

                @Override
                public Map.Entry<K, V> next() {
                    Map.Entry<K, V> result = current;
                    fetchNext();
                    return result;
                }

                @Override
                public void remove() {
                    throw new RuntimeException("Removing is not supported");
                }
            };

        }
    }

    /**
     * A versioned value (possibly null). It contains a pointer to the old
     * value, and the value itself.
     */
    static class VersionedValue {

        /**
         * The operation id.
         */
        public long operationId;

        /**
         * The value.
         */
        public Object value;

        @Override
        public String toString() {
            return value + (operationId == 0 ? "" : (
                    " " + "TransactionId: " +
                    getTransactionId(operationId) + "," + "LogId: "+
                    getLogId(operationId)));
        }

    }

    /**
     * The value type for a versioned value.
     */
    public static class VersionedValueType implements DataType {

        private final DataType valueType;

        VersionedValueType(DataType valueType) {
            this.valueType = valueType;
        }

        @Override
        public int getMemory(Object obj) {
            VersionedValue v = (VersionedValue) obj;
            return valueType.getMemory(v.value) + 8;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj == bObj) {
                return 0;
            }
            VersionedValue a = (VersionedValue) aObj;
            VersionedValue b = (VersionedValue) bObj;
            long comp = a.operationId - b.operationId;
            if (comp == 0) {
                return valueType.compare(a.value, b.value);
            }
            return Long.signum(comp);
        }

        @Override
        public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
            if (buff.get() == 0) {
                // fast path (no op ids or null entries)
                for (int i = 0; i < len; i++) {
                    VersionedValue v = new VersionedValue();
                    v.value = valueType.read(buff);
                    obj[i] = v;
                }
            } else {
                // slow path (some entries may be null)
                for (int i = 0; i < len; i++) {
                    obj[i] = read(buff);
                }
            }
        }

        @Override
        public Object read(ByteBuffer buff) {
            VersionedValue v = new VersionedValue();
            v.operationId = DataUtil.readVarLong(buff);
            if (buff.get() == 1) {
                v.value = valueType.read(buff);
            }
            return v;
        }

        @Override
        public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
            boolean fastPath = true;
            for (int i = 0; i < len; i++) {
                VersionedValue v = (VersionedValue) obj[i];
                if (v.operationId != 0 || v.value == null) {
                    fastPath = false;
                }
            }
            if (fastPath) {
                buff.put((byte) 0);
                for (int i = 0; i < len; i++) {
                    VersionedValue v = (VersionedValue) obj[i];
                    valueType.write(buff, v.value);
                }
            } else {
                // slow path:
                // store op ids, and some entries may be null
                buff.put((byte) 1);
                for (int i = 0; i < len; i++) {
                    write(buff, obj[i]);
                }
            }
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            VersionedValue v = (VersionedValue) obj;
            buff.putVarLong(v.operationId);
            if (v.value == null) {
                buff.put((byte) 0);
            } else {
                buff.put((byte) 1);
                valueType.write(buff, v.value);
            }
        }

    }

    /**
     * A data type that contains an array of objects with the specified data
     * types.
     */
    public static class ArrayType implements DataType {

        private final int arrayLength;
        private final DataType[] elementTypes;

        ArrayType(DataType[] elementTypes) {
            this.arrayLength = elementTypes.length;
            this.elementTypes = elementTypes;
        }

        @Override
        public int getMemory(Object obj) {
            Object[] array = (Object[]) obj;
            int size = 0;
            for (int i = 0; i < arrayLength; i++) {
                DataType t = elementTypes[i];
                Object o = array[i];
                if (o != null) {
                    size += t.getMemory(o);
                }
            }
            return size;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj == bObj) {
                return 0;
            }
            Object[] a = (Object[]) aObj;
            Object[] b = (Object[]) bObj;
            for (int i = 0; i < arrayLength; i++) {
                DataType t = elementTypes[i];
                int comp = t.compare(a[i], b[i]);
                if (comp != 0) {
                    return comp;
                }
            }
            return 0;
        }

        @Override
        public void read(ByteBuffer buff, Object[] obj,
                int len, boolean key) {
            for (int i = 0; i < len; i++) {
                obj[i] = read(buff);
            }
        }

        @Override
        public void write(WriteBuffer buff, Object[] obj,
                          int len, boolean key) {
            for (int i = 0; i < len; i++) {
                write(buff, obj[i]);
            }
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            Object[] array = (Object[]) obj;
            for (int i = 0; i < arrayLength; i++) {
                DataType t = elementTypes[i];
                Object o = array[i];
                if (o == null) {
                    buff.put((byte) 0);
                } else {
                    buff.put((byte) 1);
                    t.write(buff, o);
                }
            }
        }

        @Override
        public Object read(ByteBuffer buff) {
            Object[] array = new Object[arrayLength];
            for (int i = 0; i < arrayLength; i++) {
                DataType t = elementTypes[i];
                if (buff.get() == 1) {
                    array[i] = t.read(buff);
                }
            }
            return array;
        }

    }

}
