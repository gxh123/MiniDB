package org.minidb.table;

import org.minidb.engine.*;
import org.minidb.index.*;
import org.minidb.schema.SchemaObjectBase;
import org.minidb.store.Store;
import org.minidb.store.mvstore.TransactionStore;
import org.minidb.store.mvstore.TransactionStore.TransactionMap;
import org.minidb.util.*;
import org.minidb.value.Value;
import org.minidb.value.ValueArray;
import org.minidb.value.ValueLong;
import org.minidb.value.ValueNull;

import java.util.*;

/**
 * Created by gxh on 2016/6/10.
 */
public class Table extends SchemaObjectBase {

    public static final int ASCENDING = 0;
    static final ValueLong MIN = ValueLong.get(Long.MIN_VALUE);

    /**
     * The maximum long value.
     */
    static final ValueLong MAX = ValueLong.get(Long.MAX_VALUE);

    private Store store;
    private Database database;
    private Column[] columns;                                       //table有哪些column，数组形式存放
    private HashMap<String, Column> columnMap = new HashMap<>();    //table的column，以hashmap形式存放，方便寻找
    private ArrayList<BaseIndex> indexes = new ArrayList<>();       //有哪些索引
    private TransactionMap<Value,Value> dataMap;                    //存放数据的map
    private long lastKey;
    private final String mapName;                                   //table维护的存放数据的map的名字
    private int primaryIndexColumnId = -1;                          //主索引所在列的ID，为-1表示没有主索引

    public Table(CreateTableData data, Database db){
        initSchemaObjectBase(data.schema , data.id, data.tableName);
        this.database = db;
        this.store = db.getStore();
        Column[] cols = new Column[data.columns.size()];
        data.columns.toArray(cols);
        setColumns(cols);

        int[] sortTypes = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            sortTypes[i] = ASCENDING;
        }
        ValueDataType keyType = new ValueDataType(null);
        ValueDataType valueType = new ValueDataType(sortTypes);
        mapName = "table." + getId();
        TransactionStore.Transaction t = store.getTransactionStore().begin();
        dataMap = t.openMap(mapName, keyType, valueType);
        t.commit();
        Value k = dataMap.lastKey();
        lastKey = k == null ? 0 : k.getLong();
    }

    protected void setColumns(Column[] columns){
        this.columns = columns;
        for( int i = 0; i < columns.length; i++){
            Column col = columns[i];
            col.setTable(this, i);
            String columnName = columns[i].getName();
            if(columnMap.get(columnName) != null){
                throw DbException.getGeneralException("列重复 error");
            }
            columnMap.put(columnName, columns[i]);
        }
    }

    /*
      创建表时，有主键则聚集索引(PrimaryIndex)在主键上
        没有定义主键，则不创建聚集索引
     */

    public void setPrimaryIndexColumnId(int primaryIndexColumnId) {
        this.primaryIndexColumnId = primaryIndexColumnId;
    }

    public int getPrimaryIndexColumnId() {
        return primaryIndexColumnId;
    }

    public TransactionMap<Value,Value> getDataMap(){
        return dataMap;
    }

    public Row getRow(Session session, long key) {
        TransactionMap<Value, Value> map = getMap(session);
        Value v = map.get(ValueLong.get(key));
        ValueArray array = (ValueArray) v;
        Row row = session.createRow(array.getList());
        row.setKey(key);
        return row;
    }

    public void addRow(Session session, Row row){
        if (primaryIndexColumnId == -1) {
            if (row.getKey() == 0) {
                row.setKey(++lastKey);
            }
        } else {
            long c = row.getValue(primaryIndexColumnId).getLong();
            row.setKey(c);
        }
        Value key = ValueLong.get(row.getKey());
        TransactionMap<Value, Value> map = getMap(session);
        map.put(key, ValueArray.get(row.getValueList()));
//        dataMap.put(key, ValueArray.get(row.getValueList()));
        lastKey = Math.max(lastKey, row.getKey());

        for (int i = 0, size = indexes.size(); i < size; i++) {
            BaseIndex index = indexes.get(i);
            index.add(session, row);
        }
    }

    public BaseIndex addIndex(Session session, String indexName, int indexId,
                          Column col, IndexType indexType) {
        if (indexType.isPrimaryKey()) {
            col.setPrimaryKey(true);
        }

        //database.lockMeta(session);
        BaseIndex index;
        int primaryIndexColumn;
        primaryIndexColumn = getPrimaryIndexColumn(indexType, col);
        if (primaryIndexColumn != -1) {
            setPrimaryIndexColumnId(primaryIndexColumn);
            index = new PrimaryIndex(this, indexId, indexName, col, indexType);
        } else {
            index = new SecondaryIndex(session.getDatabase(), this, indexId,
                    indexName, col, indexType);
        }
        if (index.needRebuild()) {
            rebuildIndex(session, index, indexName);
        }
        if (index.getCreateSQL() != null) {
            database.addSchemaObject(session, index);
        }
        indexes.add(index);
        return index;
    }

    private void rebuildIndex(Session session, BaseIndex index, String indexName) {
        try {
            if (session.getDatabase().getStore() == null){
                // in-memory
                rebuildIndexBuffered(session, index);
            } else {
                rebuildIndexBlockMerge(session, index);
            }
        } catch (DbException e) {
            throw new RuntimeException("rebuildIndex ERROR");
        }
    }

    private void rebuildIndexBlockMerge(Session session, BaseIndex index) {
        // Read entries in memory, sort them, write to a new map (in sorted
        // order); repeat (using a new map for every block of 1 MB) until all
        // record are read. Merge all maps to the target (using merge sort;
        // duplicates are detected in the target). For randomly ordered data,
        // this should use relatively few write operations.
        // A possible optimization is: change the buffer size from "row count"
        // to "amount of memory", and buffer index keys instead of rows.
        System.out.println("rebuildIndexBlockMerge 没写");
//        long remaining = getRowCount(session);
//        long total = remaining;
//        Cursor cursor = find(session, null, null);
//        long i = 0;
//        Store store = session.getDatabase().getStore();
//
//        int bufferSize = database.getMaxMemoryRows() / 2;
//        ArrayList<Row> buffer = new ArrayList(bufferSize);
//        String n = getName() + ":" + index.getName();
//        int t = MathUtils.convertLongToInt(total);
//        ArrayList<String> bufferNames = new ArrayList();
//        while (cursor.next()) {
//            Row row = cursor.get();
//            buffer.add(row);
//            if (buffer.size() >= bufferSize) {
//                sortRows(buffer, index);
//                String mapName = store.nextTemporaryMapName();
//                index.addRowsToBuffer(buffer, mapName);
//                bufferNames.add(mapName);
//                buffer.clear();
//            }
//            remaining--;
//        }
//        sortRows(buffer, index);
//        if (bufferNames.size() > 0) {
//            String mapName = store.nextTemporaryMapName();
//            index.addRowsToBuffer(buffer, mapName);
//            bufferNames.add(mapName);
//            buffer.clear();
//            index.addBufferedRows(bufferNames);
//        } else {
//            addRowsToIndex(session, buffer, index);
//        }
    }

    private void rebuildIndexBuffered(Session session, BaseIndex index) {
        System.out.println("rebuildIndexBuffered 没写");
//        long remaining = getRowCount(session);
//        long total = remaining;
//        Cursor cursor = find(session, null, null);
//        long i = 0;
//        int bufferSize = (int) Math.min(total, database.getMaxMemoryRows());
//        ArrayList<Row> buffer = new ArrayList(bufferSize);
//        String n = getName() + ":" + index.getName();
//        int t = MathUtils.convertLongToInt(total);
//        while (cursor.next()) {
//            Row row = cursor.get();
//            buffer.add(row);
//            if (buffer.size() >= bufferSize) {
//                addRowsToIndex(session, buffer, index);
//            }
//            remaining--;
//        }
//        addRowsToIndex(session, buffer, index);
//        if (SysProperties.CHECK && remaining != 0) {
//            DbException.throwInternalError("rowcount remaining=" + remaining +
//                    " " + getName());
//        }
    }

    private int getPrimaryIndexColumn(IndexType indexType, Column col) {
//        throw new RuntimeException("getPrimaryIndexColumn 没写");
        if (primaryIndexColumnId != -1) {
            return -1;
        }
        if (!indexType.isPrimaryKey()) {
            return -1;
        }
//        if (col.sortType != ASCENDING) {
//            return -1;
//        }
/*        switch (col.getType()) {
            case Value.BYTE:
            case Value.SHORT:
            case Value.INT:
            case Value.LONG:
                break;
            default:
                return -1;
        }*/
        return col.getColumnId();
    }

    public Column getColumn(String columnName) {
        Column column = columnMap.get(columnName);
        if (column == null) {
            throw new RuntimeException("COLUMN_NOT_FOUND ERROR");
//            throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, columnName);
        }
        return column;
    }

    private static void addRowsToIndex(Session session, ArrayList<Row> list,
                                       BaseIndex index) {
        sortRows(list, index);
        for (Row row : list) {
            index.add(session, row);
        }
        list.clear();
    }

    private static void sortRows(ArrayList<Row> list, final BaseIndex index) {
        throw new RuntimeException("sortRows 没写");
//        Collections.sort(list, new Comparator<Row>() {
//            @Override
//            public int compare(Row r1, Row r2) {
//                return index.compareRows(r1, r2);
//            }
//        });
    }

    public Row getTemplateRow() {
        return database.createRow(new Value[columns.length]);
    }

    public TransactionStore.Transaction getTransaction(Session session) {
        if (session == null) {
            return store.getTransactionStore().begin();
        }
        return session.getTransaction();
    }

    @Override
    public int getType() {
        return DbObject.TABLE_OR_VIEW;
    }

    @Override
    public String getCreateSQL() {
        Database db = getDatabase();
        if (db == null) {
            // closed
            return null;
        }
        StatementBuilder buff = new StatementBuilder("CREATE ");
        buff.append("TABLE ");
        buff.append(getSQL());
        buff.append("(\n    ");
        for (Column column : columns) {
            buff.appendExceptFirst(",\n    ");
            buff.append(column.getCreateSQL());
        }
        buff.append("\n)");
        return buff.toString();
    }

    public long getRowCount(Session session) {
        TransactionMap<Value, Value> map = getMap(session);
        return map.sizeAsLong();
//        return dataMap.sizeAsLong();
    }

//    public Cursor find(Session session, ValueLong first, ValueLong last) {
//        return new TableCursor(session, dataMap.entryIterator(first), last);
//    }

//    public Cursor find(Session session, Row first, Row last) {
////        ValueLong min = getKey(first, MIN, MIN);
////        ValueLong max = getKey(last, MAX, MIN);
//
//        ValueLong min, max;
//        if (first == null) {
//            min = MIN;
//        } else if (primaryIndexColumnId < 0) {
//            min = ValueLong.get(first.getKey());
//        } else {
//            ValueLong v = (ValueLong) first.getValue(primaryIndexColumnId);
//            if (v == null) {
//                min = ValueLong.get(first.getKey());
//            } else {
//                min = v;
//            }
//        }
//        if (last == null) {
//            max = MAX;
//        } else if (primaryIndexColumnId < 0) {
//            max = ValueLong.get(last.getKey());
//        } else {
//            ValueLong v = (ValueLong) last.getValue(primaryIndexColumnId);
//            if (v == null) {
//                max = ValueLong.get(last.getKey());
//            } else {
//                max = v;
//            }
//        }
//
//        TransactionMap<Value, Value> map = getMap(session);
//        return new TableCursor(session, map.entryIterator(min), max);
//    }


    TransactionMap<Value, Value> getMap(Session session) {
        if (session == null) {
            return dataMap;
        }
        TransactionStore.Transaction t = getTransaction(session);
        return dataMap.getInstance(t, Long.MAX_VALUE);
    }

    public ValueLong getKey(Row row, ValueLong ifEmpty, ValueLong ifNull) {
        if (row == null) {
            return ifEmpty;
        }
        Value v = row.getValue(primaryIndexColumnId);
        if (v == null) {
            throw new RuntimeException("getKey ERROR");
        } else if (v == ValueNull.INSTANCE) {
            return ifNull;
        }
        return (ValueLong) v.convertTo(Value.LONG);
    }

    public Cursor find(Session session, ValueLong first, ValueLong last) {
        TransactionMap<Value, Value> map = getMap(session);
        return new TableCursor(session, map.entryIterator(first), last);
    }

    public PlanItem getBestPlanItem(Session session, int[] masks,
                                    TableFilter filter, HashSet<Column> allColumnsSet) {
        PlanItem item = new PlanItem();
//        item.setIndex(getScanIndex(session));
        item.cost = this.getBaseCost();                       //最基本的顺序扫描
        ArrayList<BaseIndex> indexes = getIndexes();
        if (indexes != null && masks != null) {
            for (int i = 0, size = indexes.size(); i < size; i++) {
                BaseIndex index = indexes.get(i);
                double cost = index.getCost(session, masks, filter, allColumnsSet);
                if (cost < item.cost) {
                    item.cost = cost;
                    item.setIndex(index);
                }
            }
        }
        return item;
    }

    public ArrayList<BaseIndex> getIndexes() {
        return indexes;
    }

    public Column[] getColumns() {
        return columns;
    }

    public double getBaseCost() {
        return dataMap.sizeAsLongMax() + 1;   //有多少行，cost就是多少
    }

    public BaseIndex findPrimaryKey() {
        ArrayList<BaseIndex> indexes = getIndexes();
        if (indexes != null) {
            for (int i = 0, size = indexes.size(); i < size; i++) {
                BaseIndex idx = indexes.get(i);
                if (idx.getIndexType().isPrimaryKey()) {
                    return idx;
                }
            }
        }
        return null;
    }

    class TableCursor implements Cursor {

        private final Session session;
        private final Iterator<Map.Entry<Value, Value>> it;
        private final ValueLong last;
        private Map.Entry<Value, Value> current;
        private Row row;

        public TableCursor(Session session, Iterator<Map.Entry<Value, Value>> it, ValueLong last) {
            this.session = session;
            this.it = it;
            this.last = last;
        }

        @Override
        public Row get() {
            if (row == null) {
                if (current != null) {
                    ValueArray array = (ValueArray) current.getValue();
                    row = session.createRow(array.getList());
                    row.setKey(current.getKey().getLong());
                }
            }
            return row;
        }

        @Override
        public Row getSearchRow() {
            return get();
        }

        @Override
        public boolean next() {
            current = it.hasNext() ? it.next() : null;
            if (current != null && current.getKey().getLong() > last.getLong()) {
                current = null;
            }
            row = null;
            return current != null;
        }

        @Override
        public boolean previous() {
            throw new RuntimeException("previous 不支持");
//            throw DbException.getUnsupportedException("previous");
        }

    }
}





























