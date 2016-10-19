package org.minidb.index;

import org.minidb.engine.Database;
import org.minidb.engine.Session;
import org.minidb.store.mvstore.TransactionStore;
import org.minidb.table.Column;
import org.minidb.table.Row;
import org.minidb.table.Table;
import org.minidb.table.TableFilter;
import org.minidb.value.Value;
import org.minidb.value.ValueLong;

import java.util.HashSet;
import java.util.List;

/**
 * Created by gxh on 2016/7/16.
 */
public class SecondaryIndex extends BaseIndex{

    public static final int ASCENDING = 0;

//    final Table table;

//    private final int keyColumns;
//    private final String mapName;
    private TransactionStore.TransactionMap<Value, Value> dataMap;

    public SecondaryIndex(Database db, Table table, int id, String indexName,
                          Column col, IndexType indexType) {
        System.out.println("SecondaryIndex 构造器 没写");
//        this.table = table;
//        initBaseIndex(table, id, indexName);
//        // always store the row key in the map key,
//        // even for unique indexes, as some of the index columns could be null
//        keyColumns = columns.length + 1;
//        mapName = "index." + getId();
//        int[] sortTypes = new int[keyColumns];
//        for (int i = 0; i < columns.length; i++) {
//            sortTypes[i] = columns[i].sortType;
//        }
//        sortTypes[keyColumns - 1] = ASCENDING;
//        ValueDataType keyType = new ValueDataType(sortTypes);
//        ValueDataType valueType = new ValueDataType(null);
//        TransactionStore.Transaction t = Table.getTransaction(null);
//        dataMap = t.openMap(mapName, keyType, valueType);
//        t.commit();
    }

    @Override
    public Cursor find(Session session, Row start, Row end) {
        System.out.println("find 没写");
        return null;
    }

    @Override
    public void add(Session session, Row row) {
        System.out.println("add 没写");
    }

    public boolean needRebuild() {
        try {
            return dataMap.sizeAsLongMax() == 0;
        } catch (IllegalStateException e) {
            throw new RuntimeException("needRebuild ERROR");
        }
    }

    public void addRowsToBuffer(List<Row> rows, String bufferName) {
        System.out.println("addRowsToBuffer 没写");
//        MVMap<Value, Value> map = openMap(bufferName);
//        for (Row row : rows) {
//            ValueArray key = convertToKey(row);
//            map.put(key, ValueNull.INSTANCE);
//        }
    }

    public void addBufferedRows(List<String> bufferNames) {
        System.out.println("addBufferedRows 没写");
//        ArrayList<String> mapNames = New.arrayList(bufferNames);
//        final CompareMode compareMode = database.getCompareMode();
//        /**
//         * A source of values.
//         */
//        class Source implements Comparable<Source> {
//            Value value;
//            Iterator<Value> next;
//            int sourceId;
//            @Override
//            public int compareTo(Source o) {
//                int comp = value.compareTo(o.value, compareMode);
//                if (comp == 0) {
//                    comp = sourceId - o.sourceId;
//                }
//                return comp;
//            }
//        }
//        TreeSet<Source> sources = new TreeSet<Source>();
//        for (int i = 0; i < bufferNames.size(); i++) {
//            MVMap<Value, Value> map = openMap(bufferNames.get(i));
//            Iterator<Value> it = map.keyIterator(null);
//            if (it.hasNext()) {
//                Source s = new Source();
//                s.value = it.next();
//                s.next = it;
//                s.sourceId = i;
//                sources.add(s);
//            }
//        }
//        try {
//            while (true) {
//                Source s = sources.first();
//                Value v = s.value;
//
//                if (indexType.isUnique()) {
//                    Value[] array = ((ValueArray) v).getList();
//                    // don't change the original value
//                    array = array.clone();
//                    array[keyColumns - 1] = ValueLong.get(Long.MIN_VALUE);
//                    ValueArray unique = ValueArray.get(array);
//                    SearchRow row = convertToSearchRow((ValueArray) v);
//                    checkUnique(row, dataMap, unique);
//                }
//
//                dataMap.putCommitted(v, ValueNull.INSTANCE);
//
//                Iterator<Value> it = s.next;
//                if (!it.hasNext()) {
//                    sources.remove(s);
//                    if (sources.size() == 0) {
//                        break;
//                    }
//                } else {
//                    Value nextValue = it.next();
//                    sources.remove(s);
//                    s.value = nextValue;
//                    sources.add(s);
//                }
//            }
//        } finally {
//            for (String tempMapName : mapNames) {
//                MVMap<Value, Value> map = openMap(tempMapName);
//                map.getStore().removeMap(map);
//            }
//        }
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public double getCost(Session session, int[] masks, TableFilter filter, HashSet<Column> allColumnsSet) {
        throw new RuntimeException("getCost 没写");
    }
}
