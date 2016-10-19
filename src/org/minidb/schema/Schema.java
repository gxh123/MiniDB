package org.minidb.schema;

import org.minidb.engine.*;
import org.minidb.index.BaseIndex;
import org.minidb.table.CreateTableData;
import org.minidb.table.Table;

import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by gxh on 2016/6/10.
 */
public class Schema extends DbObjectBase {

    private final HashMap<String, Table> tablesAndViews = new HashMap<>();
    private final HashMap<String, BaseIndex> indexes = new HashMap<>();
    private final HashSet<String> temporaryUniqueNames = new HashSet();

    public Schema(Database database, int objectId, String schemaName){
        initDbObjectBase(database, objectId , schemaName );
    }

    public String getSchemaName(){
        return getName();
    }

    public Table createTable(CreateTableData data){
        data.schema = this;
        return database.getTableFactory().createTable(data);
    }

    public Table findTableOrView(Session session, String name) {
        Table table = tablesAndViews.get(name);
        return table;
    }

    public void add(SchemaObject obj) {
        String name = obj.getName();
        HashMap<String, SchemaObject> map = getMap(obj.getType());
        map.put(name, obj);
    }

    private HashMap<String, SchemaObject> getMap(int type) {
        HashMap<String, ? extends SchemaObject> result;
        switch (type) {
            case DbObject.TABLE_OR_VIEW:
                result = tablesAndViews;
                break;
            case DbObject.INDEX:
                result = indexes;
                break;
            default:
                throw new RuntimeException("getMap ERROR" );
        }
        return (HashMap<String, SchemaObject>) result;
    }

    @Override
    public int getType() {
        return DbObject.SCHEMA;
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    public Table getTableOrView(Session session, String name) {
        Table table = tablesAndViews.get(name);
        if (table == null) {
            throw new RuntimeException("getTableOrView ERROR");
        }
        return table;
    }

    public String getUniqueIndexName(Session session, Table table, String prefix) {
        HashMap<String, BaseIndex> tableIndexes;
        tableIndexes = indexes;
        return getUniqueName(table, tableIndexes, prefix);
    }

    private String getUniqueName(DbObject obj,
                                 HashMap<String, ? extends SchemaObject> map, String prefix) {
        String hash = Integer.toHexString(obj.getName().hashCode()).toUpperCase();
        String name = null;
        synchronized (temporaryUniqueNames) {
            for (int i = 1, len = hash.length(); i < len; i++) {
                name = prefix + hash.substring(0, i);
                if (!map.containsKey(name) && !temporaryUniqueNames.contains(name)) {
                    break;
                }
                name = null;
            }
            if (name == null) {
                prefix = prefix + hash + "_";
                for (int i = 0;; i++) {
                    name = prefix + i;
                    if (!map.containsKey(name) && !temporaryUniqueNames.contains(name)) {
                        break;
                    }
                }
            }
            temporaryUniqueNames.add(name);
        }
        return name;
    }

    public void freeUniqueName(String name) {
        if (name != null) {
            synchronized (temporaryUniqueNames) {
                temporaryUniqueNames.remove(name);
            }
        }
    }

    public BaseIndex findIndex(Session session, String name) {
        BaseIndex index = indexes.get(name);
        return index;
    }
}
