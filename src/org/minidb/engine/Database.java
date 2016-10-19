package org.minidb.engine;

import org.minidb.index.BaseIndex;
import org.minidb.index.Cursor;
import org.minidb.index.IndexType;
import org.minidb.schema.Schema;
import org.minidb.schema.SchemaObject;
import org.minidb.store.Store;
import org.minidb.store.StoreBuilder;
import org.minidb.table.*;
import org.minidb.util.BitField;
import org.minidb.util.StringUtils;
import org.minidb.value.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

/**
 * Created by gxh on 2016/6/10.
 */
public class Database {

    private Schema mainSchema;
    private Schema infoSchema;
    private Session systemSession;
    private Table meta;
    private Store store;
    private static TableFactory tableEngine = new TableEngine();
    private HashMap<String, Schema> schemas = new HashMap<String, Schema>();
    private RowFactory rowFactory = new RowFactory();
    private HashMap<String, User> users = new HashMap<>();
    private volatile boolean metaTablesInitialized;    //volatile ？？
    private final BitField objectIds = new BitField();
    private boolean starting;
    private BaseIndex metaIdIndex;
    private final String databaseShortName = "TEST";

    public Database(){
        //数据库属性的初始化

        openDatabase();
    }

    private synchronized void openDatabase(){

        starting = true;
        openStore();
        starting = false;
        mainSchema = new Schema(this, 0, "PUBLIC");
        infoSchema = new Schema(this, -1, "INFORMATION_SCHEMA");
        schemas.put(mainSchema.getSchemaName(), mainSchema);
        schemas.put(infoSchema.getSchemaName(), infoSchema);

        User systemUser = new User(this, 0, "DBA");
        systemUser.setAdmin(true);
        systemSession = new Session(this,systemUser);
        meta = createMetaTable();
        objectIds.set(0);
        starting = true;
        metaIdIndex = meta.addIndex(systemSession, "SYS_ID",
                0, new Column("ID", Value.INT), IndexType.createPrimaryKey(false, false));
        Cursor cursor = metaIdIndex.find(systemSession, null, null);
//        Cursor cursor = meta.find(systemSession, null, null);
        ArrayList<MetaRecord> records = new ArrayList<>();
        while(cursor.next()){
            MetaRecord rec = new MetaRecord(cursor.get());
            objectIds.set(rec.getId());
            records.add(rec);
        }
        for(MetaRecord rec: records){
            rec.execute(this, systemSession);
        }
        starting = false;
        systemSession.commit(true);
    }

    private Table createMetaTable(){
        CreateTableData tableData = new CreateTableData();
        tableData.id = 0;
        tableData.tableName = "SYS";    //SYS存放数据定义SQL
        tableData.session = systemSession;
        tableData.columns.add(new Column("ID", Value.INT));
        tableData.columns.add(new Column("SQL", Value.STRING));
        return mainSchema.createTable(tableData);
    }

    public TableFactory getTableFactory(){
        return tableEngine;
    }

    public Store openStore(){
        if(store == null){
            StoreBuilder builder = new StoreBuilder();
            builder.setFileName("C:/Users/gxh/test.miniDb.db");
            builder.setDatabase(this);
            store = builder.openStore();
        }
        return store;
    }

    public Row createRow(Value[] data){
        return rowFactory.createRow(data);
    }

    public ArrayList<User> getAllUsers(){
        return new ArrayList<>(users.values());
    }

    public synchronized void setMasterUser(User user){
        addDatabaseObject(systemSession, user);
        systemSession.commit(true);
    }

    public synchronized void addDatabaseObject(Session session, DbObject obj){
        HashMap<String, DbObject> map = getMap(obj.getType());
        String name = obj.getName();
        if( map.get(name) !=  null){
            throw new RuntimeException("object exists error");
        }
//        lockMeta(session);     //先注释！！！
        addMeta(session, obj);
        map.put(name, obj);
    }

    private synchronized void addMeta(Session session, DbObject obj) {
        int id = obj.getId();
        if (id > 0 && !starting ) {
            Row r = meta.getTemplateRow();
            MetaRecord rec = new MetaRecord(obj);
            rec.setRecord(r);
            objectIds.set(id);
            meta.addRow(session, r);
        }
    }

    public boolean areEqual(Value a, Value b) {
        // can not use equals because ValueDecimal 0.0 is not equal to 0.00.
        return a.compareTo(b) == 0;
    }

    public int compare(Value a, Value b) {
        return a.compareTo(b);
    }

    public String getDatabaseShortName(){
        return databaseShortName;
    }

    private HashMap<String, DbObject> getMap(int type){
        HashMap<String, ?> result;                     //!!!
        switch (type){
            case DbObject.USER :
                result = users;
            break;
            case DbObject.SCHEMA:
                result = schemas;
            break;
            default:
                throw new RuntimeException("getMap error");
        }
        return (HashMap<String, DbObject>)result;
    }

    public User findUser(String name) {
        return users.get(name);
    }

    public Session createSession(User user){
        Session session = new Session(this, user);
        return session;
    }

    public Store getStore(){
        return store;
    }

    public Schema getSchema(String schemaName) {
        Schema schema = findSchema(schemaName);
        if (schema == null) {
            throw new RuntimeException("getSchema error");
        }
        return schema;
    }

    public Schema findSchema(String schemaName) {
        Schema schema = schemas.get(schemaName);
        if (schema == infoSchema) {
            initMetaTables();
        }
        return schema;
    }

    private void initMetaTables() {
        if (metaTablesInitialized) {
            return;
        }
        synchronized (infoSchema) {
            if (!metaTablesInitialized) {
                //将所有的MetaTable放入INFORMATION_SCHEMA
                for (int type = 0, count = MetaTable.getMetaTableTypeCount(); type < count; type++) {
                    CreateTableData tableData = new CreateTableData();
                    tableData.id =  -1 - type;
                    tableData.schema = infoSchema;
                    MetaTable m = new MetaTable(this, tableData, type);
                    infoSchema.add(m);
                }
                metaTablesInitialized = true;
            }
        }
    }

    public void addSchemaObject(Session session, SchemaObject obj) {
        int id = obj.getId();
//        lockMeta(session);
        synchronized (this) {
            obj.getSchema().add(obj);
            addMeta(session, obj);
        }
    }

    /**
     * Allocate a new object id.
     *
     * @return the id
     */
    public synchronized int allocateObjectId() { //如果前面的对象id回收了，这里会重复利用前面已回收的id。
        int i = objectIds.nextClearBit(0);
        objectIds.set(i);
        return i;
    }

    public boolean equalsIdentifiers(String a, String b) {
        if (a == b || a.equals(b)) {
            return true;
        }
//        if (!dbSettings.databaseToUpper && a.equalsIgnoreCase(b)) {
//            return true;
//        }
        return false;
    }

}


































