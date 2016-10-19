package org.minidb.store;

import org.minidb.engine.Database;

import java.util.HashMap;

/**
 * Created by gxh on 2016/6/11.
 */
public class StoreBuilder {   //复杂对象的构造，采用builder模式


    private HashMap<String, Object> config = new HashMap<>();

    private void set(String key, Object value){
        config.put(key,value);
    }

    public void setFileName(String fileName){
        set("fileName", fileName);
    }

    public void setDatabase(Database db){
        set("Database", db);
    }

    public void setCacheSize( int mb){
        set("cacheSize", mb);
    }

    public Store openStore(){
        return new Store(config);
    }
}
