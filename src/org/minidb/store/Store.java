package org.minidb.store;

import org.minidb.engine.Database;
import org.minidb.engine.ValueDataType;
import org.minidb.store.mvstore.MVStore;
import org.minidb.store.mvstore.TransactionStore;

import java.util.HashMap;

/**
 * Created by gxh on 2016/6/11.
 */
public class Store {

    TransactionStore transactionStore;

    public Store(HashMap<String, Object> config){
        this.transactionStore = new TransactionStore(
                new MVStore(config),
                new ValueDataType(null));
    }

    public TransactionStore getTransactionStore() {
        return transactionStore;
    }

}

