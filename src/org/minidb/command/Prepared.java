package org.minidb.command;

import org.minidb.engine.Session;
import org.minidb.result.ResultInterface;

/**
 * Created by gxh on 2016/7/12.
 */
public abstract class Prepared {

    protected Session session;
    private int objectId;
    protected String sqlStatement;
    private int currentRowNumber;

    public Prepared(Session session){
        this.session = session;
    }

    public void prepare(){

    }

    public void setSQL(String sql) {
        this.sqlStatement = sql;
    }

    protected int getObjectId() { //get完之后，如果原来的objectId不为0，那么要设为0
        int id = objectId;
        if (id == 0) {
            id = session.getDatabase().allocateObjectId();
        } else {
            objectId = 0;
        }
        return id;
    }

    public void setObjectId(int i) {
        this.objectId = i;
    }

    public int update() {
        throw new RuntimeException("UPDATE NOT ALLOWED");
    }

    public boolean isQuery() {
        return false;
    }


    public ResultInterface query(int maxrows) {
        throw new RuntimeException("query NOT ALLOWED");
    }

    protected void setCurrentRowNumber(int rowNumber) {
        this.currentRowNumber = rowNumber;
    }

    public int getCurrentRowNumber() {
        return currentRowNumber;
    }
}
