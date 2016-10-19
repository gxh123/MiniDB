package org.minidb.engine;

/**
 * Created by gxh on 2016/6/24.
 */
public interface DbObject {

    int TABLE_OR_VIEW = 0;
    int INDEX = 1;
    int USER = 2;
    int SCHEMA = 10;

    String getName();

    int getId();

    int getType();

    String getCreateSQL();
}
