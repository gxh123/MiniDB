package org.minidb.engine;

import java.sql.SQLException;

/**
 * Created by gxh on 2016/6/10.
 */
public class DbException extends RuntimeException{

    private DbException(SQLException e) {
        super(e.getMessage(), e);
    }

    public static DbException getInvalidValueException(Object value) {
        return get(value == null ? "null" : value.toString());
    }

    public static DbException get(String params) {
        return new DbException(new SQLException("getInvalidValue " + params));
    }

    public static DbException getGeneralException(String value) {
        return new DbException(new SQLException(value));
    }


}
