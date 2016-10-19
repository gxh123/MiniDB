/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.util;

import org.minidb.value.Value;
import org.minidb.value.ValueInt;
import org.minidb.value.ValueLong;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * This class contains meta data information about data types,
 * and can convert between Java objects and Values.
 */
public class DataType {

    /**
     * The list of types. An ArrayList so that Tomcat doesn't set it to null
     * when clearing references.
     */
    private static final ArrayList<DataType> TYPES = new ArrayList();
    private static final HashMap<String, DataType> TYPES_BY_NAME = new HashMap();
    private static final ArrayList<DataType> TYPES_BY_VALUE_TYPE = new ArrayList();

    /**
     * The value type of this data type.
     */
    public int type;

    /**
     * The data type name.
     */
    public String name;

    /**
     * The SQL type.
     */
    public int sqlType;

    /**
     * The Java class name.
     */
    public String jdbc;

    /**
     * How closely the data type maps to the corresponding JDBC SQL type (low is
     * best).
     */
    public int sqlTypePos;

    /**
     * The maximum supported precision.
     */
    public long maxPrecision;

    /**
     * The lowest possible scale.
     */
    public int minScale;

    /**
     * The highest possible scale.
     */
    public int maxScale;

    /**
     * If this is a numeric type.
     */
    public boolean decimal;

    /**
     * The prefix required for the SQL literal representation.
     */
    public String prefix;

    /**
     * The suffix required for the SQL literal representation.
     */
    public String suffix;

    /**
     * The list of parameters used in the column definition.
     */
    public String params;

    /**
     * If this is an autoincrement type.
     */
    public boolean autoIncrement;

    /**
     * If this data type is an autoincrement type.
     */
    public boolean caseSensitive;

    /**
     * If the precision parameter is supported.
     */
    public boolean supportsPrecision;

    /**
     * If the scale parameter is supported.
     */
    public boolean supportsScale;

    /**
     * The default precision.
     */
    public long defaultPrecision;

    /**
     * The default scale.
     */
    public int defaultScale;

    /**
     * The default display size.
     */
    public int defaultDisplaySize;

    /**
     * If this data type should not be listed in the database meta data.
     */
    public boolean hidden;

    /**
     * The number of bytes required for an object.
     */
    public int memory;

    static {
        for (int i = 0; i < 11; i++) {
            TYPES_BY_VALUE_TYPE.add(null);
        }
        add(Value.NULL, Types.NULL, "Null",
                new DataType(),
                new String[]{"NULL"},
                // the value is always in the cache
                0
        );
        add(Value.STRING, Types.VARCHAR, "String",
                createString(true),
                new String[]{"VARCHAR", "VARCHAR2", "NVARCHAR", "NVARCHAR2",
                    "VARCHAR_CASESENSITIVE", "CHARACTER VARYING", "TID"},
                // 24 for ValueString, 24 for String
                48
        );
        add(Value.INT, Types.INTEGER, "Int",
                createDecimal(ValueInt.PRECISION, ValueInt.PRECISION, 0,
                        ValueInt.DISPLAY_SIZE, false, false),
                new String[]{"INTEGER", "INT", "MEDIUMINT", "INT4", "SIGNED"},
                // in many cases the value is in the cache
                20
        );
        add(Value.LONG, Types.BIGINT, "Long",
                createDecimal(ValueLong.PRECISION, ValueLong.PRECISION, 0,
                        ValueLong.DISPLAY_SIZE, false, false),
                new String[]{"BIGINT", "INT8", "LONG"},
                24
        );
        DataType dataType = new DataType();
        dataType.prefix = "(";
        dataType.suffix = "')";
        add(Value.ARRAY, Types.ARRAY, "Array",
                dataType,
                new String[]{"ARRAY"},
                32
        );
    }

    private static void add(int type, int sqlType, String jdbc,
            DataType dataType, String[] names, int memory) {
        for (int i = 0; i < names.length; i++) {
            DataType dt = new DataType();
            dt.type = type;
            dt.sqlType = sqlType;
            dt.jdbc = jdbc;
            dt.name = names[i];
            dt.autoIncrement = dataType.autoIncrement;
            dt.decimal = dataType.decimal;
            dt.maxPrecision = dataType.maxPrecision;
            dt.maxScale = dataType.maxScale;
            dt.minScale = dataType.minScale;
            dt.params = dataType.params;
            dt.prefix = dataType.prefix;
            dt.suffix = dataType.suffix;
            dt.supportsPrecision = dataType.supportsPrecision;
            dt.supportsScale = dataType.supportsScale;
            dt.defaultPrecision = dataType.defaultPrecision;
            dt.defaultScale = dataType.defaultScale;
            dt.defaultDisplaySize = dataType.defaultDisplaySize;
            dt.caseSensitive = dataType.caseSensitive;
            //从第二个名称开始的都是隐藏类型的，如下面的int
            //new String[]{"INTEGER", "INT", "MEDIUMINT", "INT4", "SIGNED"}
            //隐藏类型在用户在数据库中没有建表时可以覆盖
            //如CREATE DATATYPE IF NOT EXISTS int AS VARCHAR(255)
            //但是非隐藏类型就不能覆盖
            //如CREATE DATATYPE IF NOT EXISTS int AS VARCHAR(255)
            dt.hidden = i > 0;
            dt.memory = memory;
            for (DataType t2 : TYPES) {
                if (t2.sqlType == dt.sqlType) {
                    dt.sqlTypePos++;
                }
            }
            TYPES_BY_NAME.put(dt.name, dt);
            if (TYPES_BY_VALUE_TYPE.get(type) == null) {
                TYPES_BY_VALUE_TYPE.set(type, dt);
            }
            TYPES.add(dt);
        }
    }

    private static DataType createDecimal(int maxPrecision,
            int defaultPrecision, int defaultScale, int defaultDisplaySize,
            boolean needsPrecisionAndScale, boolean autoInc) {
        DataType dataType = new DataType();
        dataType.maxPrecision = maxPrecision;
        dataType.defaultPrecision = defaultPrecision;
        dataType.defaultScale = defaultScale;
        dataType.defaultDisplaySize = defaultDisplaySize;
        if (needsPrecisionAndScale) {
            dataType.params = "PRECISION,SCALE";
            dataType.supportsPrecision = true;
            dataType.supportsScale = true;
        }
        dataType.decimal = true;
        dataType.autoIncrement = autoInc;
        return dataType;
    }

    private static DataType createString(boolean caseSensitive) {
        DataType dataType = new DataType();
        dataType.prefix = "'";
        dataType.suffix = "'";
        dataType.params = "LENGTH";
        dataType.caseSensitive = caseSensitive;
        dataType.supportsPrecision = true;
        dataType.maxPrecision = Integer.MAX_VALUE;
        dataType.defaultPrecision = Integer.MAX_VALUE;
        dataType.defaultDisplaySize = Integer.MAX_VALUE;
        return dataType;
    }


    /**
     * Get the data type object for the given value type.
     *
     * @param type the value type
     * @return the data type object
     */
    public static DataType getDataType(int type) {
        if (type == Value.UNKNOWN) {
            throw new RuntimeException("UNKNOWN_DATA_TYPE");
        }
        DataType dt = TYPES_BY_VALUE_TYPE.get(type);
        if (dt == null) {
            dt = TYPES_BY_VALUE_TYPE.get(Value.NULL);
        }
        return dt;
    }

    public static DataType getTypeByName(String s) {
        return TYPES_BY_NAME.get(s);
    }

}
