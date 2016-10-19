package org.minidb.table;

import org.minidb.command.Parser;
import org.minidb.util.DataType;
import org.minidb.value.Value;

/**
 * Created by gxh on 2016/6/10.
 */
public class Column {

    public static final String ROWID = "_ROWID_";

    private String name;
    private int type;
    private boolean isPrimaryKey;
    private boolean nullable = true;
    private String originalSQL;
    private int columnId;
    private Table table;

    public Column(String name, int type){
        this.name = name;
        this.type = type;
    }
    /**
     * Set the table and column id.
     *
     * @param table the table
     * @param columnId the column index
     */
    public void setTable(Table table, int columnId) {
        this.table = table;
        this.columnId = columnId;
    }


    public String getName(){
        return name;
    }

    public Boolean isPrimaryKey(){
        return isPrimaryKey;
    }

    public void setPrimaryKey(boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean b) {
        nullable = b;
    }

    public void setOriginalSQL(String original) {
        originalSQL = original;
    }

    public String getOriginalSQL() {
        return originalSQL;
    }

    public String getCreateSQL() {
        StringBuilder buff = new StringBuilder();
        if (name != null) {
            buff.append(Parser.quoteIdentifier(name)).append(' ');
        }
        if (originalSQL != null) {
            buff.append(originalSQL);
        } else {
            buff.append(DataType.getDataType(type).name);
        }
        if (!nullable) {
            buff.append(" NOT NULL");
        }
        return buff.toString();
    }

    public int getColumnId() {
        return columnId;
    }

    public String getSQL() {
        return Parser.quoteIdentifier(name);
    }

    /**
     * Convert a value to this column's type.
     *
     * @param v the value
     * @return the value
     */
    public Value convert(Value v) {
        try {
            return v.convertTo(type);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public int getType() {
        return type;
    }

}
