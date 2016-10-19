/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.minidb.engine;

import org.minidb.command.Prepared;
import org.minidb.table.Row;
import org.minidb.value.ValueInt;
import org.minidb.value.ValueString;

/**
 * A record in the system table of the database.
 * It contains the SQL statement to create the database object.
 */
public class MetaRecord implements Comparable<MetaRecord> {

    private final int id;
    private final String sql;

    public MetaRecord(Row r) {
        id = r.getValue(0).getInt();
        sql = r.getValue(1).getString();
    }

    MetaRecord(DbObject obj) {
        id = obj.getId();
        sql = obj.getCreateSQL();
    }
    
    //用MetaRecord给row赋值，分别对应ID、SQL字段
    //比较麻烦，先不写
    void setRecord(Row r) {
        r.setValue(0, ValueInt.get(id));
        r.setValue(1, ValueString.get(sql));
    }

    public int getId() {
        return id;
    }


    void execute(Database db, Session systemSession) {
        try {
            Prepared command = systemSession.prepare(sql);
            System.out.println(sql);
            command.setObjectId(id);
            command.update();
        } catch (DbException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "MetaRecord [id=" + id + ", sql=" + sql + "]";
    }

    @Override
    public int compareTo(MetaRecord o) {
        return 0;
    }
}
