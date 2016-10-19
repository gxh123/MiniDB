package org.minidb;

import org.minidb.connection.Connection;
import org.minidb.connection.ConnectionInfo;
import org.minidb.connection.ResultSet;
import org.minidb.connection.Statement;
import org.minidb.result.LocalResult;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by gxh on 2016/6/10.
 */
public class Shell {

    private BufferedReader reader;
    private Connection connection;
    private Statement statement;

    public Shell(){
        reader = new BufferedReader(new InputStreamReader(System.in));
    }

    public static void main(String[] args){
        new Shell().run();
    }

    public void run(){
        System.out.println("");
        System.out.println("Welcome to MiniDB Shell ");

        connectDatabase();
        String sql = null;

        while(true){
            if(sql == null){
                System.out.print("sql> ");
            } else {
                System.out.print("...> ");
            }
            sql = readLine();
             if(sql == null){
                System.out.print("IO error");
                break;
            }
            sql = sql.trim();
            if(sql.length() == 0) continue;
            if(sql.endsWith(";")){
                sql = sql.substring(0, sql.lastIndexOf(";"));
                execute(sql);
            }
            sql = null;
        }
    }

    private void connectDatabase(){
        Properties prop = new Properties();
        prop.setProperty("user", "admin");
        prop.setProperty("password", "admin");
        ConnectionInfo ci = new ConnectionInfo("miniDB:~/test", prop);
        connection = new Connection(ci);
        statement = connection.createStatement();
    }

    private String readLine(){
        String line;
        try {
             line = reader.readLine();
        }catch(IOException e){
            line = null;
        }
        return line;
    }

    private void execute(String sql){
        if(sql.length() == 0) return;  //只输入了一个;

        statement.execute(sql);
        LocalResult rs = statement.getResult();
        int rowCount = printResult(rs);
    }

    private int printResult(LocalResult rs){

        int len = rs.getVisibleColumnCount();
        ArrayList<String[]> rows = new ArrayList();
        // buffer the header
        String[] columns = new String[len];
        for (int i = 0; i < len; i++) {
            String s = rs.getAlias(i);
            columns[i] = s == null ? "" : s;
        }
        rows.add(columns);
        int rowCount = 0;
        while (rs.next()) {
            rowCount++;
            loadRow(rs, len, rows);
        }
        printRows(rows, len);
        rows.clear();
        return rowCount;
    }

    private boolean loadRow(LocalResult rs, int len, ArrayList<String[]> rows) {
        boolean truncated = false;
        String[] row = new String[len];
        for (int i = 0; i < len; i++) {
            String s = rs.currentRow()[i].getString();
            if (s == null) {
                s = "null";
            }
            row[i] = s;
        }
        rows.add(row);
        return truncated;
    }

    private int[] printRows(ArrayList<String[]> rows, int len) {
        int[] columnSizes = new int[len];
        for (int i = 0; i < len; i++) {
            int max = 0;
            for (String[] row : rows) {
                max = Math.max(max, row[i].length());
            }
            columnSizes[i] = max;
        }
        for (String[] row : rows) {
            StringBuilder buff = new StringBuilder();
            for (int i = 0; i < len; i++) {
                if (i > 0) {
                    buff.append(' ').append('|').append(' ');
                }
                String s = row[i];
                buff.append(s);
                if (i < len - 1) {
                    for (int j = s.length(); j < columnSizes[i]; j++) {
                        buff.append(' ');
                    }
                }
            }
            System.out.println(buff.toString());
        }
        return columnSizes;
    }



}
