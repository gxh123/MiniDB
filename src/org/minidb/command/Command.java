package org.minidb.command;

import org.minidb.engine.Database;
import org.minidb.engine.DbException;
import org.minidb.engine.Session;
import org.minidb.result.LocalResult;
import org.minidb.result.ResultInterface;

import java.sql.SQLException;

/**
 * Created by gxh on 2016/6/10.
 */
public class Command implements CommandInterface{

    protected final Session session;
    private final String sql;
    private Prepared prepared;

    Command(Parser parser, String sql, Prepared prepared) {
        this.session = parser.getSession();
        this.sql = sql;
        this.prepared = prepared;
    }

    @Override
    public boolean isQuery() {
        return prepared.isQuery();
    }

    @Override
    public LocalResult executeQuery() {
        long start = 0;
        Database database = session.getDatabase();
        Object sync = (Object) database;
        boolean callStop = true;

        synchronized (sync) {
            try {
                while (true) {
                    try {
                        return query(1000);
                    } catch (Exception e){
                        e.printStackTrace();
                    }
                }
            } catch (DbException e) {
                throw e;
            } finally {
                if (callStop) {
                    stop();
                }
            }
        }
    }

    @Override
    public int executeUpdate() {
        long start = 0;
        Database database = session.getDatabase();
        //默认一个数据库只允许一个线程更新，通过SET MULTI_THREADED 1可变成多线程的，
        //这样同步对象是session，即不同的session之间可以并发使用数据库，但是同一个session内部是只允许一个线程。
        //通过使用database作为同步对象就相当于数据库是单线程的
        Object sync = (Object) database;
        boolean callStop = true;

        synchronized (sync) {
            try {
                while (true) {
                    try {
                        return update();
                    } catch (Exception e){
                        e.printStackTrace();
                    }
                }
            } catch (DbException e) {
                throw e;
            } finally {
                if (callStop) {
                    stop();
                }
            }
        }
    }

    private void stop() {
        //DDL的isTransactional默认都是false，相当于每执行完一条DDL都默认提交事务
        session.commit(true);
    }

    public int update() {
        int updateCount = prepared.update();
        return updateCount;
    }

    public LocalResult query(int maxrows) {
//        throw new RuntimeException("query NOT ALLOWED");
        LocalResult result = (LocalResult)prepared.query(maxrows);
        return result;
    }

}
