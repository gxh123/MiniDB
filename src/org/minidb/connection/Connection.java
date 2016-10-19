package org.minidb.connection;

import org.minidb.command.Command;
import org.minidb.command.CommandInterface;
import org.minidb.engine.DbException;
import org.minidb.engine.Session;
import org.minidb.engine.SessionFactory;

/**
 * Created by gxh on 2016/6/10.
 */
public class Connection {

    private Session session;
    private static SessionFactory sessionFactory;

    public Connection(ConnectionInfo ci){
        session = createSession(ci);
    }

    private Session createSession(ConnectionInfo ci){
        try{
            if(sessionFactory == null){
                sessionFactory = (SessionFactory)Class.forName("org.minidb.engine.Engine")
                        .getMethod("getInstance").invoke(null);
            }
            return sessionFactory.createSession(ci);
        }catch(Exception e){
            e.printStackTrace();
            throw DbException.getGeneralException("createSession error");
        }
    }

    public Statement createStatement(){
        return new Statement(this, session);
    }

    Command prepareCommand(String sql) {
        return session.prepareCommand(sql);
    }
}
