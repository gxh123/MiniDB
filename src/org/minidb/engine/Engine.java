package org.minidb.engine;

import org.minidb.connection.ConnectionInfo;

import java.util.HashMap;

/**
 * Created by gxh on 2016/6/10.
 */
public class Engine implements SessionFactory{
    private static Engine INSTANCE = new Engine();
    private static HashMap<String, Database> DATABASES = new HashMap<String, Database>();

    public static Engine getInstance(){
        return  INSTANCE;
    }

    public Session createSession(ConnectionInfo ci){
        String name = ci.getDbFileName();
        Database database = DATABASES.get(name);
        User user = null;
        if(database == null){
            database = new Database();
            if(database.getAllUsers().size() == 0){
                user = new User(database, database.allocateObjectId(), ci.getUserName());
                user.setAdmin(true);
                user.setUserPasswordHash(ci.getPasswordHash());
                database.setMasterUser(user);
            }
        }
        synchronized (database){
            if(user == null){
                user = database.findUser(ci.getUserName());
                if(user != null ){
                    if( !user.validateUserPasswordHash(ci.getPasswordHash())){   //还没写，默认true
                        user = null;
                    }
                }
            }
            if(user == null){
                throw new RuntimeException("wrong user or password");
            }
            Session session = database.createSession(user);
            return session;
        }
    }


}
