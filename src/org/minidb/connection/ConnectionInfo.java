package org.minidb.connection;

import org.minidb.engine.DbException;
import org.minidb.store.mvstore.filestore.security.SHA256;

import java.util.HashSet;
import java.util.Properties;

/**
 * Created by gxh on 2016/6/10.
 */
public class ConnectionInfo {
    private String user;
    private byte[] userPasswordHash;
    private String dbFileName;

    public ConnectionInfo(String url, Properties prop){
        if(url.startsWith("miniDB:") == false){
            throw DbException.getInvalidValueException(url);
        }
        this.user = prop.get("user").toString().toUpperCase();
        this.userPasswordHash = hashPassword(user, prop.get("password").toString().toCharArray());
    }

    private static byte[] hashPassword(String userName, char[] password) {
        return SHA256.getKeyPasswordHash(userName, password);
    }

    public String getDbFileName(){
        if(dbFileName == null){
            String name = "C:/Users/gxh/test.mv.db";   //暂时先采用固定名字
            String suffix = ".mv.db";
            dbFileName = name.substring(0, name.length() - suffix.length());
        }
        return dbFileName;
    }

    public String getUserName(){
        return user;
    }

    public byte[] getPasswordHash(){
        return userPasswordHash;
    }
}
