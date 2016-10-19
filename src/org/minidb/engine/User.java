package org.minidb.engine;

import org.minidb.store.mvstore.filestore.security.SHA256;
import org.minidb.util.MathUtils;
import org.minidb.util.StringUtils;

/**
 * Created by gxh on 2016/6/24.
 */
public class User extends DbObjectBase{

    private String userName;
    private byte[] salt;
    private byte[] passwordHash;
    private Database database;
    private boolean admin;

    public User(Database database, int id, String userName){
        initDbObjectBase(database, id, userName);
        this.database = database;
        this.userName = userName;
    }

    public void setSaltAndHash(byte[] salt, byte[] hash) {
        this.salt = salt;
        this.passwordHash = hash;
    }

    @Override
    public String getName() {
        return userName;
    }

    @Override
    public int getType() {
        return DbObject.USER;
    }

    @Override
    public String getCreateSQL() {
        StringBuilder buff = new StringBuilder("CREATE USER IF NOT EXISTS ");
        buff.append(getSQL());
        buff.append(" SALT '").
                append(StringUtils.convertBytesToHex(salt)).
                append("' HASH '").
                append(StringUtils.convertBytesToHex(passwordHash)).
                append('\'');
        if (admin) {
            buff.append(" ADMIN");
        }
        return buff.toString();
    }

    boolean validateUserPasswordHash(byte[] userPasswordHash) {
        return true;
    }

    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    public boolean isAdmin() {
        return admin;
    }

    public void setUserPasswordHash(byte[] userPasswordHash) {
        if (userPasswordHash != null) {
            if (userPasswordHash.length == 0) {
                salt = passwordHash = userPasswordHash;
            } else {
                salt = new byte[Constants.SALT_LEN];
                MathUtils.randomBytes(salt);
                passwordHash = SHA256.getHashWithSalt(userPasswordHash, salt);
            }
        }
    }
}
