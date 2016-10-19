package org.minidb.connection;

import org.minidb.command.Command;
import org.minidb.engine.Session;
import org.minidb.result.LocalResult;

/**
 * Created by gxh on 2016/6/10.
 */
public class Statement {

    private Connection connection;
    private Session session;
    private LocalResult result;

    public Statement(Connection connection, Session session){
        this.connection = connection;
        this.session = session;
    }

    public void execute(String sql){
        Command command = connection.prepareCommand(sql);
        if (command.isQuery()) {
            result = command.executeQuery();
        }else {
            command.executeUpdate();
        }
    }

    public LocalResult getResult(){
        return result;
    }
}
