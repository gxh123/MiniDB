package org.minidb.engine;

import org.minidb.connection.ConnectionInfo;

/**
 * Created by gxh on 2016/6/10.
 */
public interface SessionFactory {
    Session createSession(ConnectionInfo ci);
}
