package com.flipkart.vitess.jdbc;

import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.logging.Logger;

/**
 * Created by harshit.gangal on 25/01/16.
 */
public class VitessWrapper implements Wrapper {

    /* Get actual class name to be printed on */
    private static Logger logger = Logger.getLogger(VitessWrapper.class.getName());

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
