package com.flipkart.vitess.jdbc;

import com.flipkart.vitess.util.Constants;

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
        try {
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw new SQLException(
                Constants.SQLExceptionMessages.CLASS_CAST_EXCEPTION + iface.toString(), cce);
        }
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
