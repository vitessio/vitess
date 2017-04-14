package io.vitess.util;

import io.vitess.client.Context;
import io.vitess.proto.Vtrpc;
import org.joda.time.Duration;

/**
 * Created by naveen.nahata on 24/02/16.
 */
public class CommonUtils {

    /**
     * Create context used to create grpc client and executing query.
     *
     * @param username
     * @param connectionTimeout
     * @return
     */
    public static Context createContext(String username, long connectionTimeout) {
        Context context;
        Vtrpc.CallerID callerID = null;
        if (null != username) {
            callerID = Vtrpc.CallerID.newBuilder().setPrincipal(username).build();
        }
        if (null != callerID) {
            context = Context.getDefault().withDeadlineAfter(Duration.millis(connectionTimeout))
                .withCallerId(callerID);
        } else {
            context = Context.getDefault().withDeadlineAfter(Duration.millis(connectionTimeout));
        }
        return context;
    }

}

