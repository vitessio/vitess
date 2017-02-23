package com.youtube.vitess.client.cursor;

import com.youtube.vitess.proto.Query;
import com.youtube.vitess.proto.Vtrpc;

/**
 * Created by harshit.gangal on 22/12/16.
 */
public class CursorWithError {

    private final Cursor cursor;
    private final Vtrpc.RPCError error;

    public CursorWithError(Query.ResultWithError resultWithError) {
      if (!resultWithError.hasError() ||
          Vtrpc.Code.OK == resultWithError.getError().getCode()) {
            this.cursor = new SimpleCursor(resultWithError.getResult());
            this.error = null;
        } else {
            this.cursor = null;
            this.error = resultWithError.getError();
        }
    }

    public Cursor getCursor() {
        return cursor;
    }

    public Vtrpc.RPCError getError() {
        return error;
    }
}
