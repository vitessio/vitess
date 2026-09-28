## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
    - **[New command line flags and behavior](#new-flag)**
        - [VTGate flag `--grpc-send-session-in-streaming`](#new-vtgate-streaming-sesion)

## <a id="major-changes"/>Major Changes

### <a id="new-flag"/>New command line flags and behavior

#### <a id="new-vtgate-streaming-sesion"/>VTGate GRPC stream execute session flag `--grpc-send-session-in-streaming`

This flag enables transaction support on `StreamExecute` api.
One enabled, VTGate `StreamExecute` grpc api will send session as the last packet in the response.
The client should enable it only when they have made the required changes to expect such a packet.

It is disabled by default from v17.0.3.

This was a breaking change when v17.0.0 was released was causing upgrade issue for client 
who relied on a certain behaviour of receiving streaming packets on `StreamExecute` call.