Recently added options to enable usage of an S3 appliance: Cloudian
HyperStore:
        -s3_backup_aws_endpoint <host:port> (port is required)
        -s3_backup_force_path_style=true/false
        -s3_backup_log_level <level> can be one of: LogOff, LogDebug, LogDebugWithSigning, LogDebugWithHTTPBody, LogDebugWithRequestRetries, LogDebugWithRequestErrors.  Default: LogOff

By default the s3 client will try to connect to
<path>.<region>.amazonaws.com.  Adjusting the endpoint will allow this
to be changed.

Given the way the FQDN is configured the TLS certificate may not match the
server's "base" (<region>.<end_point_address>) due to the leading <path>
so setting -s3_backup_force_path_style=true will force the s3 client to
connect to <region>.<endpoint> and then make a request using the full
path within the http calls.

-s3backup_log_level enables more verbose logging of the S3 calls.
