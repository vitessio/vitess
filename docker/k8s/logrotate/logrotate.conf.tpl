create 660 $CREATE_USER $CREATE_GROUP
missingok
nocompress
notifempty

$AUDIT_LOG_PATH {
    size $AUDIT_LOG_SIZE
    rotate $AUDIT_LOG_ROTATE
    postrotate
        $MYSQL -e "SET GLOBAL audit_log_flush = 'ON';"
    endscript
}

$ERROR_LOG_PATH {
    size $ERROR_LOG_SIZE
    rotate $ERROR_LOG_ROTATE
    postrotate
        $MYSQL -e "FLUSH LOCAL ERROR LOGS;"
    endscript
}

$GENERAL_LOG_PATH {
    size $GENERAL_LOG_SIZE
    rotate $GENERAL_LOG_ROTATE
    postrotate
        $MYSQL -e "FLUSH LOCAL GENERAL LOGS;"
    endscript
}

$SLOW_QUERY_LOG_PATH {
    size $SLOW_QUERY_LOG_SIZE
    rotate $SLOW_QUERY_LOG_ROTATE
    postrotate
        $MYSQL -e "SELECT @@global.long_query_time INTO @lqt_save; SET GLOBAL long_query_time=2000; SELECT SLEEP(2); FLUSH LOCAL SLOW LOGS; SELECT SLEEP(2); SET GLOBAL LONG_QUERY_TIME=@lqt_save;"
    endscript
}

$WILDCARD_PATH {
    size $WILDCARD_SIZE
    rotate $WILDCARD_ROTATE
    sharedscripts
    postrotate
        $MYSQL -e "SELECT @@global.long_query_time INTO @lqt_save; SET GLOBAL long_query_time=2000; SELECT SLEEP(2); FLUSH LOGS; SELECT SLEEP(2); SET GLOBAL LONG_QUERY_TIME=@lqt_save;"
    endscript
}
