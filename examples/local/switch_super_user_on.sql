SET @original_super_read_only=IF(@@global.super_read_only=1, 'ON', 'OFF');
SET GLOBAL super_read_only='ON';


