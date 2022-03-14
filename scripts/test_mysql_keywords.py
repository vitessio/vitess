# Open list of reserved and non-reserved keywords
with open("reserved_keywords.txt", "r") as f:
    reserved_keywords = f.read().split(',')
with open("non_reserved_keywords.txt", "r") as f:
    non_reserved_keywords = f.read().split(',')

# Print useful stats
print("Number of Reserved Keywords:", len(reserved_keywords))
print("Number of Non-Reserved Keywords:", len(non_reserved_keywords))
print("Total keywords:", len(reserved_keywords) + len(non_reserved_keywords))

# TODO: Pretty sure this script will only work if you set up a MySQL server the same way I have
# Also, I was lazy and didn't save results to a file, but pretty sure conclusion
# is that all RESERVED keywords can't be used for aliasing, inserting, creating, etc.
# but UNRESERVED keywords can.

# Connect to MySQL server and see how keywords behave
import mysql.connector
from mysql.connector import errorcode
conn = mysql.connector.connect(user="root", password="root", host="127.0.0.1", database="test_db")
cursor = conn.cursor()
queries = ["SELECT 1 AS {0}", "INSERT INTO t ({0})", "DELETE FROM t WHERE {0}=1", "UPDATE t SET {0}=1", "CREATE TABLE t({0} int)"]

print("Running tests for Reserved Keywords")
for word in reserved_keywords:
    print("Keyword:", word)
    for i in range(len(queries)):
        query = queries[i].format(word)
        print("\tQuery:", query, "Result: ", end="")
        try:
            cursor.execute(query)
            print("PASS")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_PARSE_ERROR:
                print("FAIL")
            else:
                print("PASS")

print("Running tests for Reserved Keywords")
for word in non_reserved_keywords:
    print("Keyword:", word)
    for i in range(len(queries)):
        query = queries[i].format(word)
        print("\tQuery:", query, "Result: ", end="")
        try:
            cursor.execute(query)
            print("PASS")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_PARSE_ERROR:
                print("FAIL")
            else:
                print("PASS")

# Close connection to MySQL server
conn.close()