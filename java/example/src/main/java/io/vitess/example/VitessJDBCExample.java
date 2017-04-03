package io.vitess.example;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import org.joda.time.Instant;

/**
 * VitessJDBCExample.java is a sample for using the Vitess JDBC driver.
 *
 * Before running this, start up a local example cluster as described in the
 * examples/local/README.md file.
 */
public class VitessJDBCExample {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("usage: VitessJDBCExample <vtgate-host:port>");
            System.exit(1);
        }

        // Connect to vtgate.
        String dbURL = "jdbc:vitess://" + args[0];
        try (Connection conn = DriverManager.getConnection(dbURL, null)) {
            // Setting AutoCommit to false as VTTablet was not up with enable-autocommit
            // Not Required if enable-autocommit flag is set in VTTablet
            conn.setAutoCommit(false);

            // Insert some messages on random pages.
            System.out.println("Inserting into master...");
            insertData(conn);

            // To Commit Open Transaction
            conn.commit();

            // Read it back from master.
            System.out.println("Reading from master...");
            readData(conn);

            // To Commit Open Transaction,
            // as select was made on master with autocommit false a transaction was open
            conn.commit();

            // Read it back from replica.
            dbURL += "/test_keyspace?TABLET_TYPE=replica";
            try (Connection connReplica = DriverManager.getConnection(dbURL, null)) {
                System.out.println("Reading from replica...");
                readData(connReplica);
            }

            // Execute DML Queries in a Batch
            batchedQueries(conn);

            // To Commit Open Transaction
            conn.commit();

        } catch (Exception e) {
            System.out.println("Vitess JDBC example failed.");
            System.out.println("Error Details:");
            e.printStackTrace();
            System.exit(2);
        }
    }

    private static void insertData(Connection conn) throws SQLException {
        Random rand = new Random();
        try (PreparedStatement stmt = conn.prepareStatement(
            "INSERT INTO messages (page,time_created_ns,message) VALUES (?,?,?)")) {
            for (int i = 0; i < 3; i++) {
                Instant timeCreated = Instant.now();
                int page = rand.nextInt(100) + 1;
                stmt.setInt(1, page);
                stmt.setLong(2, timeCreated.getMillis() * 1000000);
                stmt.setString(3, "V is for speed");
                stmt.execute();
            }
        }
    }

    private static void readData(Connection conn) throws SQLException {
        String sql = "SELECT page, time_created_ns, message FROM messages";
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                long page = rs.getLong("page");
                long timeCreated = rs.getLong("time_created_ns");
                String message = rs.getString("message");
                System.out.format("(%s, %s, %s)\n", page, timeCreated, message);
            }
        }
    }

    private static void batchedQueries(Connection conn) throws SQLException {
        Random rand = new Random();
        try (PreparedStatement stmt = conn.prepareStatement(
            "INSERT INTO messages (page,time_created_ns,message) VALUES (?,?,?)")) {
            for (int i = 0; i < 3; i++) {
                Instant timeCreated = Instant.now();
                int page = rand.nextInt(100) + 1;
                stmt.setInt(1, page);
                stmt.setLong(2, timeCreated.getMillis() * 1000000);
                stmt.setString(3, "V is for speed");
                stmt.addBatch();
            }
            int[] updateCounts;
            try {
                updateCounts = stmt.executeBatch();
            } catch (BatchUpdateException ex) {
                updateCounts = ex.getUpdateCounts();
            }
            if (null != updateCounts) {
                evalBatchResult(updateCounts);
            }
        }

        try (Statement stmt = conn.createStatement()) {
            Instant timeCreated = Instant.now();
            int page = rand.nextInt(100) + 1;
            System.out.println("Page selected for all dml operation: " + page);
            stmt.addBatch(
                "INSERT INTO messages (page,time_created_ns,message) VALUES (" + page + ","
                    + timeCreated.getMillis() * 1000000 + ",'V is for speed')");
            stmt.addBatch(
                "UPDATE messages set message = 'V Batch is for more speed' where page = " + page);
            stmt.addBatch("DELETE FROM messages where page = " + page);
            int[] updateCounts;
            try {
                updateCounts = stmt.executeBatch();
            } catch (BatchUpdateException ex) {
                updateCounts = ex.getUpdateCounts();
            }
            if (null != updateCounts) {
                evalBatchResult(updateCounts);
            }
        }

    }

    private static void evalBatchResult(int[] updateCounts) {
        for (int i = 0; i < updateCounts.length; i++) {
            switch (updateCounts[i]) {
                case Statement.EXECUTE_FAILED:
                    System.out.println("execution failed");
                    break;
                case Statement.SUCCESS_NO_INFO:
                    System.out.println("execution success with no result");
                    break;
                default:
                    System.out.println("execution success with rows changed: " + updateCounts[i]);
            }
        }
    }
}
