package com.youtube.vitess.example;

import org.joda.time.Instant;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;

/**
 * Created by harshit.gangal on 10/03/16.
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
      Random rand = new Random();
      for (int i = 0; i < 3; i++) {
        Instant timeCreated = Instant.now();
        int page = rand.nextInt(100) + 1;

        PreparedStatement stmt =
            conn.prepareStatement(
                "INSERT INTO messages (page,time_created_ns,message) VALUES (?,?,?)");
        stmt.setInt(1, page);
        stmt.setLong(2, timeCreated.getMillis() * 1000000);
        stmt.setString(3, "V is for speed");
        stmt.execute();
      }

      //To Commit Open Transaction
      conn.commit();

      // Read it back from master.
      System.out.println("Reading from master...");
      String sql = "SELECT page, time_created_ns, message FROM messages";
      try (Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery(sql)) {
        while (rs.next()) {
          long page = rs.getLong("page");
          long timeCreated = rs.getLong("time_created_ns");
          String message = rs.getString("message");
          System.out.format("(%s, %s, %s)\n", page, timeCreated, message);
        }
      }

      //To Commit Open Transaction, as select was made on master with autocommit false a transaction was open
      conn.commit();

      // Read it back from replica.
      dbURL += "/test_keyspace?TABLET_TYPE=replica";
      try (Connection connReplica = DriverManager.getConnection(dbURL, null)) {
        System.out.println("Reading from replica...");
        sql = "SELECT page, time_created_ns, message FROM messages";
        try (Statement stmt = connReplica.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
          while (rs.next()) {
            long page = rs.getLong("page");
            long timeCreated = rs.getLong("time_created_ns");
            String message = rs.getString("message");
            System.out.format("(%s, %s, %s)\n", page, timeCreated, message);
          }
        }
      }
    } catch (Exception e) {
      System.out.println("Vitess JDBC example failed.");
      System.out.println("Error Details:");
      e.printStackTrace();
      System.exit(2);
    }
  }
}
