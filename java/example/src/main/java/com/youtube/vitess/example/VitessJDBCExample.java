package com.youtube.vitess.example;

import java.sql.*;

/**
 * Created by harshit.gangal on 10/03/16.
 */
public class VitessJDBCExample {
    private static final String dbURL = "jdbc:vitess://<vtGateHostname>:<vtGatePort>/<keyspace>/<dbName>";
    private static Connection conn;

    static {
        try {
            //With JDBC 4.1 Specs class.forname is not required.
            conn = DriverManager.getConnection(dbURL, null);
        } catch (SQLException e) {
            System.out.println("failed to initialize class");
        }
    }

    public static void main(String[] args) throws Exception {
        Statement stmt;
        stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("show databases");
        ResultSetMetaData resultSetMetaData = rs.getMetaData();
        int colCount = resultSetMetaData.getColumnCount();
        while (rs.next()) {
            for (int i = 1; i <= colCount; ++i) {
                System.out.println(resultSetMetaData.getColumnName(i) + " : " + rs.getString(i));
            }
        }
        rs.close();
        stmt.close();


        String sql = "select * from test_table where id BETWEEN ? AND ?";
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        preparedStatement.setInt(1, 1);
        preparedStatement.setInt(2, 10);

        rs = preparedStatement.executeQuery();
        resultSetMetaData = rs.getMetaData();
        colCount = resultSetMetaData.getColumnCount();
        int rowCount = 1;
        while (rs.next()) {
            System.out.println("Row ----> " + rowCount);
            for (int i = 1; i <= colCount; ++i) {
                System.out.println(resultSetMetaData.getColumnName(i) + " : " + rs.getString(i));
            }
            ++rowCount;
        }
        rs.close();
        preparedStatement.close();
        conn.close();
    }
}
