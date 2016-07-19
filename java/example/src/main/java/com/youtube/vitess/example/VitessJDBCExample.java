package com.youtube.vitess.example;

import org.joda.time.Instant;

import java.sql.*;
import java.util.Random;
import java.util.UUID;

/**
 * Created by harshit.gangal on 10/03/16.
 */
public class VitessJDBCExample {
  public static void main(String[] args) throws Exception {

    try {
      Connection polyglotConnnection = DriverManager.getConnection("jdbc:vitess://10.84.179.2:15991/shipment/vt_shipment");
      //polyglotConnnection.setAutoCommit(false);
      //PreparedStatement polyglotPreparedStatement = polyglotConnnection.prepareStatement("select * from shipment where shipmentid in (3,4,5)");
      System.out.println("Start Time:"+System.currentTimeMillis());
        for(int counter = 0 ;counter<1000000;counter++) {
          //System.out.println(counter);
        PreparedStatement vitessPreparedStatement = polyglotConnnection.prepareStatement("INSERT INTO test_binary(shardkey,randomid) values(?,?)");
        String uUid = UUID.randomUUID().toString()+counter;
        vitessPreparedStatement.setBytes(1,uUid.getBytes());
        vitessPreparedStatement.setInt(2,counter+1);
        int result =  vitessPreparedStatement.executeUpdate();

//        ResultSetMetaData polyglotResultSetMetaData = polyglotResultSet.getMetaData();
//        while (polyglotResultSet.next()){
//          for(int i=0;i<polyglotResultSetMetaData.getColumnCount();i++){
//            System.out.print(polyglotResultSet.getString(i+1));
//          }
//          System.out.println();
//        }
      }
        System.out.println("End Time:"+ System.currentTimeMillis());
    } catch (SQLException sqlException){
      System.out.println("SQL Exception");
      sqlException.printStackTrace();
    }
  }
}
