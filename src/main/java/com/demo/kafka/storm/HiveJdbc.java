package com.demo.kafka.storm;

import java.sql.*;

/**
 * Created by afsar.khan on 12/28/17.
 */
public class HiveJdbc {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    /**
     * @param args
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        //replace "hive" here with the name of the user the queries should run as
        Connection con = DriverManager.getConnection("jdbc:hive2://172.16.149.158:10000/default", "hive", "");
        Statement stmt = con.createStatement();
        String tableName = "you_hive_Table_name";
        String selectQuery = "SELECT * from "+ tableName;
        ResultSet res = stmt.executeQuery(selectQuery);
        //stmt.execute("create table " + tableName_name + " (key int, value string)");
        // show tables
        // String sql = "show tables '" + tableName + "'";
        //String sql = ("show tables");
        //ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }
    }
}
