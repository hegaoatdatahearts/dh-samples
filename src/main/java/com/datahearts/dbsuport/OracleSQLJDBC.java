package com.datahearts.dbsuport;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by gaohe on 16/9/13.
 */
public class OracleSQLJDBC {
    public static void main(String args[]) {
        Connection c = null;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            c = DriverManager
                    .getConnection("jdbc:oracle:thin:@172.16.1.35:1521:db20",
                            "shulie", "shulie");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }
        System.out.println("Opened database successfully");
        try {
            c.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
