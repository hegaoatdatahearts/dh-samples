package com.datahearts.dbsuport;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by gaohe on 16/9/13.
 */
public class MysqlSQLJDBC {
    public static void main(String args[]) {
        Connection c = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            c = DriverManager
                    .getConnection("jdbc:mysql://172.16.1.36/sltest",
                            "root", "12345678");
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
