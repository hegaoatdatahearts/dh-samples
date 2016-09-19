package com.datahearts.dbsuport;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.TemporaryFailureException;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by gaohe on 16/9/13.
 */
public class GetFromPostgreSQLTest {

    private final static String DB_DRIVER = "org.postgresql.Driver";


    String dbaseServer = "";
    String dbaseBucket = "";

    String serverIp = "localhost";
    String port = "5433";
    String sid = "gtest";
    String userName = "postgres";
    String password = "123456";
    Connection conn = null;
    String dbasePrefix = "";

    String sqlStr = "select * from table1 ";

    public GetFromPostgreSQLTest() {
        // adds the resources for overriding default configuration
        Configuration.addDefaultResource("postgresql-setting.xml");

        dbaseServer = new Configuration().get("dbase.server.ip", "localhost");
        dbaseBucket = new Configuration().get("dbase.bucket.name", "default");


        serverIp = new Configuration().get("dbserver.ip", "");
        System.out.println(new Configuration().get("dbserver.ip", ""));

        port = new Configuration().get("dbserver.port", "1521");
        System.out.println(new Configuration().get("dbserver.port", ""));

        sid = new Configuration().get("dbserver.sid", "db20");
        System.out.println(new Configuration().get("dbserver.sid", ""));

        userName = new Configuration().get("db.user", "shulie");
        System.out.println(new Configuration().get("db.user", ""));

        password = new Configuration().get("db.password", "shulie");
        System.out.println(new Configuration().get("db.password", ""));

        sqlStr = new Configuration().get("sql", "select * from dual");
        System.out.println(new Configuration().get("sql", ""));

        dbasePrefix = new Configuration().get("dbase.prefix", "sample::");
        System.out.println(new Configuration().get("dbase.prefix", ""));
    }

    public void initPostgresConn() {
        /** Postgres数据库连接URL */
        String postgresUrl = "jdbc:postgresql://" + serverIp + ":" + port + "/" + sid;
        /** Postgres数据库连接驱动 */
        try {
            Class.forName(DB_DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Where is your Postgres JDBC Driver?");
            e.printStackTrace();
            return;
        }
        /*** 获取数据库连接 */
        try {
            conn = DriverManager.getConnection(postgresUrl, userName, password);
        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;
        }
    }

    public void closePostgreConn() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


    public ResultSet getTableData() {
        ResultSet ret = null;

        PreparedStatement psObject = null;
        try {
//            psObject = oracleConn.prepareStatement(sqlStr);

            Statement st = conn.createStatement();

            ret = st.executeQuery(sqlStr);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return ret;
    }

    public List<JsonDocument> doResultSetToJson(ResultSet resultSet) throws IOException {
        List<JsonDocument> ret = new ArrayList<JsonDocument>();
        try {
            // json数组

            // 获取列数
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            System.out.println("columnCount is " + columnCount);

            long t = 1;
            AsyncCouchbaseList asyncCouchbaseList = new AsyncCouchbaseList();
            asyncCouchbaseList.initClient(dbaseServer, dbaseBucket);

            while (resultSet.next()) {
                System.out.println("While start");
                JsonObject jsonObj = JsonObject.empty();
                // 遍历每一列
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    System.out.println("columnName is " + columnName);

                    String value = resultSet.getString(columnName);
                    System.out.println("value is " + value);

                    jsonObj = jsonObj.put(columnName, value);
                }
                System.out.println("jsonObj content is " + jsonObj.toString());
                JsonDocument jsonDocument = JsonDocument.create(dbasePrefix + t, jsonObj);
//                ret.add(jsonDocument);

                for (int p = 0; p < 10; p++) {
                    try {
                        asyncCouchbaseList.dbaseBucket.upsert(jsonDocument);
                        System.out.println("DocId is " + jsonDocument.id() + " upsert " + p + " time is success!");
                        break;
                    } catch (TemporaryFailureException ex) {
                        exponentialBackoff(5);
                    } catch (RuntimeException ex) {
                        exponentialBackoff(5);
                    }
                }
                t++;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }


        return ret;

    }

    public static void main(String[] args) throws IOException, URISyntaxException {
        GetFromPostgreSQLTest getFromOracleTest = new GetFromPostgreSQLTest();
        AsyncCouchbaseList asyncCouchbaseList = new AsyncCouchbaseList();

        getFromOracleTest.initPostgresConn();

        ResultSet resultSet = getFromOracleTest.getTableData();
        List<JsonDocument> jsonDocumentList = getFromOracleTest.doResultSetToJson(resultSet);

        System.out.println("jsonDocumentList.size() is " + jsonDocumentList.size());

        getFromOracleTest.closePostgreConn();

    }

    public static long getWaitTimeExp(int retryCount) {
        double waitTime = Math.pow(2, retryCount) * 100;
        return (long) Math.min(waitTime, 2000);
    }

    // Backoff before retry
    public static void exponentialBackoff(int iteration) throws IOException {
        long waitTime = getWaitTimeExp(iteration);
        try {
            Thread.sleep(waitTime);
        } catch (InterruptedException e) {
            throw new IOException("got interrupted");
        }
    }
}
