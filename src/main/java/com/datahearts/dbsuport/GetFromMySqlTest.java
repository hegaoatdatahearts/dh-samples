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
public class GetFromMySqlTest {

    private final static String DB_DRIVER = "com.mysql.jdbc.Driver";
    //    orale url=jdbc:oracle:thin:@172.16.1.35:1521:db20
    String dbaseServer = "";
    String dbaseBucket = "";

    String serverIp = "172.16.1.36";
    //    String port = "1521";
    String sid = "sltest";
    String userName = "root";
    String password = "12345678";
    Connection conn = null;
    String dbasePrefix = "";

    String sqlStr = "select * from t1_utf8";

    public GetFromMySqlTest() {
        // adds the resources for overriding default configuration
        Configuration.addDefaultResource("mysql-setting.xml");
//        DBLoadConfiguration dbconf = new DBLoadConfiguration();

        dbaseServer = new Configuration().get("dbase.server.ip", "localhost");
        dbaseBucket = new Configuration().get("dbase.bucket.name", "default");

        serverIp = new Configuration().get("dbserver.ip", "localhost");
        sid = new Configuration().get("dbserver.sid", "db20");
        userName = new Configuration().get("db.user", "shulie");
        password = new Configuration().get("db.password", "shulie");

        sqlStr = new Configuration().get("sql", "select * from dual");
        dbasePrefix = new Configuration().get("dbase.prefix", "sample::");
    }


    public void initOracleConn() {

        /** mysql数据库连接URL */
        String oracleUrl = "jdbc:mysql://" + serverIp + "/" + sid;

        /** mysql数据库连接驱动 */
        try {
            Class.forName(DB_DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Where is your Oracle JDBC Driver?");
            e.printStackTrace();
            return;
        }
        /*** 获取数据库连接 */
        try {
            conn = DriverManager.getConnection(oracleUrl, userName, password);
        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;
        }
    }

    public void closeOracleConn() {
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

            Statement st = conn.createStatement();

            ret = st.executeQuery(sqlStr);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return ret;
    }

    public List<JsonDocument> doResultSetToJson(ResultSet resultSet) throws IOException{
        List<JsonDocument> ret = new ArrayList<JsonDocument>();
        try {
            // json数组

            // 获取列数
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            long t = 1;
            AsyncCouchbaseList asyncCouchbaseList = new AsyncCouchbaseList();
            asyncCouchbaseList.initClient(dbaseServer, dbaseBucket);

            while (resultSet.next()) {
                JsonObject jsonObj = JsonObject.empty();
                // 遍历每一列
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    String value = resultSet.getString(columnName);
                    jsonObj = jsonObj.put(columnName, value);
                }
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
        GetFromMySqlTest getFromOracleTest = new GetFromMySqlTest();
        AsyncCouchbaseList asyncCouchbaseList = new AsyncCouchbaseList();

        getFromOracleTest.initOracleConn();

        ResultSet resultSet = getFromOracleTest.getTableData();
        List<JsonDocument> jsonDocumentList = getFromOracleTest.doResultSetToJson(resultSet);

//        if (jsonDocumentList.size() > 0) {
//            asyncCouchbaseList.initClient();
//            asyncCouchbaseList.upsertDbaseBucket(jsonDocumentList);
//        }

        getFromOracleTest.closeOracleConn();
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
