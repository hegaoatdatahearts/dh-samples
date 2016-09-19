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
public class GetFromOracleTest {

    private final static String DB_DRIVER = "oracle.jdbc.driver.OracleDriver";
//    orale url=jdbc:oracle:thin:@172.16.1.35:1521:db20

    String dbaseServer = "";
    String dbaseBucket = "";

    String serverIp = "localhost";
    String port = "1521";
    String sid = "db20";
    String userName = "shulie";
    String password = "shulie";
    String dbasePrefix = "T_10_";
    Connection conn = null;

    String sqlStr = "select * from T_10 where rownum<10 ";

    public GetFromOracleTest() {
        // adds the resources for overriding default configuration
//        Configuration.addDefaultResource("oracle-setting.xml");
        DBLoadConfiguration dbconf = new DBLoadConfiguration();
        System.out.println(dbconf.get("dbserver.ip"));
        dbaseServer = new Configuration().get("dbase.server.ip", "localhost");
        dbaseBucket = new Configuration().get("dbase.bucket.name", "default");

        serverIp = dbconf.get("dbserver.ip", "localhost");
        port = dbconf.get("dbserver.port", "1521");
        sid = dbconf.get("dbserver.sid", "db20");
        userName = dbconf.get("db.user", "shulie");
        password = dbconf.get("db.password", "shulie");

        sqlStr = dbconf.get("sql", sqlStr);
        dbasePrefix = dbconf.get("dbase.prefix", dbasePrefix);

    }


    public void initOracleConn() {
        /** Oracle数据库连接URL */
        String oracleUrl = "jdbc:oracle:thin:@" + serverIp + ":" + port + ":" + sid;
        /** Oracle数据库连接驱动 */
        try {
            Class.forName(DB_DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Where is your Oracle JDBC Driver?");
            e.printStackTrace();
            return;
        }
        /*** 获取数据库连接 */
        try {
            System.out.println("ConnectString is " + oracleUrl + userName + password);
            conn = DriverManager.getConnection(oracleUrl, userName, password);
            System.out.println("Connection successful!");
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

    public List<JsonDocument> doResultSetToJson(ResultSet resultSet) throws IOException {
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
        long start = System.currentTimeMillis();

        System.out.println("Start time is " + start);

        GetFromOracleTest getFromOracleTest = new GetFromOracleTest();
//        AsyncCouchbaseList asyncCouchbaseList = new AsyncCouchbaseList();

        getFromOracleTest.initOracleConn();

        ResultSet resultSet = getFromOracleTest.getTableData();
        List<JsonDocument> jsonDocumentList = getFromOracleTest.doResultSetToJson(resultSet);

//        if (jsonDocumentList.size() > 0) {
//            asyncCouchbaseList.initClient();
//            asyncCouchbaseList.upsertDbaseBucket(jsonDocumentList);
//        }

        getFromOracleTest.closeOracleConn();
        long finish = System.currentTimeMillis();

        System.out.println("end time is " + finish);

        System.out.println("run time is " + (finish - start) + " ms");
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
