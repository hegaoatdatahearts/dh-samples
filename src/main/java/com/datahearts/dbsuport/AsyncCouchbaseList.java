package com.datahearts.dbsuport;

import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.datahearts.client.DHBucket;
import com.datahearts.client.DHCluster;
import com.datahearts.client.DHConfig;
import org.apache.hadoop.conf.Configuration;
import rx.Observable;
import rx.functions.Func1;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by gaohe on 16/7/29.
 */
public class AsyncCouchbaseList {

    public static DHCluster cluster;
    public static DHBucket metaBucket;
    public static DHBucket dataBucket;
    public static DHBucket dbaseBucket;
    public final ClientFinalizer clientFinalizer = new ClientFinalizer();

    static ReplicateTo UPDATE_REPLICATE_TO = ReplicateTo.NONE;
    static PersistTo UPDATE_PERSIST_TO = PersistTo.NONE;

    public class ClientFinalizer extends Thread {
        public synchronized void run() {
            shutdownClients();
        }
    }

    public static void shutdownClients() {

//        if (metaBucket != null) {
//            metaBucket.close();
//            metaBucket = null;
//        }
//        if (dataBucket != null) {
//            dataBucket.close();
//            dataBucket = null;
//        }
        if (dbaseBucket != null) {
            dbaseBucket.close();
            dbaseBucket = null;
        }
        cluster.disconnect();
    }

    boolean initClient(String dbaseServer, String dbaseBkt) {

        try {
//            DBLoadConfiguration conf = new DBLoadConfiguration();
//            String dbase_server = conf.get("dbase.server.ip", "localhost");
//            String dbase_bucket = conf.get("dbase.bucket.name", "default");
            String dbase_server = dbaseServer;
            String dbase_bucket = dbaseBkt;
            System.out.println("dbase_server is " + dbase_server);
            System.out.println("dbase_bucket is " + dbase_bucket);

            String[] connUri = new String[]{dbase_server};

            DHConfig metaBucketConfig = new DHConfig(java.util.Arrays.asList(connUri), dbase_bucket, null);
            metaBucketConfig.setEvnKvTimeout(60000);
            cluster = new DHCluster(metaBucketConfig);
            dbaseBucket = cluster.openDHBucket();


            // when jvm close shut down Cluster and buckets
            Runtime.getRuntime().addShutdownHook(clientFinalizer);

        } catch (Exception e) {
            System.out.println("Connect cluster server is failed!");
            shutdownClients();
            return false;
        }
        return true;

    }

    public boolean upsertDbaseBucket(List<JsonDocument> lstJsonDocuments) throws IOException {
        printMethodExecutionTime(0);
        try {

//            Observable.from(lstJsonDocuments).flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
//                public Observable<JsonDocument> call(final JsonDocument docToInsert) {
//
//                    System.out.println(docToInsert.content());
//                    return dbaseBucket.async().upsert(docToInsert, UPDATE_PERSIST_TO, UPDATE_REPLICATE_TO);
//                }
//            }).last().toBlocking().single();

            Observable
                    .from(lstJsonDocuments)
                    .flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
                        @Override
                        public Observable<JsonDocument> call(final JsonDocument docToInsert) {
                            return dbaseBucket.async().upsert(docToInsert);
                        }
                    })
                    .last()
                    .toBlocking()
                    .single();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("upsert Exception is " + e.getClass());
            throw new IOException(e.getMessage());
        }
        printMethodExecutionTime(1);
        return true;
    }

    public boolean insertDbaseBucket(List<JsonDocument> lstJsonDocuments) throws IOException {
        printMethodExecutionTime(0);
        try {
            Observable.from(lstJsonDocuments).flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
                @Override
                public Observable<JsonDocument> call(final JsonDocument docToInsert) {

                    System.out.println(docToInsert.content());
                    return dbaseBucket.async().insert(docToInsert, UPDATE_PERSIST_TO, UPDATE_REPLICATE_TO);
                }
            }).last().toBlocking().single();
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
        printMethodExecutionTime(1);
        return true;
    }

    public boolean replaceDbaseBucket(List<JsonDocument> lstJsonDocuments) throws IOException {
        printMethodExecutionTime(0);
        try {
            Observable.from(lstJsonDocuments).flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
                @Override
                public Observable<JsonDocument> call(final JsonDocument docToInsert) {

                    System.out.println(docToInsert.content());
                    return dbaseBucket.async().replace(docToInsert, UPDATE_PERSIST_TO, UPDATE_REPLICATE_TO);
                }
            }).last().toBlocking().single();
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
        printMethodExecutionTime(1);
        return true;
    }

    public boolean removeDbaseBucket(List<JsonDocument> lstJsonDocuments) throws IOException {
        printMethodExecutionTime(0);
        try {
            Observable.from(lstJsonDocuments).flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
                @Override
                public Observable<JsonDocument> call(final JsonDocument docToInsert) {

                    System.out.println(docToInsert.content());
                    return dbaseBucket.async().remove(docToInsert, UPDATE_PERSIST_TO, UPDATE_REPLICATE_TO);
                }
            }).last().toBlocking().single();
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
        printMethodExecutionTime(1);
        return true;
    }

    public List<JsonDocument> createJsonDocument() {

        List<JsonDocument> lstJsonDocuments = new ArrayList<JsonDocument>();

        JsonDocument jsonDocument;

        JsonObject jsonObject;

        jsonObject = JsonObject.empty().put("firstname", "Larry").put("lastname", "liu").put("job", "master").put("age",
                18);
        jsonDocument = JsonDocument.create("User::001", jsonObject);
        lstJsonDocuments.add(jsonDocument);

        jsonObject = JsonObject.empty().put("firstname", "Herry").put("lastname", "Yu").put("job", "teacher").put("age",
                17);
        jsonDocument = JsonDocument.create("User::002", jsonObject);
        lstJsonDocuments.add(jsonDocument);

        jsonObject = JsonObject.empty().put("firstname", "He").put("lastname", "Gao").put("job", "staff").put("age",
                16);
        jsonDocument = JsonDocument.create("User::003", jsonObject);
        lstJsonDocuments.add(jsonDocument);

        jsonObject = JsonObject.empty().put("firstname", "Maecheal").put("lastname", "Jordan").put("job", "MVP")
                .put("age", 15);
        jsonDocument = JsonDocument.create("User::004", jsonObject);
        lstJsonDocuments.add(jsonDocument);
        return lstJsonDocuments;

    }

    public static void main(String[] args) throws IOException, URISyntaxException {

        AsyncCouchbaseList test = new AsyncCouchbaseList();
        Configuration conf = new Configuration();
        if (test.initClient("localhost", "default")) {

            List<JsonDocument> lstJsonDocuments = test.createJsonDocument();

            // Excute file upsert ops
            if (test.upsertDbaseBucket(lstJsonDocuments)) {
                System.out.println("Excute file upsert successful!!!");
            } else {
                System.out.println("Excute file upsert failed!");
            }
//            // Excute file remove ops
//            if (test.removeDbaseBucket(lstJsonDocuments)) {
//                System.out.println("Excute file remove successful!!!");
//            } else {
//                System.out.println("Excute file remove failed!");
//            }
//            // Excute file insert ops
//            if (test.insertDbaseBucket(lstJsonDocuments)) {
//                System.out.println("Excute file insert successful!!!");
//            } else {
//                System.out.println("Excute file insert failed!");
//            }
//
//            // Excute file replace ops
//            if (test.replaceDbaseBucket(lstJsonDocuments)) {
//                System.out.println("Excute file replace successful!!!");
//            } else {
//                System.out.println("Excute file replace failed!");
//            }
//            // Excute file remove ops
//            if (test.removeDbaseBucket(lstJsonDocuments)) {
//                System.out.println("Clear Dbase OK !!!!!!");
//            } else {
//                System.out.println("Clear Dbase failed !!!!!!");
//            }
        }

    }

    /**
     * Print method execution time.
     *
     * @param flag : 0:method called start;!0:method called finish
     */
    public static long printMethodExecutionTime(int flag) {
        if (flag == 0) {
            System.out.println("Call " + Thread.currentThread().getStackTrace()[2].getMethodName() + " START time is["
                    + System.currentTimeMillis() + "]");
        } else {
            System.out.println("Call " + Thread.currentThread().getStackTrace()[2].getMethodName() + " FINISH time is["
                    + System.currentTimeMillis() + "]");
        }
        return System.currentTimeMillis();
    }
}
