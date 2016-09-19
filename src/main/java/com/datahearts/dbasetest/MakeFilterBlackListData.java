package com.datahearts.dbasetest;

import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.datahearts.client.DHBucket;
import com.datahearts.client.DHCluster;
import com.datahearts.client.DHConfig;

import rx.Observable;
import rx.functions.Func1;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by gaohe on 16/9/18.
 */
public class MakeFilterBlackListData {

    public static DHCluster cluster;
    public static DHBucket dbaseBucket;
    public final ClientFinalizer clientFinalizer = new ClientFinalizer();

    static ReplicateTo UPDATE_REPLICATE_TO = ReplicateTo.NONE;
    static PersistTo UPDATE_PERSIST_TO = PersistTo.NONE;

    String dbase_server = "localhost";
    String dbase_bucket = "default";
    static String dbase_prefix = "";
    static int dbase_black_key_range = 0;

    static int dbase_all_key_range = 0;

    public class ClientFinalizer extends Thread {
        public synchronized void run() {
            shutdownClients();
        }
    }

    public static void shutdownClients() {

        if (dbaseBucket != null) {
            dbaseBucket.close();
            dbaseBucket = null;
        }
        cluster.disconnect();
    }

    boolean initClient() {

        try {
            DBBlackMailLoadConfiguration conf = new DBBlackMailLoadConfiguration();

            dbase_server = conf.get("dbase.server.ip", "localhost");
            dbase_bucket = conf.get("dbase.bucket.name", "default");
            dbase_black_key_range = conf.getInt("dbase.black.key.range", 0);
            dbase_all_key_range = conf.getInt("dbase.all.key.range", 0);

            System.out.println("dbase_server is " + dbase_server);
            System.out.println("dbase_bucket is " + dbase_bucket);
            System.out.println("dbase_black_key_range is " + dbase_black_key_range);
            System.out.println("dbase_all_key_range is " + dbase_all_key_range);

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

    public static void makeMailList() throws IOException {

        long start = System.currentTimeMillis();
        System.out.println("All mail list import is start!");
        System.out.println("Start time is " + start);


        MakeFilterBlackListData test = new MakeFilterBlackListData();
        String id = "";
        String from = "";
        String to = "";
        String cc = "";
        String bcc = "";
        String subject = "";
        String init_date = "";
        String from_pre = "";

        List<JsonDocument> lstJsonDocuments = new ArrayList<JsonDocument>();
        for (int i = 1; i <= dbase_all_key_range; i++) {
            id = "TT::" + String.valueOf(i);

            from_pre = (i % 3 == 0) ? "B" : ((i % 2 == 0) ? "A" : "C");
            String from_mid = (i % dbase_black_key_range == 0) ? String.valueOf(1)
                    : String.valueOf((i % dbase_black_key_range));
            from = from_pre + from_mid + "@test.com";

            to = "TO_" + i + "@test.com";
            cc = "CC_" + i + "@test.com";
            bcc = "BCC_" + i + "@test.com";
            subject = "Test Mail Subject" + i;
            init_date = (new Date()).toString();

            JsonObject content = JsonObject.empty().put("id", id).put("from", from).put("to", to).put("cc", cc)
                    .put("bcc", bcc).put("subject", subject).put("init_date", init_date);

            lstJsonDocuments.add(JsonDocument.create(id, content));
            if (lstJsonDocuments.size() == 100) {
                upsertDbaseBucket(lstJsonDocuments);
                lstJsonDocuments = new ArrayList<JsonDocument>();
            }
//			dbaseBucket.upsert(JsonDocument.create(id, content));
            if (i % 100000 == 0) {
                System.out.println("mail list " + i + " record has imported!");
            }
        }
        if (lstJsonDocuments.size() > 0) {
            upsertDbaseBucket(lstJsonDocuments);
            lstJsonDocuments = new ArrayList<JsonDocument>();
        }
        long finish = System.currentTimeMillis();

        System.out.println("end time is " + finish);

        System.out.println("run time is " + (finish - start) + " ms");

        System.out.println("All mail list is complete!");
    }

    public static void makeBlackMailList() throws IOException {

        long start = System.currentTimeMillis();
        System.out.println("Black list import is start!");
        System.out.println("Start time is " + start);



        String id = "";
        String mail_addr = "";
        String valid_flag = "";

        List<JsonDocument> lstJsonDocuments = new ArrayList<JsonDocument>();

        for (int i = 1; i <= dbase_black_key_range; i++) {
            id = String.valueOf(i);
            mail_addr = "B" + i + "@test.com";
            valid_flag = (i % 2) == 0 ? "Y" : "N";
            JsonObject content = JsonObject.empty().put("id", id).put("mail_addr", mail_addr)
                    .put("valid_flag", valid_flag).put("created_date", (new Date()).toString());

            lstJsonDocuments.add(JsonDocument.create(mail_addr, content));
            if (lstJsonDocuments.size() == 100) {
                upsertDbaseBucket(lstJsonDocuments);
                lstJsonDocuments = new ArrayList<JsonDocument>();
            }

//                dbaseBucket.upsert(JsonDocument.create(mail_addr, content));
            if (i % 100000 == 0) {
                System.out.println("black list " + i + " record has imported!");
            }

        }
        if (lstJsonDocuments.size() > 0) {
            upsertDbaseBucket(lstJsonDocuments);
            lstJsonDocuments = new ArrayList<JsonDocument>();
        }
        long finish = System.currentTimeMillis();

        System.out.println("end time is " + finish);

        System.out.println("run time is " + (finish - start) + " ms");

        System.out.println("Black list is complete!");

    }

    public static boolean upsertDbaseBucket(List<JsonDocument> lstJsonDocuments) throws IOException {
        try {
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
        return true;
    }

    public static void main(String[] args) throws IOException, URISyntaxException {

        MakeFilterBlackListData test = new MakeFilterBlackListData();
        if (test.initClient()) {
            makeBlackMailList();
            System.out.println("##################################");
            makeMailList();
        }

    }
}
