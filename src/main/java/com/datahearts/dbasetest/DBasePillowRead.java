package com.datahearts.dbasetest;

import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.JsonDocument;
import com.datahearts.client.DHBucket;
import com.datahearts.client.DHCluster;
import com.datahearts.client.DHConfig;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by gaohe on 16/7/29.
 */
public class DBasePillowRead {

	public static DHCluster cluster;
	public static DHBucket dbaseBucket;
	public final ClientFinalizer clientFinalizer = new ClientFinalizer();

	static ReplicateTo UPDATE_REPLICATE_TO = ReplicateTo.NONE;
	static PersistTo UPDATE_PERSIST_TO = PersistTo.NONE;

	String dbase_server = "localhost";
	String dbase_bucket = "default";
	static String dbase_prefix = "";
	static int dbase_key_range = 0;
	static int dbase_read_thread_count = 0;
	static CountDownLatch latch = new CountDownLatch(dbase_read_thread_count);

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
			DBPillowReadLoadConfiguration conf = new DBPillowReadLoadConfiguration();
			dbase_server = conf.get("dbase.server.ip", "localhost");
			dbase_bucket = conf.get("dbase.bucket.name", "default");
			dbase_prefix = conf.get("dbase.prefix", "");
			dbase_key_range = conf.getInt("dbase.key.range", 0);
			dbase_read_thread_count = conf.getInt("dbase.read.thread.count", 0);
			System.out.println("dbase_server is " + dbase_server);
			System.out.println("dbase_bucket is " + dbase_bucket);
			System.out.println("dbase_prefix is " + dbase_prefix);
			System.out.println("dbase_key_range is " + dbase_key_range);
			System.out.println("dbase_read_thread_count is " + dbase_read_thread_count);

			String[] connUri = new String[] { dbase_server };

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

	public static boolean pillowRead(CountDownLatch latch)  {
		
		for (int i = 1; i <= dbase_key_range; i++) {
			JsonDocument js =  dbaseBucket.get(dbase_prefix + i);
//			System.out.println("read doc is " + (dbase_prefix + i));
		}

		latch.countDown();
		
		return true;
	}

	public boolean threadread() throws IOException {
		// 同时启动1000个线程，去进行i++计算，看看实际结果
		for (int i = 0; i < dbase_read_thread_count; i++) {
			System.out.println("Thread No is " + i);
			new Thread(new Runnable() {
				@Override
				public void run() {
						DBasePillowRead.pillowRead(latch);
				}
			}).start();
		}
		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return true;
	}

	public static void main(String[] args) throws IOException, URISyntaxException {

		DBasePillowRead test = new DBasePillowRead();
		if (test.initClient()) {

			if (test.threadread()) {
				System.out.println("Excute file read successful!!!");
			} else {
				System.out.println("Excute file read failed!");
			}
		}

	}

	/**
	 * Print method execution time.
	 *
	 * @param flag
	 *            : 0:method called start;!0:method called finish
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
