package org.pentaho.di.lookup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Hello world!
 */
public class App {
    ExecutorService threadPool;

    private HConnection conn;

    private Configuration conf;

    private LookupCache lookupCache;

    public App() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "ubuntu-cdh-vm");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        try {
            conn = HConnectionManager.createConnection(conf);
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }

        threadPool = Executors.newFixedThreadPool(10);
        lookupCache = new FixedLookupCache(3);
    }

    public static void main(String[] args) throws InterruptedException {
        App app = new App();
        app.setCacheComparator(LookupObject.LookupHitsComparator);

        boolean keyExists = app.keyExistsGet("06.327.375.677|2012", "weblogs");
        keyExists = app.keyExistsGet("06.327.375.677|2012", "weblogs");
        keyExists = app.keyExistsGet("06.327.375.677|2012", "weblogs");
        keyExists = app.keyExistsGet("06.327.375.677|2012", "weblogs");
        keyExists = app.keyExistsGet("06.327.375.677|2012", "weblogs");
        keyExists = app.keyExistsGet("06.327.375.677|2012", "weblogs");
        Thread.sleep(2);
        keyExists = app.keyExistsGet("0.45.305.7|2012", "weblogs");
        keyExists = app.keyExistsGet("0.45.305.7|2012", "weblogs");
        keyExists = app.keyExistsGet("0.45.305.7|2012", "weblogs");
        Thread.sleep(2);
        keyExists = app.keyExistsGet("06.2.84.602|2012", "weblogs");
        keyExists = app.keyExistsGet("01.668.88.657|2012", "weblogs");

        keyExists = app.keyExistsGet("06.320.615.7|2012", "weblogs");
        keyExists = app.keyExistsGet("06.320.658.75|2012", "weblogs");
        keyExists = app.keyExistsGet("06.321.43.660|2012", "weblogs");
        keyExists = app.keyExistsGet("06.321.44.01|2012", "weblogs");
        keyExists = app.keyExistsGet("06.321.67.3|2012", "weblogs");
        keyExists = app.keyExistsGet("06.323.328.86|2012", "weblogs");
        keyExists = app.keyExistsGet("06.323.35.605|2012", "weblogs");
        keyExists = app.keyExistsGet("06.323.46.17|2012", "weblogs");
        keyExists = app.keyExistsGet("06.323.361.303|2012", "weblogs");
        System.out.println("keyExists: " + keyExists);

        app.terminate();
    }

    public boolean keyExists(String key, String tableName) {
        boolean keyExists = false;
        ResultScanner resultScanner = null;
        HTableInterface table = null;
        Scan scan = null;

        try {
            table = getHtable(tableName);

            scan = new Scan();
            scan.setStartRow(Bytes.toBytes(key));
            FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            fl.addFilter(new FirstKeyOnlyFilter());
            fl.addFilter(new KeyOnlyFilter());
            scan.setFilter(fl);

            // Don't pre-fetch more than 1 row.
            scan.setCaching(1);

            resultScanner = table.getScanner(scan);
            Result result = resultScanner.next();


            if (!result.isEmpty()) {
                if (Bytes.toString(result.getRow()).equals(key)) {
                    keyExists = true;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            resultScanner.close();

            try {
                table.close();
            } catch (IOException e) {

            }
        }

        return keyExists;
    }

    public boolean keyExistsGet(String key, String tableName) {
        boolean keyExists = false;
        HTableInterface table = null;

        Result result = null;

        LookupObject lo = (LookupObject) lookupCache.get(key);
        if (lo != null) {
            return true;
        }

        System.out.println("Cache Miss!");

        try {
            table = getHtable(tableName);

            Get g = new Get(Bytes.toBytes(key));

            /*FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            fl.addFilter(new FirstKeyOnlyFilter());
            fl.addFilter(new KeyOnlyFilter());
            g.setFilter(fl);*/

            result = table.get(g);

            if (!result.isEmpty()) {
                keyExists = true;
                lookupCache.put(key, result);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {

            }
        }

        return keyExists;
    }

    public HTableInterface getHtable(String tableName) throws IOException {
        return new HTable(Bytes.toBytes(tableName), conn, threadPool);
    }

    public void setCacheComparator(Comparator comparator) {
        lookupCache.setComparator(comparator);
    }

    public void terminate() {
        try {
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        threadPool.shutdown();
    }
}
