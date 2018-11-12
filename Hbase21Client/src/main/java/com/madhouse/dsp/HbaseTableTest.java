package com.madhouse.dsp;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Hbase client test class
 *
 * @author Madhouse
 * @date 2018/01/30
 */
public class HbaseTableTest {
    private static Logger log = LoggerFactory.getLogger(HbaseTableTest.class);

    private static ArrayList<String> getByteArrayFromFile(String path) {
        File readFile = new File(path);
        InputStream in = null;
        InputStreamReader ir = null;
        BufferedReader br = null;
        ArrayList<String> res = new ArrayList<String>();

        try {
            in = new BufferedInputStream(new FileInputStream(readFile));
            ir = new InputStreamReader(in, "utf-8");
            br = new BufferedReader(ir);
            String line;
            String andPostfix = ":didmd5";
            String iosPostfix = ":ifa";
            int cnt = 0;
            while ((line = br.readLine()) != null) {
                if (line.length() > 3) {
                    //String rowkey = line.toLowerCase() + ((line.contains("-")) ? iosPostfix : andPostfix);
                    String rowkey = line.toLowerCase();
                    cnt++;
                    res.add(rowkey);
                }
            }
            System.out.println("there are " + cnt + " records in the file:" + path);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
                if (ir != null) {
                    ir.close();
                }
                if (in != null) {
                    in.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return res;
    }

    public static void main(String[] args) {
        Options opt = new Options();

        opt.addOption("h", "help", false, "help message");
        opt.addOption("n", "table_name", true, "table name in hbase");
        opt.addOption("o", "time_out", true, "time out of hbase");
        opt.addOption("p", "path", true, "the file name used to make the path of input file");
        opt.addOption("q", "quorum", true, "quorum of hbase");
        opt.addOption("t", "thread", true, "the count of threads, default: 10");

        String basePath = "/home/wanghaishen/apps/";
        int threadNum = 10;
        String path = "Hbase21client/id2.csv";
        String tableName = "label_test_new";
        String quorum = "10.10.66.55,10.10.66.56,10.10.66.57";
        int timeout = 50;

        String formatstr = "java -jar cassandraclient-1.0-SNAPSHOT-jar-with-dependencies.jar ...";
        HelpFormatter formatter = new HelpFormatter();
        PosixParser parser = new PosixParser();

        CommandLine cl = null;
        try {
            cl = parser.parse(opt, args);
        } catch (Exception e) {
            e.printStackTrace();
            formatter.printHelp(formatstr, opt);
            System.exit(1);
        }

        if (cl.hasOption("n")) {
            tableName = cl.getOptionValue("n");
        }
        if (cl.hasOption("q")) {
            quorum = cl.getOptionValue("q");
        }
        if (cl.hasOption("o")) {
            timeout = Integer.parseInt(cl.getOptionValue("o"));
        }
        if (cl.hasOption("h")) {
            formatter.printHelp(formatstr, opt);
            System.exit(0);
        }
        if (cl.hasOption("p")) {
            path = cl.getOptionValue("p");
        }
        if (cl.hasOption("t")) {
            threadNum = Integer.parseInt(cl.getOptionValue("t"));
        }

        String filePath = basePath + path;

        System.out.println("##### quorum=" + quorum +
                ", tableName = " + tableName +
                ", filePath = " + filePath +
                ", threadcount = " + threadNum +
                ", timeout = " + timeout);


        final HTableUtils htable = new HTableUtils(quorum, "2181", "/hbase", timeout);
        System.out.println("##### htable get finished...");

        final ArrayList<String> records = getByteArrayFromFile(filePath);

        System.out.println("#####start to get by one rowkey...");
        ExecutorService exec = Executors.newFixedThreadPool(threadNum);
        Long s = System.currentTimeMillis();
        for (int i = 0; i < records.size(); i++) {
            final int finalI = i;
            final String finalTableName = tableName;
            Runnable run = new Runnable() {
                public void run() {
                    try {
                        long st = System.currentTimeMillis();
                        htable.getAllTagsByTarget(finalTableName, records.get(finalI));
                        System.out.println("#####get records by one rowkey in client, time costing " + (System.currentTimeMillis() - st) + "ms");
                        //Thread.sleep(10);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            exec.execute(run);
        }
        exec.shutdown();
        System.out.println("#####get by one rowkey end...");

        try {
            while (!exec.isTerminated()) {
                Thread.sleep(200);
                //System.out.println("Executors is not terminated!");
            }
            System.out.println("#####exec end...");
        } catch (Exception e) {
            e.printStackTrace();
        }
        Long cost = (System.currentTimeMillis() - s) / 1000;
        System.out.println("#####time cost = " + cost + "!");
        /*for (String record : records) {
            System.out.println("#######=============for rowkey: " + record);
            Map<String, List<String>> a = htable.getAllTagsByTarget(tableName, record);
            for (String key : a.keySet()) {
                System.out.println("#####=====key = " + key + ", value = " + a.get(key).toString());
            }
        }*/
        htable.closeTable();
        System.out.println("#####finished.............");
    }
}
