package com.madhouse.dsp;

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
                    String rowkey = line.toLowerCase() + ((line.contains("-")) ? iosPostfix : andPostfix);
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
        int threadnum = 10;
        String no = "10000";
        String tableName = "label_test_new";
        String quorum = "10.10.66.55,10.10.66.56,10.10.66.57";
        int timeout = 50;
        System.out.println("##### args.length = " + args.length);
        if (args.length < 5) {
            System.out.println("#####the args.lenth < 5");
            System.exit(1);
        }
        if(Integer.parseInt(args[0])>0){
            threadnum = Integer.parseInt(args[0]);
        }
        if(!"".equalsIgnoreCase(args[1])){
            no = args[1];
        }
        if(!"".equalsIgnoreCase(args[2])){
            tableName = args[2];
        }
        if(!"".equalsIgnoreCase(args[3])){
            quorum = args[3];
        }
        if(Integer.parseInt(args[4])>0){
            timeout = Integer.parseInt(args[4]);
        }

        final HTableUtils htable = new HTableUtils(quorum, "2181", "/hbase", timeout);
        System.out.println("##### htable get finished...");

        String filePath = "/home/wanghaishen/apps/hbase2client/id" + no + ".csv";

        final ArrayList<String> records = getByteArrayFromFile(filePath);

        log.info("#####start to get by one rowkey...");
        ExecutorService exec = Executors.newFixedThreadPool(threadnum);
        for (int i = 0; i < records.size(); i++) {
            final int finalI = i;
            final String finalTableName = tableName;
            Runnable run = new Runnable() {
                public void run() {
                    try {
                        long st = System.currentTimeMillis();
                        htable.getAllTagsByTarget(finalTableName, records.get(finalI));
                        log.info("#####get records by one rowkey in client, time costing " + (System.currentTimeMillis() - st) + "ms");
                        Thread.sleep(10);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            exec.execute(run);
        }
        exec.shutdown();
        log.info("#####get by one rowkey end...");


        for(int j=0; j< records.size(); j++){
            Map<String, List<String>> a = htable.getAllTagsByTarget(tableName, records.get(j));
            log.info("=============for rowkey: " + records.get(1));
            for (String key : a.keySet()) {
                log.info("=====key = " + key + ", value = " + a.get(key).toString());
            }
        }

        htable.closeTable();
        log.info("#####finished.............");
    }
}
