package com.madhouse.dsp;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import org.apache.hadoop.hbase.TableName;
//import org.jruby.RubyProcess;

/**
 * Created by Madhouse on 2017/10/17.
 */
public class HTableUtils {
    private Logger log = LoggerFactory.getLogger(HTableUtils.class);
    private HTable table = null;
    private String hbase_ip;
    private String hbase_port;
    private String hbase_parent;
    private int timeout = 20;

    public HTableUtils(String quorum, String port, String parent, int time) {
        this.hbase_ip = quorum;
        this.hbase_port = port;
        this.hbase_parent = parent;
        this.timeout = time;
    }

    public HTableUtils(String quorum, String port, String parent) {
        this.hbase_ip = quorum;
        this.hbase_port = port;
        this.hbase_parent = parent;
    }

    /*public HTableUtils(String quorum, String port, String parent, int time, String tableName) {
        this.hbase_ip = quorum;
        this.hbase_port = port;
        this.hbase_parent = parent;
        this.timeout = time;
        table = getTable(tableName);
    }

    public HTableUtils(String quorum, String port, String parent, String tableName) {
        this.hbase_ip = quorum;
        this.hbase_port = port;
        this.hbase_parent = parent;
        table = getTable(tableName);
    }*/


    public HTable getTable(String tableName) {
        String tname = "";
        if ("".equalsIgnoreCase(tableName)) {
            tname = "maddsp_multiplefusion_data";
        } else {
            tname = tableName;
        }
        try {
            table = (HTable) ConnectionPool.getInstance().getConnection(hbase_ip, hbase_port, hbase_parent, timeout).getTable(TableName.valueOf(tname));
        } catch (IOException e) {
            System.out.println("#####connection created failed.." + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        return table;
    }

    /*public Map<String, List<String>> getAllTagsByTargetNew(String rowKey, String qualifier) {
        //table = getTable(tableName);
        Map<String, List<String>> r = new HashMap<String, List<String>>();
        *//*madhouse：列簇：cf，列名:mh_*
         talkingdata:列簇：cf,列名:td_*
		 unionpay-银联：列簇：cf,列名:up_*
		 admaster-admaster：列簇：cf,列名:am_*
		 qianxun-千寻：列簇：cf,列名:qx_*
		  科大-：列簇：cf,列名:kd_*
		 数据表：maddsp_threedata_merge*//*
        List<String> up = new ArrayList<String>();
        List<String> md = new ArrayList<String>();
        List<String> td = new ArrayList<String>();
        List<String> am = new ArrayList<String>();
        List<String> qx = new ArrayList<String>();
        List<String> kd = new ArrayList<String>();
        List<String> sz = new ArrayList<String>();
        List<String> ap = new ArrayList<String>();
        Result result = null;
        Get get = new Get(Bytes.toBytes(rowKey));

        get.setCacheBlocks(true);
        if (!"".equalsIgnoreCase(qualifier)) {
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(qualifier));
        }
        try {
            get.readVersions(1);
            result = table.get(get);
        } catch (IOException e) {
            System.out.println("####getting by one rowkey failed" + e.getMessage());
            e.printStackTrace();
        }
        if (result != null && result.size() > 0) {
            //System.out.println("####result.size = " + result.size());
            for (Cell cell : result.listCells()) {
                //String key = new String(cell.getQualifierArray());
                String key = new String(CellUtil.cloneQualifier(cell));
                String[] d = key.split("_");
                String who = d[0];
                try {
                    if ("td".equals(who)) {
                        td.add(d[1]);
                    } else if ("mh".equals(who)) {
                        md.add(d[1]);
                    } else if ("up".equals(who)) {
                        up.add(d[1]);
                    } else if ("am".equals(who)) {
                        am.add(d[1]);
                    } else if ("qx".equals(who)) {
                        qx.add(d[1]);
                    } else if ("kd".equals(who)) {
                        kd.add(d[1]);
                    } else if ("sz".equals(who)) {
                        sz.add(d[1]);
                    } else if ("ap".equals(who)) {
                        ap.add(d[1]);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        r.put("md", md);
        r.put("td", td);
        r.put("up", up);
        r.put("am", am);
        r.put("qx", qx);
        r.put("kd", kd);
        r.put("sz", sz);
        r.put("ap", ap);
        return r;
    }

    public Map<String, List<String>> getAllTagsByTarget(String rowKey) {
        return getAllTagsByTargetNew(rowKey, "");
    }*/

    public Map<String, List<String>> getAllTagsByTarget(String tableName, String rowKey, String qualifier) {
        table = getTable(tableName);
        Map<String, List<String>> r = new HashMap<String, List<String>>();
        /*madhouse：列簇：cf，列名:mh_*
         talkingdata:列簇：cf,列名:td_*
		 unionpay-银联：列簇：cf,列名:up_*
		 admaster-admaster：列簇：cf,列名:am_*
		 qianxun-千寻：列簇：cf,列名:qx_*
		  科大-：列簇：cf,列名:kd_*
		 数据表：maddsp_threedata_merge*/
        List<String> up = new ArrayList<String>();
        List<String> md = new ArrayList<String>();
        List<String> td = new ArrayList<String>();
        List<String> am = new ArrayList<String>();
        List<String> qx = new ArrayList<String>();
        List<String> kd = new ArrayList<String>();
        List<String> sz = new ArrayList<String>();
        List<String> ap = new ArrayList<String>();
        Result result = null;
        Get get = new Get(Bytes.toBytes(rowKey));

        get.setCacheBlocks(true);
        if (!"".equalsIgnoreCase(qualifier)) {
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(qualifier));
        }
        try {
            get.readVersions(1);
            result = table.get(get);
        } catch (IOException e) {
            System.out.println("####getting by one rowkey failed" + e.getMessage());
            e.printStackTrace();
        }
        if (result != null && result.size() > 0) {
            //System.out.println("####result.size = " + result.size());
            for (Cell cell : result.listCells()) {
                //String key = new String(cell.getQualifierArray());
                String key = new String(CellUtil.cloneQualifier(cell));
                String[] d = key.split("_");
                String who = d[0];
                try {
                    if ("td".equals(who)) {
                        td.add(d[1]);
                    } else if ("mh".equals(who)) {
                        md.add(d[1]);
                    } else if ("up".equals(who)) {
                        up.add(d[1]);
                    } else if ("am".equals(who)) {
                        am.add(d[1]);
                    } else if ("qx".equals(who)) {
                        qx.add(d[1]);
                    } else if ("kd".equals(who)) {
                        kd.add(d[1]);
                    } else if ("sz".equals(who)) {
                        sz.add(d[1]);
                    } else if ("ap".equals(who)) {
                        ap.add(d[1]);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        r.put("md", md);
        r.put("td", td);
        r.put("up", up);
        r.put("am", am);
        r.put("qx", qx);
        r.put("kd", kd);
        r.put("sz", sz);
        r.put("ap", ap);
        return r;
    }

    public Map<String, List<String>> getAllTagsByTarget(String tableName, String rowKey) {
        return getAllTagsByTarget(tableName, rowKey, "");
    }

    public void closeTable() {
        try {
            if (table != null) {
                table.close();
            }
        } catch (IOException e) {
            System.out.println("#####table close failed" + e.getMessage());
            e.printStackTrace();
        }
    }
}
