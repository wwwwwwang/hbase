package com.madhouse.dsp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Hbase Connection Pool
 *
 * @author Madhouse
 * @date 2018/01/30
 */
public class ConnectionPool {
    private Logger log = LoggerFactory.getLogger(ConnectionPool.class);

    private static Connection connection = null;

    private static class ConnectionPoolHolder {
        private static ConnectionPool instance = new ConnectionPool();
    }

    private ConnectionPool() {
    }

    static ConnectionPool getInstance() {
        return ConnectionPoolHolder.instance;
    }

    private Connection createConnection(String quorum, String clientPort, String parent, int time) {
        ExecutorService pool = Executors.newCachedThreadPool();
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", quorum);
        conf.set("hbase.zookeeper.property.clientPort", clientPort);
        conf.set("zookeeper.znode.parent", parent);

        conf.set("hbase.client.ipc.pool.type", "RoundRobin");
        conf.set("hbase.client.ipc.pool.size", "5000");
        conf.set("ipc.server.tcpnodelay", "true");
        conf.set("ipc.client.tcpnodelay", "true");

        conf.set("hbase.regionserver.handler.count", "100");

        conf.setInt("hbase.rpc.timeout", time);
        conf.setInt("ipc.socket.timeout", time);
        conf.setInt("hbase.client.operation.timeout", time);

        conf.set("hbase.client.pause", "10");
        conf.set("hbase.client.retries.number", "1");

        conf.set("zookeeper.recovery.retry", "0");
        conf.setInt("zookeeper.session.timeout", time * 2);

        log.info("#####hbase.zookeeper.quorum = " + quorum +
                "\nhbase.zookeeper.property.clientPort = " + clientPort +
                "\nzookeeper.znode.parent = " + parent +
                "\nhbase.rpc.timeout = " + time +
                "\nipc.socket.timeout = " + time +
                "\nhbase.client.operation.timeout = " + time +
                "\nzookeeper.session.timeout = " + time * 2);
        try {
            connection = ConnectionFactory.createConnection(conf, pool);
        } catch (IOException e) {
            log.error("hbase connection created failed!!\n"+e);
            e.printStackTrace();
        }
        return connection;
    }

    public Connection getConnection(String quorum, String clientPort, String parent) {
        return getConnection(quorum, clientPort, parent, 20);
    }

    public Connection getConnection(String quorum, String clientPort, String parent, int time) {
        if (connection != null) {
            return connection;
        } else {
            connection = createConnection(quorum, clientPort, parent, time);
            return connection;
        }
    }

}
