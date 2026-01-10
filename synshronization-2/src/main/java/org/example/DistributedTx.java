package org.example;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;

public abstract class DistributedTx implements Watcher {
    public static final String VOTE_COMMIT = "vote_commit";
    public static final String VOTE_ABORT = "vote_abort";
    public static final String GLOBAL_COMMIT = "final_commit";
    public static final String GLOBAL_ABORT = "final_abort";

    static String zookeeperUrl;
    DistributedTxListner listener;
    String currentTransaction;
    ZooKeeperClient client;

    public DistributedTx(DistributedTxListner listner){
        this.listener = listner;
    }

    public static void setZookeeperUrl(String zookeeperUrl) {
        DistributedTx.zookeeperUrl = zookeeperUrl;
    }

    public void start (String transactionId, String participantId) throws IOException {
        client = new ZooKeeperClient(zookeeperUrl,5000,this);
        onStartTransaction(transactionId,participantId);
    }
    abstract void onStartTransaction(String transactionId, String participantId);
    public void process(WatchedEvent watchedEvent){

    }


}
