package org.example;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

public class DistributedTxParticipant extends DistributedTx implements Watcher {
    private static final String PARTICIANT_PREFIX = "/txp_";
    private String transactionRoot;
    private final AtomicBoolean snapshotReady;

    public DistributedTxParticipant(DistributedTxListner listener, AtomicBoolean snapshotReady){
        super(listener);
        this.snapshotReady = snapshotReady;
    }

    @Override
    void onStartTransaction(String transactionId, String participantId) {
        try{
            transactionRoot = "/"+ transactionId;
            // System.out.println("DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD");
            currentTransaction = transactionRoot + PARTICIANT_PREFIX + participantId;
            client.createNode(currentTransaction,true, CreateMode.EPHEMERAL,"".getBytes(StandardCharsets.UTF_8));
            client.addWatch(transactionRoot);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public void voteCommit(){
        try{
            if(currentTransaction != null){
                System.out.println("Voting to commit the transaction : "+currentTransaction);
                client.write(currentTransaction,DistributedTx.VOTE_COMMIT.getBytes(StandardCharsets.UTF_8));
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    public void voteAbort(){
        try{
            if(currentTransaction != null){
                System.out.println("Voting to commit the transaction : "+currentTransaction);
                client.write(currentTransaction,DistributedTx.VOTE_ABORT.getBytes(StandardCharsets.UTF_8));
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    private void rest(){
        currentTransaction = null;
        transactionRoot = null;
    }
    private void handleRootDataChange() {
        try {
            byte[] data = client.getData(transactionRoot, true);
            String datastring = new String(data);
            if (DistributedTx.GLOBAL_COMMIT.equals(datastring)) {
                listener.onGlobalCommit();
            } else if (DistributedTx.GLOBAL_ABORT.equals(datastring)) {
                listener.onGlobalAbort();
            } else {
                System.out.println("Unknown data change in root : " + datastring);
            }
        }
        catch (KeeperException.NoNodeException e) {
            System.out.println("Transaction root does not exist yet: " + transactionRoot);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (transactionRoot == null) return;
        if (!snapshotReady.get()) return;

        String path = event.getPath();
        Event.EventType type = event.getType();
        if (Event.EventType.NodeDataChanged == type && transactionRoot.equals(path)) {
            handleRootDataChange();
        }
        if (Event.EventType.NodeDeleted == type && transactionRoot.equals(path)) {
            rest();
        }
    }
}
