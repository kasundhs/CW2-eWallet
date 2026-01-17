package org.example;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class DistributedTxCoordinator extends DistributedTx {

    public DistributedTxCoordinator(DistributedTxListner listener) {
        super(listener);
    }

    @Override
    void onStartTransaction(String transactionId, String randomId) {
        try {
            currentTransaction = "/" + transactionId;

            // Check if node already exists from a previous transaction
            if (client.CheckExists(currentTransaction)) {
                System.out.println("Transaction node already exists. Deleting old node: " + currentTransaction);
                client.forceDelete(currentTransaction);
            }

            client.createNode(currentTransaction, true, CreateMode.PERSISTENT, "".getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean perform() throws KeeperException, InterruptedException {
        List<String> childNodePaths = client.getChildrenNodePaths(currentTransaction);
        boolean result = true;
        byte[] data;
        System.out.println("Child Count : " + childNodePaths.size());
        for (String path : childNodePaths) {
            path = currentTransaction + "/" + path;
            System.out.println("Path : " + path);
            data = client.getData(path, false);
            String dataString = new String(data);
            if (!VOTE_COMMIT.equals(dataString)) {
                System.out.println("Child Path : " + path + " caused to transaction abort. Sending GLOBAL_ABORT");
                sendGlobalAbort();
                return (false);
            }
        }
        System.out.println("All nodes are ready to commit. Sending GLOBAL_COMMIT");
        sendGlobalCommit();
        reset();
        return result;
    }

    public void sendGlobalCommit() throws InterruptedException, KeeperException {
        if (currentTransaction != null) {
            System.out.println("Send Global commit for Transaction : " + currentTransaction);
            client.write(currentTransaction, DistributedTxCoordinator.GLOBAL_COMMIT.getBytes(StandardCharsets.UTF_8));
            listener.onGlobalCommit();
        }
    }

    public void sendGlobalAbort() throws InterruptedException, KeeperException {
        if (currentTransaction != null) {
            System.out.println("Send Global abort for Transaction : " + currentTransaction);
            client.write(currentTransaction, DistributedTxCoordinator.GLOBAL_ABORT.getBytes(StandardCharsets.UTF_8));
            listener.onGlobalAbort();
        }
    }

    private void reset() throws InterruptedException, KeeperException {
        client.forceDelete(currentTransaction);
        currentTransaction = null;
    }

}
