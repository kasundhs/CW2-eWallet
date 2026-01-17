package org.example;

import ds.tutorial.communication.grpc.generated.SetCrossPartTransRequest;
import ds.tutorial.communication.grpc.generated.SetCrossPartTransResponse;
import ds.tutorial.communication.grpc.generated.SetCrossPartTransServiceGrpc;
import ds.tutorial.communication.grpc.generated.AckCrossPartTransRequest;
import ds.tutorial.communication.grpc.generated.AckCrossPartTransResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import javafx.util.Pair;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Timer;
import java.util.TimerTask;
// For cross partition transfers
public class SetCrossPartTransServiceImpl extends SetCrossPartTransServiceGrpc.SetCrossPartTransServiceImplBase
        implements DistributedTxListner {

    private BankServer server;
    private ManagedChannel channel;
    private SetCrossPartTransServiceGrpc.SetCrossPartTransServiceBlockingStub clientStub;

    private Pair<String, Double> tempDataHolder;
    private boolean transactionStatus = false;

    // Store pending debit transactions waiting for acknowledgment
    private Map<String, PendingTransaction> pendingDebits = new ConcurrentHashMap<>();
    private static final long ACK_TIMEOUT_MS = 2000; // 2 seconds timeout

    public SetCrossPartTransServiceImpl(BankServer server) {
        this.server = server;
    }

    // private class to track pending transactions
    private class PendingTransaction {
        String accountId;
        double amount;
        boolean isDebit;
        Timer timer;
        boolean isRolledBack = false;

        PendingTransaction(String accountId, double amount, boolean isDebit) {
            this.accountId = accountId;
            this.amount = amount;
            this.isDebit = isDebit;
        }
    }

    @Override
    public void setTransaction(SetCrossPartTransRequest request,
            io.grpc.stub.StreamObserver<SetCrossPartTransResponse> responseObserver) {

        String accountId = request.getAccId();
        double amount = request.getAmount();
        boolean isDebit = request.getIsDebit();
        String transactionId = UUID.randomUUID().toString();

        if (server.isLeader()) {
            // Act as primary
            try {
                System.out.println("Cross-partition transaction as Primary: " +
                        (isDebit ? "DEBIT" : "CREDIT") + " " + amount + " from/to " + accountId);

                double currentBalance = server.getAccountBalance(accountId);

                // For debit, check if sufficient balance
                if (isDebit && currentBalance < amount) {
                    System.out.println("Insufficient balance. Current: " + currentBalance + ", Required: " + amount);
                    transactionStatus = false;
                } else {
                    startDistributedTx(accountId, amount, isDebit);
                    updateSecondaryServers(accountId, amount, isDebit);
                    transactionStatus = ((DistributedTxCoordinator) server.getCrossTransferTransact()).perform();
                    System.out.println("Cross-partition transaction status: " + transactionStatus);

                    // Start timeout timer for both debit and credit operations
                    if (transactionStatus) {
                        startAckTimeout(transactionId, accountId, amount, isDebit);
                    }
                }
            } catch (Exception e) {
                System.out.println("Error in cross-partition transaction: " + e.getMessage());
                e.printStackTrace();
                transactionStatus = false;
            }
        } else {
            // Act as secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("Secondary processing cross-partition transaction from Primary");
                try {
                    startDistributedTx(accountId, amount, isDebit);

                    double currentBalance = server.getAccountBalance(accountId);

                    // Vote based on balance check for debit operations
                    if (isDebit && currentBalance < amount) {
                        ((DistributedTxParticipant) server.getCrossTransferTransact()).voteAbort();
                    } else {
                        ((DistributedTxParticipant) server.getCrossTransferTransact()).voteCommit();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                // Forward to primary
                SetCrossPartTransResponse response = callPrimary(accountId, amount, isDebit);
                transactionStatus = response.getStatus();
            }
        }

        SetCrossPartTransResponse response = SetCrossPartTransResponse
                .newBuilder()
                .setStatus(transactionStatus)
                .setTransactionId(transactionId)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    // If ack is not received
    private void startAckTimeout(String transactionId, String accountId, double amount, boolean isDebit) {
        PendingTransaction pending = new PendingTransaction(accountId, amount, isDebit);
        pendingDebits.put(transactionId, pending);

        System.out.println("Starting ACK timeout for transaction: " + transactionId +
                " (" + (isDebit ? "DEBIT" : "CREDIT") + ")");

        pending.timer = new Timer();
        pending.timer.schedule(new TimerTask() {
            @Override
            public void run() {
                PendingTransaction pendingTrans = pendingDebits.get(transactionId);
                if (pendingTrans != null && !pendingTrans.isRolledBack) {
                    System.out.println("ACK TIMEOUT: Rolling back transaction " + transactionId);
                    pendingTrans.isRolledBack = true; // avoid retry
                    performRollback(pendingTrans);
                    pendingDebits.remove(transactionId);
                }
            }
        }, ACK_TIMEOUT_MS);
    }

    private void startDistributedTx(String accountId, double amount, boolean isDebit) {
        try {
            String txId = UUID.randomUUID().toString();
            String operation = isDebit ? "debit" : "credit";
            server.getCrossTransferTransact().start(accountId + "-" + operation, txId);

            // Store the update to apply on commit
            double delta = isDebit ? -amount : amount;
            tempDataHolder = new Pair<>(accountId, delta);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void applyUpdate() {
        if (tempDataHolder != null) {
            String accountId = tempDataHolder.getKey();
            double delta = tempDataHolder.getValue();
            double currentBalance = server.getAccountBalance(accountId);
            double newBalance = currentBalance + delta;

            server.setAccountBalance(accountId, newBalance);
            System.out.println("Applied cross-partition update: Account " + accountId +
                    " | Old: " + currentBalance + " | Delta: " + delta + " | New: " + newBalance);
            tempDataHolder = null;
        }
    }

    private void clearTemp() {
        tempDataHolder = null;
    }

    private void updateSecondaryServers(String accountId, double amount, boolean isDebit)
            throws KeeperException, InterruptedException {

        List<String[]> othersData = server.getOthersData();
        for (String[] data : othersData) {
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);

            System.out.println("Updating secondary at " + IPAddress + ":" + port);
            callServer(accountId, amount, isDebit, true, IPAddress, port);
        }
    }

    private SetCrossPartTransResponse callServer(String accountId, double amount, boolean isDebit,
            boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Calling server " + IPAddress + ":" + port +
                " for " + (isDebit ? "debit" : "credit"));

        channel = ManagedChannelBuilder.forAddress(IPAddress, port)
                .usePlaintext()
                .build();

        clientStub = SetCrossPartTransServiceGrpc.newBlockingStub(channel);

        SetCrossPartTransRequest request = SetCrossPartTransRequest
                .newBuilder()
                .setAccId(accountId)
                .setAmount(amount)
                .setIsDebit(isDebit)
                .setIsSentByPrimary(isSentByPrimary)
                .build();

        SetCrossPartTransResponse response = clientStub.setTransaction(request);
        channel.shutdown();

        return response;
    }

    private SetCrossPartTransResponse callPrimary(String accountId, double amount, boolean isDebit) {
        try {
            String[] currentLeaderData = server.getCurrentLeaderData();
            String IPAddress = currentLeaderData[0];
            int port = Integer.parseInt(currentLeaderData[1]);

            System.out.println("Forwarding to primary at " + IPAddress + ":" + port);
            return callServer(accountId, amount, isDebit, false, IPAddress, port);
        } catch (Exception e) {
            e.printStackTrace();
            return SetCrossPartTransResponse.newBuilder().setStatus(false).build();
        }
    }

    @Override
    public void onGlobalCommit() {
        System.out.println("Cross-partition transaction: GLOBAL COMMIT");
        applyUpdate();
        transactionStatus = true;
    }

    @Override
    public void onGlobalAbort() {
        System.out.println("Cross-partition transaction: GLOBAL ABORT");
        clearTemp();
        transactionStatus = false;
    }

    // New method to handle acknowledgments
    @Override
    public void acknowledgeCrossPartTrans(AckCrossPartTransRequest request,
            io.grpc.stub.StreamObserver<AckCrossPartTransResponse> responseObserver) {

        String transactionId = request.getTransactionId();
        boolean success = request.getSuccess();

        System.out.println("Received ACK for transaction: " + transactionId + ", Success: " + success);

        PendingTransaction pending = pendingDebits.get(transactionId);

        if (pending != null && !pending.isRolledBack) {
            // Cancel the timeout timer
            if (pending.timer != null) {
                pending.timer.cancel();
            }

            if (success) {
                System.out.println("ACK SUCCESS: Finalizing debit for account " + pending.accountId);
                // Transaction is already committed, just cleanup
            } else {
                System.out.println("ACK FAILURE: Rolling back debit for account " + pending.accountId);
                // Rollback the debit by crediting back
                performRollback(pending);
            }

            pendingDebits.remove(transactionId);
        } else if (pending != null && pending.isRolledBack) {
            System.out.println("Transaction already rolled back due to timeout");
        } else {
            System.out.println("Unknown transaction ID: " + transactionId);
        }

        AckCrossPartTransResponse response = AckCrossPartTransResponse
                .newBuilder()
                .setAcknowledged(true)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private void performRollback(PendingTransaction pending) {
        try {
            // For debit: credit back the amount
            // For credit: debit back the amount
            boolean rollbackIsDebit = !pending.isDebit;

            System.out.println(
                    "Performing rollback: " + (rollbackIsDebit ? "debiting" : "crediting") +
                            " " + pending.amount + " to/from account " + pending.accountId);

            // Start a new transaction to reverse the operation
            startDistributedTx(pending.accountId, pending.amount, rollbackIsDebit);
            updateSecondaryServers(pending.accountId, pending.amount, rollbackIsDebit);
            boolean rollbackStatus = ((DistributedTxCoordinator) server.getCrossTransferTransact()).perform();

            System.out.println("Rollback status: " + rollbackStatus);
        } catch (Exception e) {
            System.err.println("Rollback failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
