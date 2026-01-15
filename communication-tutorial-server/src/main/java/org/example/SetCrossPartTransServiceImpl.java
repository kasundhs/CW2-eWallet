package org.example;

import ds.tutorial.communication.grpc.generated.SetCrossPartTransRequest;
import ds.tutorial.communication.grpc.generated.SetCrossPartTransResponse;
import ds.tutorial.communication.grpc.generated.SetCrossPartTransServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import javafx.util.Pair;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class SetCrossPartTransServiceImpl extends SetCrossPartTransServiceGrpc.SetCrossPartTransServiceImplBase
        implements DistributedTxListner {

    private BankServer server;
    private ManagedChannel channel;
    private SetCrossPartTransServiceGrpc.SetCrossPartTransServiceBlockingStub clientStub;

    private Pair<String, Double> tempDataHolder;
    private boolean transactionStatus = false;

    public SetCrossPartTransServiceImpl(BankServer server) {
        this.server = server;
    }

    @Override
    public void setTransaction(SetCrossPartTransRequest request,
            io.grpc.stub.StreamObserver<SetCrossPartTransResponse> responseObserver) {

        String accountId = request.getAccId();
        double amount = request.getAmount();
        boolean isDebit = request.getIsDebit();

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
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
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
}
