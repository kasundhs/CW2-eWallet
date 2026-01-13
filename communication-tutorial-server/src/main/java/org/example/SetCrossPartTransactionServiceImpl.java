package org.example;

import ds.tutorial.communication.grpc.generated.SetCrossPartTransRequest;
import ds.tutorial.communication.grpc.generated.SetCrossPartTransResponse;
import ds.tutorial.communication.grpc.generated.SetCrossPartTransServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import javafx.util.Pair;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SetCrossPartTransactionServiceImpl extends SetCrossPartTransServiceGrpc.SetCrossPartTransServiceImplBase
        implements DistributedTxListner {

    private BankServer server;
    private boolean transactionStatus = false;
    private Pair<String, Double> update;

    public SetCrossPartTransactionServiceImpl(BankServer server) {
        this.server = server;
    }

    @Override
    public void setTransaction(SetCrossPartTransRequest request,
                               StreamObserver<SetCrossPartTransResponse> responseObserver) {

        String accId = request.getAccId();
        double amount = request.getAmount();
        boolean isDebit = request.getIsDebit();
        boolean isSentByPrimary = request.getIsSentByPrimary();
        String txId = request.getTxId();

        try {
            if (server.isLeader()) {
                System.out.println("Leader handling Cross partition transfer");
                if (txId == null || txId.isEmpty()) {
                    txId = "CrossPartition-" + accId + "-" + UUID.randomUUID();
                }
                System.out.println("TX ID : "+txId);
                startDistributedTx(accId, amount, isDebit,txId);
                if (isDebit){
                    if (((DistributedTxCoordinator) server.getCrossTransferTransact()).getChildCount() != 0) {
                        updateSecondaryServers(accId, amount, isDebit, txId);
                        transactionStatus = ((DistributedTxCoordinator) server.getCrossTransferTransact()).perform();
                    } else if (isDebit) {
                        double balance = server.getAccountBalance(accId);
                        if (balance >= amount) {
                            updateSecondaryServers(accId, amount, true, txId);
                            transactionStatus = ((DistributedTxCoordinator) server.getCrossTransferTransact()).perform();
                        } else {
                            transactionStatus = false;
                        }
                    }
                }
                else {
                    updateSecondaryServers(accId, amount, isDebit, txId);
                    transactionStatus = ((DistributedTxCoordinator) server.getCrossTransferTransact()).perform();
                }
            } else {
                if (isSentByPrimary) {
                    System.out.println("Secondary received transfer request");
                    startDistributedTx(accId, amount, isDebit,txId);
                    if (isDebit) {
                        double balance = server.getAccountBalance(accId);
                        if (balance >= amount) {
                            ((DistributedTxParticipant) server.getCrossTransferTransact()).voteCommit();
                        } else {
                            ((DistributedTxParticipant) server.getCrossTransferTransact()).voteAbort();
                        }
                    } else {
                        ((DistributedTxParticipant) server.getCrossTransferTransact()).voteCommit();
                    }
                    transactionStatus = true;
                } else {
                    SetCrossPartTransResponse response = callPrimary(accId, amount, isDebit,txId);
                    transactionStatus = response.getStatus();
                }
            }

        }
        catch (Exception e) {
            e.printStackTrace();
            transactionStatus = false;
        }

        responseObserver.onNext(SetCrossPartTransResponse.newBuilder()
                        .setStatus(transactionStatus)
                        .build());
        responseObserver.onCompleted();
    }

    private void startDistributedTx(String accId, double amount, boolean isDebit, String txId) throws IOException {
        // String txId = UUID.randomUUID().toString();
        String parentPath = "CrossPartition";
        System.out.println("startDistributedTx");
        // Start the transaction with txId as the root
        server.getCrossTransferTransact().start(parentPath + "/" + txId, server.getPartitionId());
        update = new Pair<>(accId, isDebit ? -amount : amount);
    }
    private void updateSecondaryServers(String accId, double amount, boolean isDebit, String txId)
            throws Exception {
        List<String[]> others = server.getOthersData();
        for (String[] data : others) {
            callServer(accId, amount, isDebit, txId, true, data[0], Integer.parseInt(data[1]));
        }
    }
    private SetCrossPartTransResponse callPrimary(String accId, double amount, boolean isDebit, String txId) {
        String[] leader = server.getCurrentLeaderData();
        return callServer(accId, amount, isDebit,txId, false, leader[0], Integer.parseInt(leader[1]));
    }

    private SetCrossPartTransResponse callServer(String accId, double amount, boolean isDebit,String txId, boolean isSentByPrimary,
            String ip, int port) {

        ManagedChannel channel = null;

        try {
            channel = ManagedChannelBuilder
                    .forAddress(ip, port)
                    .usePlaintext()
                    .build();
            SetCrossPartTransServiceGrpc.SetCrossPartTransServiceBlockingStub stub = SetCrossPartTransServiceGrpc.newBlockingStub(channel);

            SetCrossPartTransResponse response =
                    stub.setTransaction(
                            SetCrossPartTransRequest.newBuilder()
                                    .setAccId(accId)
                                    .setAmount(amount)
                                    .setIsDebit(isDebit)
                                    .setIsSentByPrimary(isSentByPrimary)
                                    .setTxId(txId)
                                    .build()
                    );
            return response;
        }
        // To avoid shutdown exceptions
        finally {
            if (channel != null) {
                channel.shutdown();
                try {
                    if (!channel.awaitTermination(3, TimeUnit.SECONDS)) {
                        channel.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    channel.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Override
    public synchronized void onGlobalCommit() {
        System.out.println("On Global Commit from Cross partition Transfers");
        if (update == null) return;
        String accId = update.getKey();
        double newValue = server.getAccountBalance(accId) + update.getValue();
        server.setAccountBalance(accId, newValue);
        update = null;
        System.out.println("Cross-partition update committed");
    }

    @Override
    public synchronized void onGlobalAbort() {
        update = null;
        System.out.println("Cross-partition update aborted");
    }
}
