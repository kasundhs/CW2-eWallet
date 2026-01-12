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

public class SetCrossPartTransactionImpl extends SetCrossPartTransServiceGrpc.SetCrossPartTransServiceImplBase
        implements DistributedTxListner {

    private BankServer server;
    private boolean transactionStatus = false;
    private Pair<String, Double> update;

    public SetCrossPartTransactionImpl(BankServer server) {
        this.server = server;
    }

    @Override
    public void setTransaction(SetCrossPartTransRequest request,
                               StreamObserver<SetCrossPartTransResponse> responseObserver) {

        String accId = request.getAccId();
        double amount = request.getAmount();
        boolean isDebit = request.getIsDebit();
        boolean isSentByPrimary = request.getIsSentByPrimary();

        try {
            if (server.isLeader()) {
                startDistributedTx(accId, amount, isDebit);
                updateSecondaryServers(accId, amount, isDebit);
                transactionStatus = ((DistributedTxCoordinator)server.getCrossTransferTransact()).perform();

            } else {
                if (isSentByPrimary) {
                    startDistributedTx(accId, amount, isDebit);
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
                    SetCrossPartTransResponse response = callPrimary(accId, amount, isDebit);
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

    private void startDistributedTx(String accId, double amount, boolean isDebit) throws IOException {
        String txId = UUID.randomUUID().toString();
        server.getCrossTransferTransact().start("CrossPartition" + accId + server.getPartitionId(), server.getPartitionId());
        update = new Pair<>(accId, isDebit ? -amount : amount);
    }
    private void updateSecondaryServers(String accId, double amount, boolean isDebit)
            throws Exception {
        List<String[]> others = server.getOthersData();
        for (String[] data : others) {
            callServer(accId, amount, isDebit, true, data[0], Integer.parseInt(data[1]));
        }
    }
    private SetCrossPartTransResponse callPrimary(String accId, double amount, boolean isDebit) {
        String[] leader = server.getCurrentLeaderData();
        return callServer(accId, amount, isDebit, false, leader[0], Integer.parseInt(leader[1]));
    }

    private SetCrossPartTransResponse callServer(String accId, double amount, boolean isDebit, boolean isSentByPrimary,
            String ip, int port) {

        ManagedChannel channel = ManagedChannelBuilder
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
                                .build()
                );
        channel.shutdown();
        return response;
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
