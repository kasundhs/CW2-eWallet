package org.example;

import ds.tutorial.communication.grpc.generated.SetUpdateRequest;
import ds.tutorial.communication.grpc.generated.SetUpdateResponse;
import ds.tutorial.communication.grpc.generated.SetUpdateServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import javafx.util.Pair;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class SetUpdateServiceImpl extends SetUpdateServiceGrpc.SetUpdateServiceImplBase implements DistributedTxListner {
    private BankServer server;
    private ManagedChannel channel;
    private SetUpdateServiceGrpc.SetUpdateServiceBlockingStub clientStub;

    private Pair<String, Double> fromUpdate;
    private Pair<String, Double> toUpdate;
    private boolean transactionStatus = false;

    public SetUpdateServiceImpl(BankServer server) {
        this.server = server;
    }

    @Override
    public void setUpdate(SetUpdateRequest request,
                          io.grpc.stub.StreamObserver<SetUpdateResponse> responseObserver) {

        String fromAcc = request.getFromAccId();
        String toAcc = request.getToAccId();
        double value = request.getValue();

        if (server.isLeader()) {
            try {
                System.out.println("Leader handling transfer");
                startDistributedTx(fromAcc, toAcc, value);
                updateSecondaryServers(fromAcc, toAcc, value);
                transactionStatus = ((DistributedTxCoordinator) server.getTransferTransaction()).perform();
                System.out.println("Transaction Status : "+transactionStatus);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            try{
                if (request.getIsSentByPrimary()) {
                    System.out.println("Secondary received transfer request");
                    startDistributedTx(fromAcc, toAcc, value);

                    double fromBalance = server.getAccountBalance(fromAcc);
                    if (fromBalance >= value) {
                        ((DistributedTxParticipant) server.getTransferTransaction()).voteCommit();
                    } else {
                        ((DistributedTxParticipant) server.getTransferTransaction()).voteAbort();
                    }
                } else {
                    SetUpdateResponse response = callPrimary(fromAcc, toAcc, value);
                    transactionStatus = response.getStatus();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        SetUpdateResponse response = SetUpdateResponse
                .newBuilder()
                .setStatus(transactionStatus)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private void startDistributedTx(String fromAcc, String toAcc, double value) throws IOException {
        String txId = UUID.randomUUID().toString();
        server.getTransferTransaction().start(fromAcc + "->" + toAcc, txId);
        fromUpdate = new Pair<>(fromAcc, -value);
        toUpdate = new Pair<>(toAcc, value);
    }

    private void applyUpdate(Pair<String, Double> update) {
        String accId = update.getKey();
        double newValue = server.getAccountBalance(accId) + update.getValue();
        server.setAccountBalance(accId, newValue);
    }

    private void clearTemp() {
        fromUpdate = null;
        toUpdate = null;
    }

    private void updateSecondaryServers(String fromAcc, String toAcc, double value)
            throws KeeperException, InterruptedException {

        List<String[]> othersData = server.getOthersData();
        for (String[] data : othersData) {
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);
            callServer(fromAcc, toAcc, value, true, IPAddress, port);
        }
    }

    private SetUpdateResponse callPrimary(String fromAcc, String toAcc, double value) {
        String[] leader = server.getCurrentLeaderData();
        return callServer(fromAcc, toAcc, value, false, leader[0], Integer.parseInt(leader[1]));
    }

    private SetUpdateResponse callServer(String fromAcc, String toAcc, double value,
                                         boolean isSentByPrimary,
                                         String ip, int port) {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(ip, port)
                .usePlaintext()
                .build();

        SetUpdateServiceGrpc.SetUpdateServiceBlockingStub stub =
                SetUpdateServiceGrpc.newBlockingStub(channel);

        SetUpdateRequest request = SetUpdateRequest.newBuilder()
                .setFromAccId(fromAcc)
                .setToAccId(toAcc)
                .setValue(value)
                .setIsSentByPrimary(isSentByPrimary)
                .build();
        return stub.setUpdate(request);
    }



    @Override
    public void onGlobalCommit() {
        System.out.println("onGlobalCommit from Transfers");
        applyUpdate(fromUpdate);
        applyUpdate(toUpdate);
        clearTemp();
        System.out.println("Transfer committed");
    }

    @Override
    public void onGlobalAbort() {
        clearTemp();
        System.out.println("Transfer aborted");
    }
}
