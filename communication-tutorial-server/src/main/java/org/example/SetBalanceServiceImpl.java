package org.example;

import ds.tutorial.communication.grpc.generated.SetBalanceRequest;
import ds.tutorial.communication.grpc.generated.SetBalanceResponse;
import ds.tutorial.communication.grpc.generated.SetBalanceServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import javafx.util.Pair;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
// For account creation
public class SetBalanceServiceImpl extends SetBalanceServiceGrpc.SetBalanceServiceImplBase
        implements DistributedTxListner {
    private ManagedChannel channel = null;
    SetBalanceServiceGrpc.SetBalanceServiceBlockingStub clientStub = null;
    private BankServer server;
    private Pair<String, Double> tempDataHolder;
    private boolean transactionStatus = false;

    public SetBalanceServiceImpl(BankServer server) {
        this.server = server;
    }

    @Override
    public void setBalance(ds.tutorial.communication.grpc.generated.SetBalanceRequest request,
            io.grpc.stub.StreamObserver<ds.tutorial.communication.grpc.generated.SetBalanceResponse> responseObserver) {
        String accountId = request.getAccountId();
        double value = request.getValue();
        if (server.isLeader()) {
            // Act as primary
            try {
                System.out.println("Updating account balance as Primary");
                startDistributedTx(accountId, value);
                updateSecondaryServers(accountId, value);
                System.out.println("going to perform");
                transactionStatus = ((DistributedTxCoordinator) server.getBalanceTransaction()).perform();
                System.out.println("Status : " + transactionStatus);
            } catch (Exception e) {
                System.out.println("Error while updating the account balance" + e.getMessage());
                e.printStackTrace();
            }
        } else {
            // Act As Secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("Updating account balance on secondary, on Primary's command");
                startDistributedTx(accountId, value);
                if (value >= 0.0d) {
                    ((DistributedTxParticipant) server.getBalanceTransaction()).voteCommit();
                } else {
                    ((DistributedTxParticipant) server.getBalanceTransaction()).voteAbort();
                }
            } else {
                SetBalanceResponse response = callPrimary(accountId, value);
                if (response.getStatus()) {
                    transactionStatus = true;
                }
            }
        }
        System.out.println("setBalance");
        SetBalanceResponse response = SetBalanceResponse
                .newBuilder()
                .setStatus(transactionStatus)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private void updateBalance() {
        if (tempDataHolder != null) {
            String accountID = tempDataHolder.getKey();
            double value = tempDataHolder.getValue();
            server.setAccountBalance(accountID, value);
            System.out.println("Account id : " + accountID + " Balance : " + value + " is Updated");
            tempDataHolder = null;
        }
    }

    private SetBalanceResponse callServer(String accountId, double value, boolean isSentByPrimary, String IPAddress,
            int port) {
        System.out.println("Call Server " + IPAddress + ":" + port);
        channel = ManagedChannelBuilder.forAddress(IPAddress, port)
                .usePlaintext()
                .build();
        clientStub = SetBalanceServiceGrpc.newBlockingStub(channel);
        SetBalanceRequest request = SetBalanceRequest
                .newBuilder()
                .setAccountId(accountId)
                .setValue(value)
                .setIsSentByPrimary(isSentByPrimary)
                .build();
        SetBalanceResponse response = clientStub.setBalance(request);
        channel.shutdown();
        System.out.println("callServer");
        return response;
    }

    private SetBalanceResponse callPrimary(String accountId, double value) {
        System.out.println("Calling Primary server");
        String[] currentLeaderData = server.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(accountId, value, false, IPAddress, port);
    }

    private void updateSecondaryServers(String accountId, double value) throws KeeperException, InterruptedException {
        System.out.println("Updating secondary servers");
        List<String[]> othersData = server.getOthersData();
        for (String[] data : othersData) {
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);
            callServer(accountId, value, true, IPAddress, port);
        }
    }

    private void startDistributedTx(String accountId, double value) {
        try {
            server.getBalanceTransaction().start(accountId, String.valueOf(UUID.randomUUID()));
            tempDataHolder = new Pair<>(accountId, value);
            System.out.println("startDistributedTx");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onGlobalAbort() {
        tempDataHolder = null;
        System.out.println("Transaction Aborted by the Coordinator");
    }

    @Override
    public void onGlobalCommit() {
        System.out.println("onGlobalCommit from setBalance");
        updateBalance();
    }
}
