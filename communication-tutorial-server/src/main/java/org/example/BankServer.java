package org.example;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import ds.tutorial.communication.grpc.generated.*;


public class BankServer {
    private NameServiceClient nameServiceClient;
    public static final String NAME_SERVICE_ADDRESS = "http://localhost:2379";
    private final int serverPort;
    private DistributedLock leaderLock;
    private AtomicBoolean isLeader = new AtomicBoolean(false);
    private byte[] leaderData;
    Map<String, Double> accounts = new HashMap();
    private int partitionId;
    private final AtomicBoolean syncedAsFollower = new AtomicBoolean(false);
    private final AtomicBoolean snapshotReady = new AtomicBoolean(false);


    DistributedTx balanceTransaction;
    DistributedTx transferTransaction;
    DistributedTx crossTransferTransact;
    SetBalanceServiceImpl setBalanceService;
    CheckBalanceServiceImpl checkBalanceService;
    SetUpdateServiceImpl setUpdateService;
    AccountSyncServiceImpl accountSyncService;
    SetCrossPartTransactionImpl setCrossPartTransaction;

    public static void main (String[] args) throws Exception{
        DistributedLock.setZooKeeperURL("localhost:2181");
        DistributedTx.setZookeeperUrl("localhost:2181");
        int serverPort;
        if (args.length != 2) {
            System.out.println("Usage BankServer <port> <partition id>");
            System.exit(1);
        }
        serverPort = Integer.parseInt(args[0].trim());
        int partitionId = Integer.parseInt(args[1].trim());
        BankServer server = new BankServer("localhost", serverPort, partitionId);
        server.startServer();
    }
    public BankServer(String host, int port, int partitionId) throws InterruptedException, IOException, KeeperException {
        this.serverPort = port;
        this.partitionId = partitionId;
        snapshotReady.set(false);
        leaderLock = new DistributedLock("BankServerTestCluster", buildServerData(host, port), partitionId);
        nameServiceClient = new NameServiceClient(NAME_SERVICE_ADDRESS);
        setBalanceService = new SetBalanceServiceImpl(this);
        checkBalanceService = new CheckBalanceServiceImpl(this);
        setUpdateService = new SetUpdateServiceImpl(this);
        accountSyncService = new AccountSyncServiceImpl(this);
        setCrossPartTransaction = new SetCrossPartTransactionImpl(this);
        balanceTransaction = new DistributedTxParticipant(setBalanceService, snapshotReady);
        transferTransaction = new DistributedTxParticipant(setUpdateService, snapshotReady);
        crossTransferTransact = new DistributedTxParticipant(setCrossPartTransaction,snapshotReady);
    }

    public DistributedTx getBalanceTransaction() {
        return balanceTransaction;
    }
    public DistributedTx getTransferTransaction(){
        return transferTransaction;
    }
    public DistributedTx getCrossTransferTransact(){
        return crossTransferTransact;
    }

    public String getPartitionId() {
        return String.valueOf(partitionId);
    }

    public static String buildServerData(String IP, int port) {
        StringBuilder builder = new StringBuilder();
        builder.append(IP).append(":").append(port);
        return builder.toString();
    }

    public boolean isLeader() {
        return isLeader.get();
    }
    private synchronized void setCurrentLeaderData(byte[] leaderData) {
        this.leaderData = leaderData;
    }

    class LeaderCampaignThread implements Runnable {
        private byte[] currentLeaderData = null;
        @Override
        public void run() {
            System.out.println("Starting leader campaign for partition " + partitionId);
            try {
                boolean leader = leaderLock.tryAcquireLock();
                if (!leader && (syncedAsFollower.compareAndSet(false, true))){
                    byte[] leaderData = leaderLock.getLockHolderData();
                    if (currentLeaderData != leaderData) {
                        currentLeaderData = leaderData;
                        setCurrentLeaderData(currentLeaderData);
                        syncAccountsFromPrimary();
                        snapshotReady.set(true);
                    }
                }
                while (!leader) {
                    Thread.sleep(10000);
                    leader = leaderLock.tryAcquireLock();
                }
                System.out.println("I got the leader lock for partition " + partitionId + ". Acting as primary.");
                currentLeaderData = null;
                beTheLeader();
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    private void tryToBeLeader() throws KeeperException, InterruptedException {
        Thread leaderCampaignThread = new Thread(new LeaderCampaignThread());
        leaderCampaignThread.start();
    }
    public void startServer() throws IOException, InterruptedException, KeeperException {
        Server server = ServerBuilder
                .forPort(serverPort)
                .addService(checkBalanceService)
                .addService(setBalanceService)
                .addService(setUpdateService)
                .addService(accountSyncService)
                .addService(setCrossPartTransaction)
                .build();
        server.start();
        System.out.println("BankServer Started and ready to accept requests on port " + serverPort);
        tryToBeLeader();
        server.awaitTermination();
    }
    public void setAccountBalance(String accountId, double value) {
        accounts.put(accountId, value);
    }
    public double getAccountBalance(String accountId) {
        Double value = accounts.get(accountId);
        System.out.println("Map Size is : "+accounts.size());
        return (value != null) ? value : 0.0;
    }
    public synchronized String[] getCurrentLeaderData() {
        return new String(leaderData).split(":");
    }
    public List<String[]> getOthersData() throws KeeperException, InterruptedException {
        List<String[]> result = new ArrayList<>();
        List<byte[]> othersData = leaderLock.getOthersData();
        for (byte[] data : othersData) {
            String[] dataStrings = new String(data).split(":");
            result.add(dataStrings);
        }
        return result;
    }
    private void beTheLeader() throws IOException, InterruptedException, KeeperException {
        System.out.println("I got the leader lock. Now acting as primary");
        isLeader.set(true);
        snapshotReady.set(true);
        registerService();
        balanceTransaction = new DistributedTxCoordinator(setBalanceService);
        transferTransaction = new DistributedTxCoordinator(setUpdateService);
        crossTransferTransact = new DistributedTxCoordinator(setCrossPartTransaction);
    }
    private void registerService() throws IOException, InterruptedException, KeeperException {
        String serviceName = "CheckBalanceService_partition_" + partitionId;
        nameServiceClient.registerService(
                serviceName,
                "127.0.0.1",
                serverPort,
                "tcp"
        );
    }
    public synchronized void syncFromLeader(Map<String, Double> leaderAccounts) {
        System.out.println("Syncing accounts from primary. Entries = " + leaderAccounts.size());
        this.accounts.clear();
        this.accounts.putAll(leaderAccounts);
    }
    private void syncAccountsFromPrimary() {
        try {
            String[] leader = getCurrentLeaderData();
            String leaderHost = leader[0];
            int leaderPort = Integer.parseInt(leader[1]);

            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(leaderHost, leaderPort)
                    .usePlaintext()
                    .build();

            AccountSyncServiceGrpc.AccountSyncServiceBlockingStub stub =
                    AccountSyncServiceGrpc.newBlockingStub(channel);

            GetAllAccountsResponse response = stub.getAllAccounts(
                    GetAllAccountsRequest.newBuilder().build());

            Map<String, Double> leaderAccounts = new HashMap<>();
            response.getAccountsMap().forEach(leaderAccounts::put);


            syncFromLeader(leaderAccounts);
            channel.shutdown();
            System.out.println("Account Sync Successfully Completed");

        } catch (Exception e) {
            System.err.println("Failed to sync from primary: " + e.getMessage());
        }
    }


}