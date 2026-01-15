package org.example;

import ds.tutorial.communication.grpc.generated.*;
import org.example.NameServiceClient;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.sql.SQLOutput;
import java.util.Scanner;

public class CheckBalanceServiceClient {
    public static final String NAME_SERVICE_ADDRESS = "http://localhost:2379";
    private ManagedChannel channel = null;
    private CheckBalanceServiceGrpc.CheckBalanceServiceBlockingStub clientStub = null;
    private SetBalanceServiceGrpc.SetBalanceServiceBlockingStub setBalanceClient = null;
    private SetUpdateServiceGrpc.SetUpdateServiceBlockingStub setUpdateClient = null;
    private SetCrossPartTransServiceGrpc.SetCrossPartTransServiceBlockingStub setCrossPartTransClient = null;
    private String host = null;
    int port = -1;
    private String mode = null;

    public static void main(String[] args) throws InterruptedException, IOException {
        if (args.length != 1) {
            System.out.println("Usage CheckBalanceServiceClient <Mode>");
            System.exit(1);
        }

        CheckBalanceServiceClient client = new CheckBalanceServiceClient(args[0]);
        // client.initializeConnection();
        client.processUserRequests();
        // client.closeConnection();
    }

    public CheckBalanceServiceClient(String mode) throws InterruptedException, IOException {
        this.mode = mode;
        // fetchServerDetails();
    }

    private void fetchServerDetails(String accountId) throws IOException, InterruptedException {
        int partitionId = getPartitionForAccount(accountId);
        String serviceName = "CheckBalanceService_partition_" + partitionId;
        NameServiceClient client = new NameServiceClient(NAME_SERVICE_ADDRESS);
        NameServiceClient.ServiceDetails serviceDetails = client.findService(serviceName);
        host = serviceDetails.getIPAddress();
        port = serviceDetails.getPort();
        System.out.println("Connecting to partition " + partitionId + " server at " + host + ":" + port);
    }

    private void initializeConnection() {
        System.out.println("Initializing Connecting to server at " + host + ":" +
                port);
        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        clientStub = CheckBalanceServiceGrpc.newBlockingStub(channel);
        setBalanceClient = SetBalanceServiceGrpc.newBlockingStub(channel);
        setUpdateClient = SetUpdateServiceGrpc.newBlockingStub(channel);
        setCrossPartTransClient = SetCrossPartTransServiceGrpc.newBlockingStub(channel);
        channel.getState(true);
    }

    private void closeConnection() {
        channel.shutdown();
    }

    private SetUpdateResponse fundTransfers(String fromAccId, String toAccId, double amount) {
        SetUpdateRequest request = SetUpdateRequest
                .newBuilder()
                .setFromAccId(fromAccId)
                .setToAccId(toAccId)
                .setValue(amount)
                .build();
        return setUpdateClient.setUpdate(request);
    }

    private SetBalanceResponse setBalance(String accountId, double amount) {
        SetBalanceRequest request = SetBalanceRequest
                .newBuilder()
                .setAccountId(accountId)
                .setValue(amount)
                .build();

        return setBalanceClient.setBalance(request);
    }

    private SetCrossPartTransResponse setTransaction(String accId, double amount, boolean isDebit) {
        SetCrossPartTransRequest request = SetCrossPartTransRequest
                .newBuilder()
                .setAccId(accId)
                .setAmount(amount)
                .setIsDebit(isDebit)
                .build();
        return setCrossPartTransClient.setTransaction(request);
    }

    private void processUserRequests() throws InterruptedException, IOException {
        while (true) {
            if (mode.equals("c")) {

                Scanner userInput = new Scanner(System.in);
                System.out.println("\n1. Enter Account ID to check the balance \n2. Enter details for Fund transfer\n");
                String inputList = userInput.nextLine().trim();
                String[] inputs = null;
                if (inputList.isEmpty()) {
                    System.out.println(
                            "Usage: to check Balance enter account Id or to transfers enter Sender id, Beneficiary id, Amount\n");
                    continue;
                } else {
                    inputs = inputList.split("\\s*,\\s*");
                }
                if (inputs.length == 1) {
                    String accountId = inputs[0];
                    ensureConnection(accountId);
                    System.out.println("Requesting server to check the account balance for " + accountId);
                    CheckBalanceRequest request = CheckBalanceRequest
                            .newBuilder()
                            .setAccountId(accountId)
                            .build();
                    CheckBalanceResponse response = clientStub.checkBalance(request);
                    System.out.println("My balance is " + response.getBalance() + " LKR\n");
                } else if (inputs.length == 3) {
                    String fromAccId = inputs[0];
                    String toAccId = inputs[1];
                    double amount = Double.parseDouble(inputs[2]);

                    if (isSamePartition(fromAccId, toAccId)) {
                        // Same partition transfer
                        handleSamePartitionTransfer(fromAccId, toAccId, amount);
                    } else {
                        // Cross-partition transfer
                        handleCrossPartitionTransfer(fromAccId, toAccId, amount);
                    }
                }
                closeConnection();
                Thread.sleep(1000);
            } else {
                Scanner userInput = new Scanner(System.in);
                System.out.println(
                        "\n1. Enter Account ID,amount to set the balance :\n2. Enter Account ID to Check Balance\n");
                String inputList = userInput.nextLine().trim();
                String[] inputs = null;
                if (inputList.isEmpty()) {
                    System.out.println(
                            "Usage: Enter Account ID and amount to Create Account or Enter Account ID to check Balance\n");
                    continue;
                } else {
                    inputs = inputList.split("\\s*,\\s*");
                }
                if (inputs.length == 1) {
                    String accountId = inputs[0];
                    ensureConnection(accountId);
                    System.out.println("Requesting server to check the account balance for " + accountId);
                    CheckBalanceRequest request = CheckBalanceRequest
                            .newBuilder()
                            .setAccountId(accountId)
                            .build();
                    CheckBalanceResponse response = clientStub.checkBalance(request);
                    System.out.printf("My balance is " + response.getBalance() + " LKR. Requested by Clark\n");
                } else if (inputs.length == 2) {
                    String accountId = inputs[0];
                    double amount = Double.parseDouble(inputs[1]);
                    ensureConnection(accountId);
                    System.out.println(
                            "Requesting server to set the account balance for " + accountId + " as " + amount + " LKR");

                    SetBalanceResponse response = setBalance(accountId, amount);
                    System.out.printf("Set balance request status is " + response.getStatus());
                } else {
                    System.out.println("Length is " + inputList.length());
                }
                closeConnection();
                Thread.sleep(1000);
            }
        }
    }

    private void ensureConnection(String accountdId) throws IOException, InterruptedException {
        if (channel == null || channel.isShutdown() || channel.isTerminated()
                || channel.getState(true) != ConnectivityState.READY) {
            System.out.println("Connecting to server...");
            fetchServerDetails(accountdId);
            initializeConnection();
        }
    }

    private int getPartitionForAccount(String accountId) {
        int accNumber = Integer.parseInt(accountId);
        return accNumber % 2; // 0 for even, 1 for odd
    }

    private boolean isSamePartition(String fromAccId, String toAccId) {
        return getPartitionForAccount(fromAccId) == getPartitionForAccount(toAccId);
    }

    private void handleSamePartitionTransfer(String fromAccId, String toAccId, double amount)
            throws IOException, InterruptedException {
        ensureConnection(fromAccId);
        SetUpdateResponse response = fundTransfers(fromAccId, toAccId, amount);
        if (response.getStatus()) {
            System.out.println("✓ Same-partition transfer completed successfully");
            System.out.println("  From: " + fromAccId + " → To: " + toAccId + " | Amount: " + amount + " LKR");
        } else {
            System.out.println("✗ Same-partition transfer failed");
        }
    }

    private void handleCrossPartitionTransfer(String fromAccId, String toAccId, double amount)
            throws IOException, InterruptedException {
        System.out.println("=== Cross-Partition Transfer ===");
        System.out.println("From Account: " + fromAccId + " (Partition " + getPartitionForAccount(fromAccId) + ")");
        System.out.println("To Account: " + toAccId + " (Partition " + getPartitionForAccount(toAccId) + ")");
        System.out.println("Amount: " + amount + " LKR");

        // Step 1: Debit from source partition
        ensureConnection(fromAccId);
        SetCrossPartTransResponse debitResponse = debitFromSourcePartition(fromAccId, amount);

        if (!debitResponse.getStatus()) {
            System.out.println("✗ Transaction rejected by source partition");
            return;
        }

        System.out.println("✓ Debit successful from source partition");

        // Step 2: Credit to destination partition
        closeConnection();
        ensureConnection(toAccId);
        SetCrossPartTransResponse creditResponse = creditToDestinationPartition(toAccId, amount);

        if (!creditResponse.getStatus()) {
            System.out.println("✗ Credit rejected by destination partition");
            System.out.println("⚠ Rolling back transaction...");

            // Step 3: Rollback - credit back to source
            closeConnection();
            ensureConnection(fromAccId);
            SetCrossPartTransResponse rollbackResponse = rollbackSourcePartition(fromAccId, amount);

            if (rollbackResponse.getStatus()) {
                System.out.println("✓ Transaction rolled back successfully");
            } else {
                System.out.println("✗ CRITICAL: Rollback failed! Manual intervention required.");
            }
            return;
        }

        System.out.println("✓ Cross-partition transfer completed successfully");
    }

    private SetCrossPartTransResponse debitFromSourcePartition(String accountId, double amount) {
        System.out.println("→ Debiting " + amount + " LKR from account " + accountId);
        return setTransaction(accountId, amount, true);
    }

    private SetCrossPartTransResponse creditToDestinationPartition(String accountId, double amount) {
        System.out.println("→ Crediting " + amount + " LKR to account " + accountId);
        return setTransaction(accountId, amount, false);
    }

    private SetCrossPartTransResponse rollbackSourcePartition(String accountId, double amount) {
        System.out.println("→ Reverting debit: crediting " + amount + " LKR back to account " + accountId);
        return setTransaction(accountId, amount, false);
    }
}