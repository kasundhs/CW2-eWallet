package org.example;

import ds.tutorial.communication.grpc.generated.*;
import org.example.NameServiceClient;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.Scanner;
public class CheckBalanceServiceClient {
    public static final String NAME_SERVICE_ADDRESS = "http://localhost:2379";
    private ManagedChannel channel = null;
    private CheckBalanceServiceGrpc.CheckBalanceServiceBlockingStub clientStub = null;
    private SetBalanceServiceGrpc.SetBalanceServiceBlockingStub setBalanceClient = null;
    private SetUpdateServiceGrpc.SetUpdateServiceBlockingStub setUpdateClient = null;
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
        //client.closeConnection();
    }
    public CheckBalanceServiceClient (String mode) throws InterruptedException, IOException {
        this.mode = mode;
        // fetchServerDetails();
    }
    private void fetchServerDetails() throws IOException, InterruptedException {
        NameServiceClient client = new NameServiceClient(NAME_SERVICE_ADDRESS);
        NameServiceClient.ServiceDetails serviceDetails = client.findService("CheckBalanceService");
        host = serviceDetails.getIPAddress();
        port = serviceDetails.getPort();
    }
    private void initializeConnection () {
        System.out.println("Initializing Connecting to server at " + host + ":" +
                port);
        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        clientStub = CheckBalanceServiceGrpc.newBlockingStub(channel);
        setBalanceClient = SetBalanceServiceGrpc.newBlockingStub(channel);
        setUpdateClient = SetUpdateServiceGrpc.newBlockingStub(channel);
        channel.getState(true);
    }
    private void closeConnection() {
        channel.shutdown();
    }
    private void processUserRequests() throws InterruptedException, IOException {
        while(true){
            if (mode.equals("c")) {

                Scanner userInput = new Scanner(System.in);
                System.out.println("\n1. Enter Account ID to check the balance \n2. Enter details for Fund transfer\n");
                String inputList = userInput.nextLine().trim();
                ensureConnection();
                String[] inputs =null;
                if (inputList.isEmpty()) {
                    System.out.println("Usage: to check Balance enter account Id or to transfers enter Sender id, Beneficiary id, Amount\n");
                    continue;
                } else {
                    inputs = inputList.split("\\s*,\\s*");
                }
                if(inputs.length == 1){
                    String accountId = inputs[0];
                    System.out.println("Requesting server to check the account balance for " + accountId);
                    CheckBalanceRequest request = CheckBalanceRequest
                            .newBuilder()
                            .setAccountId(accountId)
                            .build();
                    CheckBalanceResponse response = clientStub.checkBalance(request);
                    System.out.println("My balance is " + response.getBalance() + " LKR\n");
                }
                else if (inputs.length == 3){
                    String fromAccId = inputs[0];
                    String toAccId = inputs[1];
                    double amount = Double.parseDouble(inputs[2]);
                    SetUpdateRequest request = SetUpdateRequest
                            .newBuilder()
                            .setFromAccId(fromAccId)
                            .setToAccId(toAccId)
                            .setValue(amount)
                            .build();
                    SetUpdateResponse response = setUpdateClient.setUpdate(request);
                    System.out.println("Fund Transfer From "+fromAccId+" To "+toAccId+" is "+response.getStatus());
                }
                closeConnection();
                Thread.sleep(1000);
            } else {
                Scanner userInput = new Scanner(System.in);
                System.out.println("\n1. Enter Account ID,amount to set the balance :\n2. Enter Account ID to Check Balance\n");
                String inputList = userInput.nextLine().trim();
                ensureConnection();
                String[] inputs =null;
                if (inputList.isEmpty()) {
                    System.out.println("Usage: Enter Account ID and amount to Create Account or Enter Account ID to check Balance\n");
                    continue;
                } else {
                    inputs = inputList.split("\\s*,\\s*");
                }
                if(inputs.length == 1){
                    String accountId = inputs[0];
                    System.out.println("Requesting server to check the account balance for " + accountId);
                    CheckBalanceRequest request = CheckBalanceRequest
                            .newBuilder()
                            .setAccountId(accountId)
                            .build();
                    CheckBalanceResponse response = clientStub.checkBalance(request);
                    System.out.printf("My balance is " + response.getBalance() + " LKR. Requested by Clark\n");
                }
                else if(inputs.length == 2) {
                    String accountId = inputs[0];
                    double amount = Double.parseDouble(inputs[1]);
                    System.out.println("Requesting server to set the account balance for " + accountId + " as " + amount + " LKR");
                    SetBalanceRequest request = SetBalanceRequest
                            .newBuilder()
                            .setAccountId(accountId)
                            .setValue(amount)
                            .build();

                    SetBalanceResponse response = setBalanceClient.setBalance(request);
                    System.out.printf("Set balance request status is " + response.getStatus());
                }
                else{
                    System.out.println("Length is "+inputList.length());
                }
                closeConnection();
                Thread.sleep(1000);
            }
        }
    }

    private void ensureConnection() throws IOException, InterruptedException {
        if (channel == null || channel.isShutdown() || channel.isTerminated()
                || channel.getState(true) != ConnectivityState.READY) {
            System.out.println("Connecting to server...");
            fetchServerDetails();
            initializeConnection();
        }
    }
}