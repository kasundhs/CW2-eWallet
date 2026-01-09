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
    SetBalanceServiceGrpc.SetBalanceServiceBlockingStub setBalanceClient = null;
    private String host = null;
    int port = -1;
    private String mode = null;
    public static void main(String[] args) throws InterruptedException, IOException {
        if (args.length != 1) {
            System.out.println("Usage CheckBalanceServiceClient <Mode>");
            System.exit(1);
        }

        CheckBalanceServiceClient client = new CheckBalanceServiceClient(args[0]);
        client.initializeConnection();
        client.processUserRequests();
        client.closeConnection();
    }
    public CheckBalanceServiceClient (String mode) throws InterruptedException, IOException {
        this.mode = mode;
        fetchServerDetails();
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
        channel.getState(true);
    }
    private void closeConnection() {
        channel.shutdown();
    }
    private void processUserRequests() throws InterruptedException, IOException {
        while(true){
            if (mode.equals("c")) {

                Scanner userInput = new Scanner(System.in);
                System.out.println("\nEnter Account ID to check the balance :");

                String accountId = userInput.nextLine().trim();
                System.out.println("Requesting server to check the account balance for " + accountId.toString());
                CheckBalanceRequest request = CheckBalanceRequest
                        .newBuilder()
                        .setAccountId(accountId)
                        .build();

                CheckBalanceResponse response = clientStub.checkBalance(request);
                System.out.printf("My balance is " + response.getBalance() + " LKR");

                Thread.sleep(1000);
            } else {
                Scanner userInput = new Scanner(System.in);
                System.out.println("\nEnter Account ID,amount to set the balance :");

                String setBalanceInput = userInput.nextLine().trim();
                String accountId = setBalanceInput.split(",")[0];
                double amount = Double.parseDouble(setBalanceInput.split(",")[1]);
                System.out.println("Requesting server to set the account balance for " + accountId.toString() + " as " + amount + " LKR");
                SetBalanceRequest request = SetBalanceRequest
                        .newBuilder()
                        .setAccountId(accountId)
                        .setValue(amount)
                        .build();

                SetBalanceResponse response = setBalanceClient.setBalance(request);
                System.out.printf("Set balance request status is " + response.getStatus());

                Thread.sleep(1000);
            }
        }
    }
}