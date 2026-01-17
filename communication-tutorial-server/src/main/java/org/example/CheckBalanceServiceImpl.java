package org.example;

import ds.tutorial.communication.grpc.generated.CheckBalanceResponse;
import ds.tutorial.communication.grpc.generated.CheckBalanceServiceGrpc;

import java.util.Random;
// For balance inquiries
public class CheckBalanceServiceImpl extends CheckBalanceServiceGrpc.CheckBalanceServiceImplBase {
    private BankServer server;
    public CheckBalanceServiceImpl(BankServer server){
        this.server = server;
    }
    private double getAccountBalance(String accountId) {
        return server.getAccountBalance(accountId);
    }


    @Override
    public void checkBalance(ds.tutorial.communication.grpc.generated.CheckBalanceRequest request,
                             io.grpc.stub.StreamObserver<ds.tutorial.communication.grpc.generated.CheckBalanceResponse> responseObserver) {
        String accountId = request.getAccountId();
        System.out.println("Request received..");
        double balance = getAccountBalance(accountId);
        CheckBalanceResponse response = CheckBalanceResponse.newBuilder()
                .setBalance(balance)
                .build();
        System.out.println("Responding, balance for account " + accountId + " is " + balance);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
