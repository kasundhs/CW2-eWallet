package org.example;

import ds.tutorial.communication.grpc.generated.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class AccountSyncServiceImpl extends AccountSyncServiceGrpc.AccountSyncServiceImplBase {

    private final BankServer server;

    public AccountSyncServiceImpl(BankServer server) {
        this.server = server;
    }

    @Override
    public void getAllAccounts(GetAllAccountsRequest request,
            StreamObserver<GetAllAccountsResponse> responseObserver) {

        if (!server.isLeader()) {
            responseObserver.onError(
                    Status.FAILED_PRECONDITION
                            .withDescription("Not leader")
                            .asRuntimeException());
            return;
        }

        GetAllAccountsResponse response = GetAllAccountsResponse.newBuilder()
                .putAllAccounts(server.accounts)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
