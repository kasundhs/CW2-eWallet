package org.example;

public interface DistributedTxListner {
    void onGlobalCommit();
    void onGlobalAbort();
}
