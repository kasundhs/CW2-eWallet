# CW2-eWallet System Documentation

## Project Overview

This project implements a distributed eWallet system designed to handle account balance checks, fund transfers, and account management across multiple partitions. The system leverages modern distributed systems technologies to ensure consistency, fault tolerance, and scalability. It is built using Java 17, Maven for build management, gRPC for inter-service communication, etcd for service discovery, and Apache ZooKeeper for distributed coordination and transaction management.

The eWallet system supports:
- Checking account balances
- Setting account balances (for administrative purposes)
- Intra-partition fund transfers
- Cross-partition fund transfers
- Automatic leader election and data replication within partitions
- Distributed transaction coordination using a two-phase commit protocol

## Architecture

### System Components

The system is composed of five main Maven modules:

1. **communication-client**: A gRPC client application that allows users to interact with the eWallet services. It supports both customer mode (balance checks and transfers) and administrative mode (balance setting).

2. **communication-tutorial-server**: The core server component implementing gRPC services. It handles partitioned data storage, leader election, and coordinates distributed transactions.

3. **Naming**: A service discovery module using etcd as the backend for registering and discovering service endpoints.

4. **synchronization**: Provides distributed locking mechanisms using Apache ZooKeeper for coordinating access to shared resources.

5. **synshronization-2**: Extends the synchronization module with distributed transaction support, implementing a two-phase commit protocol for ensuring atomicity across multiple partitions.

### Architecture Diagram Description

```
[Client Applications]
    |
    | (gRPC)
    v
[Load Balancer / Service Discovery (etcd)]
    |
    +----------------+----------------+
    | Partition 0    | Partition 1    |
    | (Even Account  | (Odd Account   |
    |  IDs)          |  IDs)          |
    +----------------+----------------+
    | Primary Server | Primary Server |
    | +------------+ | +------------+ |
    | | gRPC       | | | gRPC       | |
    | | Services   | | | Services   | |
    | +------------+ | +------------+ |
    | | Account    | | | Account    | |
    | | Storage    | | | Storage    | |
    | +------------+ | +------------+ |
    | Secondary     | Secondary     |
    | Servers       | Servers       |
    +----------------+----------------+
         |     |          |     |
         |     |          |     |
         +-----+----------+-----+
               |
               v
    [ZooKeeper Ensemble]
    (Leader Election, Distributed Locks, 2PC Coordination)
```

**Key Architectural Elements:**

- **Partitioning**: Accounts are partitioned based on account ID parity (even IDs in partition 0, odd IDs in partition 1). This allows horizontal scaling while maintaining data locality for most operations.

- **Leader Election**: Within each partition, servers compete for leadership using ZooKeeper ephemeral sequential nodes. The leader handles all write operations and coordinates replication.

- **Replication**: The leader maintains an in-memory account map and replicates changes to secondary servers using gRPC calls.

- **Service Discovery**: Servers register their endpoints with etcd. Clients query etcd to discover the appropriate partition leader for a given account.

- **Distributed Transactions**: Cross-partition transfers use a two-phase commit protocol coordinated through ZooKeeper to ensure atomicity.

- **Consistency Model**: The system provides strong consistency for single-partition operations and eventual consistency for cross-partition operations through the 2PC protocol.

## Component Descriptions

### communication-client
- **Purpose**: Provides a command-line interface for eWallet operations
- **Key Classes**:
  - `CheckBalanceServiceClient`: Main client class handling gRPC connections and user interactions
  - `NameServiceClient`: Handles service discovery via etcd
- **Modes**: 
  - Customer mode (`c`): Balance checks and fund transfers
  - Administrative mode (`a`): Balance setting operations

### communication-tutorial-server
- **Purpose**: Implements the core eWallet business logic and gRPC services
- **Key Classes**:
  - `BankServer`: Main server class managing lifecycle, leader election, and service registration
  - `CheckBalanceServiceImpl`: Handles balance inquiry requests
  - `SetBalanceServiceImpl`: Manages balance setting operations with distributed coordination
  - `SetUpdateServiceImpl`: Handles intra-partition fund transfers
  - `SetCrossPartTransactionServiceImpl`: Manages cross-partition transfers
  - `AccountSyncServiceImpl`: Provides account synchronization for followers
- **Features**: Leader election, distributed transactions, account storage

### Naming
- **Purpose**: Service discovery and registration
- **Key Classes**:
  - `EtcdClient`: Low-level etcd operations (put/get)
  - `NameServiceClient`: High-level service discovery interface
- **Functionality**: Register services with IP/port/protocol, discover services by name

### synchronization
- **Purpose**: Distributed locking for mutual exclusion
- **Key Classes**:
  - `DistributedLock`: Implements ZooKeeper-based distributed locks
  - `ZooKeeperClient`: Wrapper for ZooKeeper operations
  - `DummyProcess`: Example usage of distributed locks
- **Algorithm**: Uses ephemeral sequential nodes for fair locking

### synshronization-2
- **Purpose**: Distributed transaction coordination
- **Key Classes**:
  - `DistributedTxCoordinator`: Initiates and coordinates 2PC transactions
  - `DistributedTxParticipant`: Participates in distributed transactions
  - `DistributedTx`: Abstract base class for transaction participants
  - `DistributedTxListner`: Interface for transaction lifecycle callbacks
- **Protocol**: Two-phase commit with voting and global decision phases

## Setup Instructions

### Prerequisites
- Java 17 JDK
- Apache Maven 3.6+
- etcd (for service discovery)
- Apache ZooKeeper 3.6+ (for coordination)

### Environment Setup
1. **Install etcd**:
   ```bash
   # On Ubuntu/Debian
   sudo apt-get install etcd

   # Or download from https://etcd.io/docs/v3.5/install/
   ```

2. **Install ZooKeeper**:
   ```bash
   # Download and extract ZooKeeper
   wget https://downloads.apache.org/zookeeper/zookeeper-3.6.3/apache-zookeeper-3.6.3-bin.tar.gz
   tar -xzf apache-zookeeper-3.6.3-bin.tar.gz
   cd apache-zookeeper-3.6.3-bin
   ```

3. **Configure ZooKeeper**:
   Create `conf/zoo.cfg`:
   ```
   tickTime=2000
   dataDir=/tmp/zookeeper
   clientPort=2181
   ```

### Project Setup
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd CW2-eWallet
   ```

2. Build all modules:
   ```bash
   mvn clean install
   ```

## Build and Run Instructions

### Starting Infrastructure Services

1. **Start etcd**:
   ```bash
   etcd
   ```

2. **Start ZooKeeper**:
   ```bash
   ./bin/zkServer.sh start
   ```

### Running the System

1. **Start Bank Servers** (run multiple instances for each partition):
   ```bash
   # Partition 0 servers
   java -jar communication-tutorial-server/target/communication-tutorial-server-1.0-SNAPSHOT-jar-with-dependencies.jar 8080 0
   java -jar communication-tutorial-server/target/communication-tutorial-server-1.0-SNAPSHOT-jar-with-dependencies.jar 8081 0

   # Partition 1 servers
   java -jar communication-tutorial-server/target/communication-tutorial-server-1.0-SNAPSHOT-jar-with-dependencies.jar 8082 1
   java -jar communication-tutorial-server/target/communication-tutorial-server-1.0-SNAPSHOT-jar-with-dependencies.jar 8083 1
   ```

2. **Start Client**:
   ```bash
   # Customer mode
   java -jar communication-client/target/communication-client-1.0-SNAPSHOT-jar-with-dependencies.jar c

   # Administrative mode
   java -jar communication-client/target/communication-client-1.0-SNAPSHOT-jar-with-dependencies.jar a
   ```

### Client Usage Examples

**Customer Mode**:
```
1. Enter Account ID to check the balance
2. Enter details for Fund transfer

1
1234
My balance is 1000.0 LKR

2
1234,5678,500
Fund Transfer From 1234 To 5678 is true As Same Partition
```

**Administrative Mode**:
```
1. Enter Account ID,amount to set the balance :
2. Enter Account ID to Check Balance

1
1234,1000
Set balance request status is true

2
1234
My balance is 1000.0 LKR. Requested by Clark
```

## API Documentation

The system exposes gRPC services defined in `BankService.proto`. All services use protocol buffers for message serialization.

### CheckBalanceService
```protobuf
service CheckBalanceService {
  rpc checkBalance(CheckBalanceRequest) returns (CheckBalanceResponse);
}

message CheckBalanceRequest {
  string accountId = 1;
}

message CheckBalanceResponse {
  double balance = 1;
}
```

**Description**: Retrieves the current balance for a specified account.

### SetBalanceService
```protobuf
service SetBalanceService {
  rpc setBalance(SetBalanceRequest) returns (SetBalanceResponse);
}

message SetBalanceRequest {
  string accountId = 1;
  double value = 2;
  bool isSentByPrimary = 3;
}

message SetBalanceResponse {
  bool status = 1;
}
```

**Description**: Sets the balance for an account. Used for administrative purposes and internal replication.

### SetUpdateService
```protobuf
service SetUpdateService {
  rpc setUpdate(SetUpdateRequest) returns (SetUpdateResponse);
}

message SetUpdateRequest {
  string fromAccId = 1;
  string toAccId = 2;
  double value = 3;
  bool isSentByPrimary = 4;
  string transactionId = 5;
}

message SetUpdateResponse {
  bool status = 1;
}
```

**Description**: Performs fund transfers between accounts within the same partition.

### AccountSyncService
```protobuf
service AccountSyncService {
  rpc GetAllAccounts(GetAllAccountsRequest) returns (GetAllAccountsResponse);
}

message GetAllAccountsRequest {}

message GetAllAccountsResponse {
  map<string, double> accounts = 1;
}
```

**Description**: Used by secondary servers to synchronize account data from the primary.

### SetCrossPartTransService
```protobuf
service SetCrossPartTransService {
  rpc setTransaction(SetCrossPartTransRequest) returns (SetCrossPartTransResponse);
}

message SetCrossPartTransRequest {
  string accId = 1;
  double amount = 2;
  bool isDebit = 3;
  bool isSentByPrimary = 4;
  string txId = 5;
}

message SetCrossPartTransResponse {
  bool status = 1;
}
```

**Description**: Handles cross-partition transactions using distributed commit protocol.

## Distributed Coordination Details

### Leader Election
- Uses ZooKeeper ephemeral sequential nodes
- Each server creates a node under `/BankServerTestCluster_partition_{id}/lp_`
- Smallest sequence number becomes leader
- Automatic failover when leader fails

### Distributed Transactions
- Implements two-phase commit protocol
- Coordinator creates transaction root node
- Participants vote by creating child nodes with vote values
- Coordinator decides based on all votes
- Global decision propagated to all participants

### Service Discovery
- Servers register with etcd on becoming leader
- Key format: `CheckBalanceService_partition_{id}`
- Value: JSON with IP, port, protocol
- Clients query etcd to find appropriate service endpoint

## Performance Considerations

- **Partitioning**: Reduces contention by distributing accounts across partitions
- **In-memory Storage**: Fast access but requires careful replication
- **gRPC**: Efficient binary protocol for inter-service communication
- **ZooKeeper**: Reliable coordination but adds network latency

## Limitations and Future Improvements

- **Scalability**: Current implementation uses in-memory storage; consider persistent storage for production
- **Fault Tolerance**: Limited to partition-level replication; consider cross-partition replication
- **Security**: No authentication/authorization implemented
- **Monitoring**: Limited logging and metrics collection
- **Testing**: Requires integration testing with multiple servers

## Academic Context

This project demonstrates key concepts in distributed systems including:
- Distributed consensus and leader election
- Distributed transactions and two-phase commit
- Service discovery and load balancing
- Partitioning and data locality
- Fault tolerance and replication
- gRPC and protocol buffers for efficient communication

The implementation serves as a practical example of building scalable, fault-tolerant distributed systems using industry-standard tools and protocols.