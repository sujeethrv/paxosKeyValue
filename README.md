# paxosKeyValue

### Project Title: Fault-Tolerant Key-Value Store with Paxos Protocol

#### Project Context:
This project was developed as a part of a Distributed Systems course, focusing on implementing the Paxos consensus algorithm to create a robust and fault-tolerant key-value store. The project emphasized practical skills in building distributed systems with Go, including aspects of concurrency, and fault tolerance.

#### Project Description:
Developed a fault-tolerant key-value store leveraging the Paxos protocol to ensure data consistency and reliability across multiple server replicas. The implementation was done in Go, with a focus on RPC communication and concurrent programming.

#### Responsibilities and Achievements:
- Implemented client-side operations (`src/kvpaxos/client.go`) for Put, Append, and Get functions, tailored for distributed systems.
- Developed server-side logic (`src/kvpaxos/server.go`) to handle client requests and maintain data consistency across replicas using the Paxos consensus algorithm.
- Created a common module (`src/kvpaxos/common.go`) to define standard operations and data structures for the system.
- Utilized RPC for communication between clients and servers, and among Paxos peers within servers.
- Ensured data consistency and fault tolerance by integrating the Paxos algorithm into each server, keeping all replicas synchronized.
- Addressed challenges such as duplicate client requests and server recovery to maintain system consistency.
- Debugged and resolved complex distributed system issues, including network partitions and server failures.

#### Technical Stack:
- Language: Go
- Concepts: Distributed Systems, Consensus Algorithms, Fault Tolerance

#### Project Outcomes:
- Successfully delivered a fault-tolerant key-value store as a course project, demonstrating practical application of distributed system concepts.
- Gained advanced skills in Go for distributed systems programming, including concurrency control, RPC mechanisms, and implementing consensus algorithms.
- Developed a deeper understanding of distributed system challenges and solutions, particularly in the context of fault tolerance and data consistency.
