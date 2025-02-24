# DuckDB-MPP: Distributed DuckDB

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

`DuckDB-MPP` is a distributed query processing extension for DuckDB, designed to scale
data storage and analytical workloads across multiple nodes. Inspired by the [Citus extension](https://github.com/citusdata/citus) for
PostgreSQL, it transforms DuckDB into a powerful MPP (Massively Parallel Processing) system while retaining its
lightweight nature and SQL compatibility.

> [!WARNING]     
> This is a work-in-progress (WIP) personal project, primarily aimed at deepening my understanding of DuckDB's implementation,
> not for production usage

## Building
### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/mpp/mpp.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `mpp.duckdb_extension` is the loadable binary as it would be distributed.

## Key Features
### Distributed Data Storage
Automatically shards tables across multiple nodes using a share-nothing architecture, enabling horizontal scalability for large datasets.
### Parallel Query Execution
Leverages multiple CPU cores and nodes to process queries in parallel, accelerating analytical workloads.
### Coordinator-Worker Model
Employs a central coordinator node to manage query routing and result aggregation, simplifying client interactions.
### Native DuckDB Integration
Builds directly on DuckDB's query engine, maintaining full SQL compatibility and leveraging its efficient columnar storage.

## Example Setup

### Configure Nodes  
Suppose you have three nodes with IP addresses:
- 192.168.10.10 (Coordinator)
- 192.168.10.11 (Worker 1)
- 192.168.10.12 (Worker 2)

### Initialize Databases
On each node, start DuckDB and create a database:
```bash
# On all nodes (coordinator and workers)
duckdb /path/to/test.db
```

### Attach Distributed Database
On each node, attach the distributed database using the `ATTACH` command:
```SQL
-- On 192.168.10.10 (Coordinator)
ATTACH 'test' AS mpp_test (type mpp, endpoint '192.168.10.10:12345');
USE mpp_test;

-- On 192.168.10.11 (Worker 1)
ATTACH 'test' AS mpp_test (type mpp, endpoint '192.168.10.11:12345');
USE mpp_test;

-- On 192.168.10.12 (Worker 2)
ATTACH 'test' AS mpp_test (type mpp, endpoint '192.168.10.12:12345');
USE mpp_test;
```

### Register Worker Nodes
On the coordinator node, register the worker nodes using master_add_node:
```SQL
-- On coordinator node (192.168.10.10)
SELECT * FROM master_add_node('192.168.10.11', 12345);
SELECT * FROM master_add_node('192.168.10.12', 12345);
```

### Create and Populate Distributed Table
```SQL
-- On coordinator node (192.168.10.10)
-- Create a distributed table partitioned by 'id' with 6 shards
CREATE TABLE test(id INTEGER, name TEXT) PARTITION BY(id) WITH BUCKETS 6;

-- Insert data (automatically distributed across workers)
INSERT INTO test VALUES(1, 'alex'), (2, 'jason'), (3, 'james');

-- Query the distributed table
SELECT * FROM test;
```

## TODO List
- [x] Create table
- [ ] Create table as 
- [x] Drop table 
- [x] Insert
- [x] Select
- [ ] Update
- [ ] Delete
- [ ] Alter table 
- [ ] Distributed transactions
- [ ] Shard rebalancing
- [ ] Global row id
- [x] Shard pruning
- [ ] Co-located tables
- [ ] Reference tables
- [ ] TPC-H
- [ ] TPC-DS 