# `calvinite`

[![Rust](https://github.com/epellis/calvinite/actions/workflows/rust.yml/badge.svg)](https://github.com/epellis/calvinite/actions/workflows/rust.yml)

Calvinite is a Work In Progress (WIP) Distributed SQL Database based on
the [Calvin](http://cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf) paper.

## TODO List

- [x] `SELECT * FROM foo WHERE id = 1` on single partition, single replica
- [x] Exception handling all the way back to the client
- [x] `SELECT * FROM foo WHERE id = 1` on single partition, multiple replica
- [ ] `SELECT * FROM foo WHERE id = 1` on multiple partition, single replica
- [ ] `SELECT * FROM foo WHERE id = 1` on multiple partition, multiple replica
- [ ] Implement raft log or use OSS library
- [ ] Integration test for strong transaction consistency
- [ ] `CREATE TABLE` and proper support for simple data types
- [ ] SQL Selects not on `id` (i.e. reconnaissance queries)
- [ ] Pass [TPC-C](https://tpc.org/tpcc/default5.asp)
- [ ] Application level "chaos testing" framework

## Aspirations

- Application level multitenancy (i.e. strong isolation without an OS level VM)
- In addition to SQL, support user specified transaction logic via scripting language (e.g. JS, Lua, ...)
- Integration with [Jepsen](https://github.com/jepsen-io/jepsen) model checker
- Allows number of partitions and number of replicas to be changed while remaining available
- [Open Tracing](https://opentracing.io/) integration
- Builtin online backup that streams logfiles to S3 (ala [Litestream](https://litestream.io/))
- HTTP/1.1 client using GRPC Web
- First party Kubernetes operator
