# `calvinite`

Calvinite is a Work In Progress (WIP) Distributed SQL Database based on the [Calvin](http://cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf) paper.

## Aspirations
- Application level multitenancy (i.e. strong isolation without an OS level VM)
- In addition to SQL, support user specified transaction logic via scripting language (e.g. JS, Lua, ...)
- Integration with [Jepsen](https://github.com/jepsen-io/jepsen) model checker
- Allows number of partitions and number of replicas to be changed while remaining available
- [Open Tracing](https://opentracing.io/) integration
- Builtin online backup that streams logfiles to S3 (ala [Litestream](https://litestream.io/))
