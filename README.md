[![Build Status](https://travis-ci.org/codomatech/clodss.svg?branch=master)](https://travis-ci.org/codomatech/clodss)
[![Code Quality Grade](https://www.code-inspector.com/project/2529/status/svg)](https://www.code-inspector.com/public/project/2529/clodss/dashboard)
[![Code Quality Score](https://www.code-inspector.com/project/2529/score/svg)](https://www.code-inspector.com/public/project/2529/clodss/dashboard)

# `CLODSS`: *Clo*se<sup>[*](#myfootnote1)</sup> *D*ata-*S*tructures *S*tore #

`clodss` is a data-structures *on-disk* store with an API largly compatible with
`redis`. The goal is to develop a store with the simplicity of the `redis` API
which scales beyond memory capacity, allows harnessing multi-core processors, and
does not burden accesses with network latency.
`clodss` is a product of [Codoma.tech](https://www.codoma.tech/).

---
<a name="myfootnote1">*</a> *"Close"* as opposed to *"Remote"*.

## Roadmap:

Features will be implemented in the order of demand by the community. Please use
The issues section to vote for the next functionality to be developed.

- [x] get
- [x] set
- [x] del
- [x] incr
- [x] incrby
- [x] decr
- [x] decrby
- [x] keys
- [x] scan
- [x] flushdb
- [x] expire
- [x] persist
- [x] lists
    - [x] llen
    - [x] lpop
    - [x] lpush
    - [x] rpop
    - [x] rpush
    - [x] lindex
    - [x] lset
    - [x] lrange
    - [x] linsert
    - [x] ltrim
    - [x] lrem
- [x] hashmaps
    - [x] hset
    - [x] hget
    - [x] hdel
    - [x] hkeys
    - [x] hvalues
    - [x] hgetall
    - [x] hmset
    - [x] hmget
