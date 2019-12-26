# `CLODSS`: *Clo*se<sup>[*](#myfootnote1)</sup> *D*ata-*S*tructures *S*tore #

`clodss` is a data-structures *on-disk* store with an API largly compatible with
`redis`. The goal is to develop a store with the simplicity of the `redis` API
which scales beyond memory capacity, allows harnessing multi-core processors, and
does not burden accesses with network latency.

---
<a name="myfootnote1">*</a> *"Close"* as opposed to *"Remote"*.

## Roadmap:

Features will be implemented in the order of demand by the community. Please use
The issues section to vote for the next functionality to be developed.

- [x] get
- [x] set
- [x] del
- [ ] keys
- [ ] scan
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
- [ ] hashmaps
    - [ ] hset
    - [ ] hget
    - [ ] hdel
    - [ ] hkeys
    - [ ] hvalues
    - [ ] hgetall
