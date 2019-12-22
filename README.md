# `CLODSS`: *Clo*se *D*ata-*S*tructures *S*tore #

`clodss` is a data-structures *on-disk* store with an API compatible with
`redis`. The goal is to develop a store with the simplicity of the `redis` API
which scales beyond memory size, allows harnessing multi-core processors, and
does not burden accesses with network latency.

## Roadmap:

Features will be implemented in the order of demand by the community. Please use
The issues section to vote for the next functionality to be developed.

- [ ] get
- [ ] set
- [ ] del
- [ ] keys
- [ ] scan
- [ ] lists
    - [x] llen
    - [x] lpop
    - [x] lpush
    - [x] rpop
    - [x] rpush
    - [x] lindex
    - [x] lrange
    - [ ] linsert
    - [ ] lrem
- [ ] hashmaps
    - [ ] hset
    - [ ] hget
    - [ ] hdel
    - [ ] hkeys
    - [ ] hvalues
    - [ ] hgetall
