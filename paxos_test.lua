ambox = require('ambox')
paxos = require('paxos')

assert(paxos)
assert(paxos.accept)
assert(paxos.propose)
assert(paxos.makeseq)
assert(paxos.stats)

p = paxos_module()
assert(p)
assert(p.accept)
assert(p.propose)
assert(p.makeseq)
assert(p.stats)


