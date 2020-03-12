'''
CLODSS: Close* Data-Structures Store

clodss is a data-structures on-disk store with an API largly compatible with
redis. The goal is to develop a store with the simplicity of the redis API
which scales beyond memory capacity, allows harnessing multi-core processors,
and does not burden accesses with network latency.

Homepage: https://github.com/codomatech/clodss
Author: Codoma.tech Advanced Technologies
        https://www.codoma.tech/
License: BSD

'''

from .clodss import StrictRedis
