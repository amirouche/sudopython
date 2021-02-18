import sys
import re
from collections import Counter
from unidecode import unidecode
from lsm import LSM
from fdb import tuple as lexode
import ulid
import Stemmer
import bbkh
from time import time
import fuzzywuzzy


SUBSPACE_PREVIEW = -1
SUBSPACE_BACKWARD = 0
SUBSPACE_FOWARD = 1
SUBSPACE_STEM_DOCUMENT_COUNTER = 2
SUBSPACE_BBKH = 3


def strinc(key):
    """Next bytes that are not prefix of KEY"""
    key = key.rstrip(b'\xff')
    if len(key) == 0:
        raise ValueError('Key must contain at least one byte not equal to 0xFF.')

    return key[:-1] + bytes([key[-1] + 1])


db = LSM('db.okvslite')


start = time()
query = " ".join(sys.argv[1:])
token = ' '.join(unidecode(query.lower()).split())
hash = bbkh.bbkh(token)
near = lexode.pack((SUBSPACE_BBKH, hash, token))

scores = Counter()

# select candidates
candidates = db[near:strinc(lexode.pack((SUBSPACE_BBKH,)))]

for index, (key, _) in enumerate(candidates):
    if index == 100:
        break
    _, _, other = lexode.unpack(key)
    scores[other] = fuzzywuzzy.fuzz.ratio(token, other)

candidates = db[near:lexode.pack((SUBSPACE_BBKH,))]

for index, (key, _) in enumerate(candidates):
    if index == 100:
        break
    _, _, other = lexode.unpack(key)
    scores[other] = fuzzywuzzy.fuzz.ratio(token, other)

print(time() - start)

for token, score in scores.most_common(10):
    if score > 60:
        print(score, token)


# TODO: use cursor, and properly close the database
# db.close()
