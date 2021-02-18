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

RE_WORD = re.compile(r"[A-Za-z]+")
RE_TOKENS = re.compile(r"[A-Za-z_-]+")
counter = Counter()

stemmer = Stemmer.Stemmer('english')
stemit = stemmer.stemWords

start = time()
query = " ".join(sys.argv[1:])
words = unidecode(query.lower()).split()
stems = set(stemit(words))

counter = lexode.unpack(db[lexode.pack((SUBSPACE_STEM_DOCUMENT_COUNTER,))])
counter = dict(counter)

# select candidates' seed.
seed_score = 2**64
seed = "python"

for stem in stems:
    score = counter[stem]
    seed, seed_score = (seed, seed_score) if seed_score < score else (stem, score)

# select candidates
key = lexode.pack((SUBSPACE_BACKWARD, stem))
candidates = [lexode.unpack(key)[2] for key, _ in db[key:strinc(key)]]

# filter candidates
scores = Counter()
for uid in candidates:
    document, counter = lexode.unpack(db[lexode.pack((SUBSPACE_FOWARD, uid))])
    counter = dict(counter)
    score = 0
    for word in words:
        if word in document:
            score += counter.get(word, 0.5)
        else:
            break
    else:
        scores[uid] = score

print(time() - start)

for uid, score in scores.most_common(10):
    preview = db[lexode.pack((SUBSPACE_PREVIEW, uid))].decode('utf8')
    print(score, preview)

db.close()
