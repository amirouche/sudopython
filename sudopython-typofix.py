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
import fuzzywuzzy.fuzz
import Levenshtein as c
import levenshtein as py

SUBSPACE_PREVIEW = -1
SUBSPACE_BACKWARD = 0
SUBSPACE_FOWARD = 1
SUBSPACE_STEM_DOCUMENT_COUNTER = 2
SUBSPACE_BBKH = 3


db = LSM('db.okvslite')


def score(a, b):
    d = c.distance(a, b)
    if d > 3:
        return 0
    return fuzzywuzzy.fuzz.ratio(a, b)

start = time()
query = " ".join(sys.argv[1:])
query = ' '.join(unidecode(query.lower()).split())

top = bbkh.search(db, SUBSPACE_BBKH, query, score)
print(time() - start)

for name, score in top:
    print(score, name)


# TODO: use cursor, and properly close the database
# db.close()
