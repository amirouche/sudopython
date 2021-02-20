import sys
from collections import Counter
from time import time

from lsm import LSM
import bbkh

from pathlib import Path
import lxml.html
import fuzzywuzzy.fuzz
import Levenshtein as c


def c_distance(a, b):
    return -c.distance(a, b)

def score(a, b):
    d = c.distance(a, b)
    if d > 3:
        return 0
    return fuzzywuzzy.fuzz.ratio(a, b)

with open('pypi-index.html') as f:
    index = lxml.html.parse(f)

names = index.xpath('/html/body/a/text()')

query = sys.argv[1]

# typofix over all the corpus
start = time()
scores = Counter()

for name in names:
    scores[name] = score(name, query)

top = scores.most_common(10)

print(time() - start)

for name, value in top:
    print(value, name)

# typofix over neighboor

db = Path('typofix.okvslite')
if db.exists():
    db.unlink()

db = LSM(str(db))

for index, name in enumerate(names):
    if (index % 10_000) == 0:
        print(index, name)
    bbkh.index(db, b'foobar', name)

start = time()
top = bbkh.search(db, b'foobar', query, score)
print(time() - start)

for name, value in top:
    print(value, name)
