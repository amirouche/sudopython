import sys
from collections import Counter
from time import time

import lxml.html
import fuzzywuzzy.fuzz
import Levenshtein as c
import levenshtein as py


def c_distance(a, b):
    return -c.distance(a, b)

def py_distance(a, b):
    return -py.distance(a, b, 3)

def fw_distance(a, b):
    return fuzzywuzzy.fuzz.ratio(a, b)

def best(a, b):
    d = c.distance(a, b)
    if d > 3:
        return -float('inf')
    return fuzzywuzzy.fuzz.ratio(a, b)

distance = sys.argv[1]
if distance == "c":
    distance = c_distance
elif distance == "py":
    distance = py_distance
elif distance == "fw":
    distance = fw_distance
else:
    distance = best


with open('pypi-index.html') as f:
    index = lxml.html.parse(f)

names = index.xpath('//a/text()')

query = sys.argv[2]


start = time()
distances = Counter()

for name in names:
    distances[name] = distance(name, query)

print(time() - start)

for name, score in distances.most_common(10):
    print(score, name)
