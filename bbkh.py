import asyncio
import string
import itertools
from time import time
from collections import Counter
from urllib.parse import parse_qs
from urllib.parse import quote

from lsm import LSM
from fdb import tuple as lexode
from fuzzywuzzy import fuzz


chars = string.ascii_lowercase + string.digits + "$ "

# TODO: maybe extend to trigram
BIGRAM = [''.join(x) for x in itertools.product(chars, chars)]
TRIGRAM = [''.join(x) for x in itertools.product(chars, chars, chars)]
ONE_HOT_ENCODER = sorted(BIGRAM)
BITS_COUNT = 2**11

# BITS_COUNT must be the first power of two that is bigger than
# ONE_HOT_ENCODER.
assert len(ONE_HOT_ENCODER) <= BITS_COUNT

# That is related to the merkletree serialization.
BYTES_COUNT = BITS_COUNT // 8


def ngram(string, n):
    return [string[i:i+n] for i in range(len(string)-n+1)]


def integer2booleans(integer):
    return [x == '1' for x in bin(integer)[2:].zfill(BITS_COUNT)]


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def merkletree(booleans):
    length = (2 * len(booleans) - 1)
    out = [None] * length
    index = length - 1
    booleans = list(reversed(booleans))
    while len(booleans) > 1:
        for boolean in booleans:
            out[index] = boolean
            index -= 1
        new = []
        for (right, left) in chunks(booleans, 2):
            value = right or left
            new.append(value)
        booleans = new
    assert index == 0
    out[0] = booleans[0]
    return out


def rotate(strg, n):
    return strg[n:] + strg[:n]


def bbkh(string):
    integer = 0
    string = ' '.join("$" + token + "$" for token in string.split())
    for n in [2]:
        for gram in ngram(string, n):
            hotbit = ONE_HOT_ENCODER.index(gram)
            hotinteger = 1 << hotbit
            integer = integer | hotinteger
    booleans = integer2booleans(integer)

    out = []
    for i, op in enumerate([lambda x: x, lambda x: list(reversed(x))]):
        r = 2
        for j in range(r):
            bits = rotate(op(booleans), BITS_COUNT // r * j)
            fuzz = ''.join('1' if x else '0' for x in bits)
            buzz = int(fuzz, 2)
            hash = buzz.to_bytes(BYTES_COUNT, 'little')
            out.append(hash)

    return out


def strinc(key):
    """Next bytes that are not prefix of KEY"""
    key = key.rstrip(b'\xff')
    if len(key) == 0:
        raise ValueError('Key must contain at least one byte not equal to 0xFF.')

    return key[:-1] + bytes([key[-1] + 1])


def search(db, space, query, distance, limit=10):
    scores = Counter()
    effort = 10
    keys = bbkh(query)
    for key in keys:

        near = lexode.pack((space, key, query))

        # select candidates foward
        candidates = db.iterator(start=near, stop=strinc(lexode.pack((space,))))
        for index, (key, _) in enumerate(candidates):
            if index == (limit * effort):
                break
            _, _, other = lexode.unpack(key)
            score = distance(query, other)
            if score > 0:
                if other not in scores:
                    scores[other] = score

        # select candidates backward
        candidates = db.iterator(stop=near, start=lexode.pack((space,)), reverse=True)
        for index, (key, _) in enumerate(candidates):
            if index == (limit * effort):
                break
            _, _, other = lexode.unpack(key)
            score = distance(query, other)
            if score > 0:
                if other not in scores:
                    scores[other] = score

    return scores.most_common(limit)
