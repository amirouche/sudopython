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
ONE_HOT_ENCODER = sorted(BIGRAM + TRIGRAM)
BITS_COUNT = 2**16

# BITS_COUNT must be the first power of two that is bigger than
# ONE_HOT_ENCODER.
assert len(ONE_HOT_ENCODER) <= BITS_COUNT

# That is related to the merkletree serialization.
BYTES_COUNT = (2 * BITS_COUNT) // 8


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
    out = [False] * length
    index = length - 1
    booleans = list(reversed(booleans))
    while len(booleans) > 1:
        for boolean in booleans:
            out[index] = boolean
            index -= 1
        new = []
        for (right, left) in chunks(booleans, 2):
            value = right or left
            # new.append(value)
            # TODO: maybe this is better:
            new.insert(0, value)
        booleans = new
    return out


def bbkh(string):
    integer = 0
    string = ' '.join("$" + token + "$" for token in string.split())
    for n in [2, 3]:
        for gram in ngram(string, n):
            hotbit = ONE_HOT_ENCODER.index(gram)
            hotinteger = 1 << hotbit
            integer = integer | hotinteger
    booleans = integer2booleans(integer)
    tree = merkletree(booleans)
    fuzz = ''.join('1' if x else '0' for x in tree)
    buzz = int(fuzz, 2)
    assert buzz <= 2 ** (BYTES_COUNT * 8)
    hash = buzz.to_bytes(BYTES_COUNT, 'big')
    return hash


def index(db, space, name):
    name = name.lower()
    tokens = sorted(set(''.join(x if x in chars else ' ' for x in name).split()))
    string = ' '.join(token for token in tokens if len(token) > 1)
    if string.isspace():
        return
    key = bbkh(string)
    db[lexode.pack((space, key, name))] = b''


def strinc(key):
    """Next bytes that are not prefix of KEY"""
    key = key.rstrip(b'\xff')
    if len(key) == 0:
        raise ValueError('Key must contain at least one byte not equal to 0xFF.')

    return key[:-1] + bytes([key[-1] + 1])


def search(db, space, query, distance, limit=10):
    hash = bbkh(query)
    near = lexode.pack((space, hash, query))

    scores = Counter()

    # select candidates foward
    candidates = db[near:strinc(lexode.pack((space,)))]
    for index, (key, _) in enumerate(candidates):
        if index == (limit * 10):
            break
        _, _, other = lexode.unpack(key)
        score = distance(query, other)
        if score > 65:  # depends on fuzzywuzzy and wild approximation
            scores[other] = score

    # select candidates backward
    candidates = db[near:lexode.pack((space,))]
    for index, (key, _) in enumerate(candidates):
        if index == (limit * 10):
            break
        _, _, other = lexode.unpack(key)
        score = distance(query, other)
        if score > 65:
            scores[other] = score

    return scores.most_common(limit)
