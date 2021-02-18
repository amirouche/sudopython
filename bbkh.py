import string
import itertools
from time import time
from collections import Counter
from urllib.parse import parse_qs
from urllib.parse import quote

from lsm import LSM
from fdb import tuple as lexode
from fuzzywuzzy import fuzz


HASH_SIZE = 2**11  # TODO: how is that set?
BBKH_LENGTH = int(HASH_SIZE * 2 / 8)  # TODO: how is that computed?
chars = string.ascii_lowercase + string.digits + "-_.$ "

# TODO: maybe extend to trigram
ONE_HOT_ENCODER = sorted([''.join(x) for x in itertools.product(chars, chars)])

spacy = str.maketrans(string.punctuation, ' '*len(string.punctuation))


def ngram(string, n):
    return [string[i:i+n] for i in range(len(string)-n+1)]


def integer2booleans(integer):
    return [x == '1' for x in bin(integer)[2:].zfill(HASH_SIZE)]


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def merkletree(booleans):
    assert len(booleans) == HASH_SIZE
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
            new.append(value)
        booleans = new
    return out


def bbkh(string):
    integer = 0
    string = ' '.join("$" + token + "$" for token in string.split())
    for gram in ngram(string, 2):
        hotbit = ONE_HOT_ENCODER.index(gram)
        hotinteger = 1 << hotbit
        integer = integer | hotinteger
    booleans = integer2booleans(integer)
    tree = merkletree(booleans)
    fuzz = ''.join('1' if x else '0' for x in tree)
    buzz = int(fuzz, 2)
    hash = buzz.to_bytes(BBKH_LENGTH, 'big')
    return hash


def index(db, space, name):
    name = name.lower()
    tokens = sorted(set(name.translate(spacy).split()))
    string = ' '.join(token for token in tokens if len(token) > 1)
    if string.isspace():
        return
    key = bbkh(string)
    db[lexode.pack((space, key, name))] = b''


def strinc(key):
    key = key.rstrip(b'\xff')
    if len(key) == 0:
        raise ValueError('Key must contain at least one byte not equal to 0xFF.')

    return key[:-1] + bytes([key[-1:] + 1])


def search(db, space, string, limit=100):
    distances = Counter()

    fuzzy = lexode.pack((space, bbkh(string),))

    for (index, (key, value)) in enumerate(db[fuzzy:strinc(space)]):
        if index == limit:
            break

        key, label = lexode.unpack(key)
        delta = fuzz.ratio(string, label)
        distances[label] = delta

    for (index, (key, value)) in enumerate(db[fuzzy:space]):
        if index == limit:
            break

        key, label = lexode.unpack(key)
        delta = fuzz.ratio(string, label)
        distances[label] = delta

    return reversed(distances.most_common(limit))
