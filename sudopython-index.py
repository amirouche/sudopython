import multiprocessing
import asyncio
import sys
import re
from collections import Counter
from unidecode import unidecode
from lsm import LSM
from fdb import tuple as lexode
from html2text import HTML2Text
import ulid
import Stemmer
import bbkh
import plyvel
from concurrent import futures
from multicore import pool_for_each_par_map


SUBSPACE_PREVIEW = -1
SUBSPACE_BACKWARD = 0
SUBSPACE_FOWARD = 1
SUBSPACE_STEM_DOCUMENT_COUNTER = 2
SUBSPACE_BBKH = 3

handler = HTML2Text()
handler.ignore_links = True
handler.images_to_alt = True
html2text = handler.handle

pypi = LSM('pypi.okvslite')
db = plyvel.DB('sudopython.leveldb', create_if_missing=True)

RE_WORD = re.compile(r"[a-z]+")
RE_TOKENS = re.compile(r"[a-z0-9._-]+")

stemmer = Stemmer.Stemmer('english')
stemit = stemmer.stemWords

counter_stems = Counter()


def generator():
    for (key, value) in pypi:
        yield key, value


def index(args):
    key, value = args
    name = lexode.unpack(key)[0]
    summary, description = lexode.unpack(value)

    # Prepare unique identifier
    uid = ulid.new().bytes

    # Prepare document
    document = name + " - " + summary + " - " + description
    document = unidecode(document.lower())

    # Prepare words
    words = [unidecode(x) for x in RE_WORD.findall(document) if 3 <= len(x) <= 255]

    # Yes, stemit will produce multiple times the same stem.
    stems = set(stemit(words))

    if not stems:
        return name

    out = []
    # forward index
    counter_words = Counter(words)
    counter_words = tuple(counter_words.items())
    out.append((lexode.pack((SUBSPACE_FOWARD, uid)), lexode.pack((document, counter_words))))
    # Store stems with backward index
    for stem in stems:
        out.append((lexode.pack((SUBSPACE_BACKWARD, stem, uid)), b''))

    # store preview
    preview = ' '.join(document.split())[:1024]
    out.append((lexode.pack((SUBSPACE_PREVIEW, uid)), preview.encode('utf8')))

    # update stem counter
    counter = Counter(stems)

    # Store "tokens" with bbkh
    tokens = set(unidecode(x) for x in RE_TOKENS.findall(document) if 3 <= len(x) <= 255)
    for token in tokens:
        name = name.lower()
        tokens = sorted(set(''.join(x if x in bbkh.chars else ' ' for x in name).split()))
        string = ' '.join(token for token in tokens if len(token) > 1)

        if string.isspace():
            continue

        key = bbkh.bbkh(string)
        key = lexode.pack((b'foobar', key, name))
        out.append((key, b''))

    return name, counter, out


total = 0
def progress(args):
    global total, counter_stems
    name, counter, kvs = args
    if total % 10_000 == 0:
        print(total, name)
    total += 1
    counter_stems += counter
    for key, value in kvs:
        db.put(key, value)


async def main(loop):

    with futures.ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as pool:
        await pool_for_each_par_map(
            loop, pool, progress, index, generator()
        )

loop = asyncio.get_event_loop()
loop.run_until_complete(main(loop))
loop.close()


db.put(lexode.pack((SUBSPACE_STEM_DOCUMENT_COUNTER,)), lexode.pack(tuple(counter_stems.items())))


db.close()
pypi.close()
