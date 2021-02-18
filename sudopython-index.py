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
db = LSM('db.okvslite')

RE_WORD = re.compile(r"[A-Za-z]+")
RE_TOKENS = re.compile(r"[a-z0-9._-]+")

stemmer = Stemmer.Stemmer('english')
stemit = stemmer.stemWords


# def init_counter(db, key):
#     db[lexode.pack((key,))] = lexode.pack(())


# init_counter(db, SUBSPACE_STEM_DOCUMENT_COUNTER)

counter = Counter()

for index, (key, value) in enumerate(pypi):
    name = lexode.unpack(key)[0]
    value = lexode.unpack(value)[0]
    if index % 1_000 == 0:
        print(index, name, file=sys.stderr)

    # Prepare unique identifier
    uid = ulid.new().bytes

    # Prepare document
    document = name + " - " + value
    document = unidecode(value.lower())
    document = html2text(document)

    # Prepare forward index and store preview
    preview = ' '.join(document.split())[:1024]
    db[lexode.pack((SUBSPACE_PREVIEW, uid))] = preview.encode('utf8')

    # Prepare words
    words = [unidecode(x) for x in RE_WORD.findall(document) if 3 <= len(x) <= 255]
    # Store document with term frequency
    counter_words = Counter(words)
    counter_words = tuple(counter_words.items())
    db[lexode.pack((SUBSPACE_FOWARD, uid))] = lexode.pack((document, counter_words))

    # Yes, stemit will produce multiple times the same stem.
    stems = set(stemit(words))

    # Store stems with backward index
    for stem in stems:
        db[lexode.pack((SUBSPACE_BACKWARD, stem, uid))] = b''

    # update stem counter
    # counter = lexode.unpack(db[lexode.pack((SUBSPACE_STEM_DOCUMENT_COUNTER,))])
    # counter = dict(counter)
    # counter = Counter(counter)
    counter += Counter(stems)

    # Store "tokens" with bbkh
    tokens = set(unidecode(x) for x in RE_TOKENS.findall(document) if 3 <= len(x) <= 255)
    for token in tokens:
        bbkh.index(db, SUBSPACE_BBKH, token)

db[lexode.pack((SUBSPACE_STEM_DOCUMENT_COUNTER,))] = lexode.pack(tuple(counter.items()))


db.close()
pypi.close()
