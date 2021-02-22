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

counter = Counter()

for index, (key, value) in enumerate(pypi):
    name = lexode.unpack(key)[0]
    summary, description = lexode.unpack(value)
    if index % 100 == 0:
        print(index, name, file=sys.stderr)

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
        continue

    # forward index
    counter_words = Counter(words)
    counter_words = tuple(counter_words.items())
    db.put(lexode.pack((SUBSPACE_FOWARD, uid)), lexode.pack((document, counter_words)))
    # Store stems with backward index
    for stem in stems:
        db.put(lexode.pack((SUBSPACE_BACKWARD, stem, uid)), b'')

    # store preview
    preview = ' '.join(document.split())[:1024]
    db.put(lexode.pack((SUBSPACE_PREVIEW, uid)), preview.encode('utf8'))

    # update stem counter
    counter += Counter(stems)

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
        db.put(key, b'')

db.put(lexode.pack((SUBSPACE_STEM_DOCUMENT_COUNTER,)), lexode.pack(tuple(counter.items())))


db.close()
pypi.close()
