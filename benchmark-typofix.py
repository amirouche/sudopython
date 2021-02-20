from aiostream import pipe, stream
import asyncio
from concurrent import futures
import sys
from collections import Counter
from time import time
import multiprocessing
import plyvel

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

options = dict(
    # readonly=True,
    multiple_processes=False,
    transaction_log=False,
    page_size=1024 ** 2,
    block_size=10 * 1024 ** 2,    
)

db = Path('typofix.okvslite')
if db.exists():
    db.unlink()

def index(name):
    name = name.lower()
    tokens = sorted(set(''.join(x if x in bbkh.chars else ' ' for x in name).split()))
    string = ' '.join(token for token in tokens if len(token) > 1)

    if string.isspace():
        return None, None

    key = bbkh.bbkh(string)

    return name, key


async def pool_for_each_par_map(loop, pool, f, p, iterator):
    zx = stream.iterate(iterator)
    zx = zx | pipe.map(lambda x: loop.run_in_executor(pool, p, x))
    async with zx.stream() as streamer:
        limit = pool._max_workers
        unfinished = []
        while True:
            tasks = []
            for i in range(limit):
                try:
                    task = await streamer.__anext__()
                except StopAsyncIteration:
                    limit = 0
                else:
                    tasks.append(task)
            tasks = tasks + list(unfinished)
            assert len(tasks) <= pool._max_workers
            if not tasks:
                break
            finished, unfinished = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for finish in finished:
                out = finish.result()
                f(out)
            limit = len(finished)

total = 0
size = 0

# db = LSM('typofix.okvslite', **options)
db = plyvel.DB('typofix.okvslite', create_if_missing=True)

def progress(args):
    global total, size

    name, key = args

    total += 1
    
    if name is None:
        return
    
    key = bbkh.lexode.pack((b'foobar', key, name))
    if len(key) > size:
        print("new max key", len(key))
        size = len(key)

    db.put(key, b'')
    
    if (total % 1_000) == 0:
        print(total, name, size, len(key), int(time() - start))





async def main(loop):

    with futures.ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as pool:
        await pool_for_each_par_map(
            loop, pool, progress, index, names
        )

loop = asyncio.get_event_loop()
loop.run_until_complete(main(loop))
loop.close()



start = time()
top = bbkh.search(db, b'foobar', query, score)
print(time() - start)

for name, value in top:
    print(value, name)
