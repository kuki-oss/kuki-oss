import re, json, sys
from collections import Counter

def load(p):
    h = open(p).read()
    return json.loads(re.search(r'const UNIT_DATA = (\[.*?\n\]);', h, re.S).group(1))

def keys(units):
    return Counter((u['name'], u['target']) for u in units)

a, b = sys.argv[1], sys.argv[2]
ka, kb = keys(load(a)), keys(load(b))

print(f"\nin {a.split('/')[-1]} but not in {b.split('/')[-1]}:")
for k, n in sorted((ka - kb).items()):
    print(f"   {k[0]:30} {k[1]:20} x{n}")

print(f"\nin {b.split('/')[-1]} but not in {a.split('/')[-1]}:")
for k, n in sorted((kb - ka).items()):
    print(f"   {k[0]:30} {k[1]:20} x{n}")
