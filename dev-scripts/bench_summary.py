import re, json, sys, glob, statistics as s
out = sys.argv[1]

def load(p):
    h = open(p).read()
    return json.loads(re.search(r'const UNIT_DATA = (\[.*?\n\]);', h, re.S).group(1))

def side(tag):
    walls, works, counts = [], [], set()
    for p in sorted(glob.glob(f"{out}/{tag}-*.html")):
        u = load(p)
        walls.append(max(x['start'] + x['duration'] for x in u))
        works.append(sum(x['duration'] for x in u))
        counts.add(len(u))
    return walls, works, counts

a, b = side('a'), side('b')
for tag, (w, k, c) in (('A', a), ('B', b)):
    if not w:
        sys.exit(f"no successful builds for {tag}")
    print(f"{tag} ({open(f'{out}/{tag.lower()}.dir').read().strip()})")
    print(f"   walls  {[round(x) for x in w]}  median {s.median(w):.0f}s  min {min(w):.0f}  max {max(w):.0f}")
    print(f"   work   median {s.median(k):.0f}s   units {sorted(c)}")

diff = s.median(a[0]) - s.median(b[0])
print(f"\nmedian wall: A - B = {diff:+.0f}s ({diff / s.median(a[0]) * 100:+.0f}% of A)")
if max(b[0]) < min(a[0]) or max(a[0]) < min(b[0]):
    print("ranges do not overlap: the difference is likely real")
else:
    print("ranges overlap: inconclusive, add more rounds or check the vmstat logs")
