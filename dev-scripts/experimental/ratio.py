import re, json, sys

def load(p):
    h = open(p).read()
    return json.loads(re.search(r'const UNIT_DATA = (\[.*?\n\]);', h, re.S).group(1))

def table(units):
    d = {}
    for u in units:
        k = (u['name'], u['target'])
        d[k] = d.get(k, 0) + u['duration']
    return d

ta, tb = table(load(sys.argv[1])), table(load(sys.argv[2]))
common = [k for k in ta if k in tb]
common.sort(key=lambda k: -max(ta[k], tb[k]))

print(f"\n{'unit':42} {'A':>7} {'B':>7} {'A/B':>6}")
for k in common[:15]:
    label = f"{k[0]} {k[1]}"[:41]
    r = ta[k] / tb[k] if tb[k] else 0
    print(f"{label:42} {ta[k]:7.1f} {tb[k]:7.1f} {r:6.2f}")

ratios = sorted(ta[k] / tb[k] for k in common if tb[k] > 1)
print(f"\nmedian ratio over {len(ratios)} units (B > 1s): {ratios[len(ratios)//2]:.2f}")
