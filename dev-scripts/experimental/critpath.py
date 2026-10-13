# critpath.py: usage: python3 critpath.py report.html
import re, json, sys
sys.setrecursionlimit(10000)

html = open(sys.argv[1]).read()
raw = re.search(r'const UNIT_DATA = (\[.*?\n\]);', html, re.S).group(1)
units = {u['i']: u for u in json.loads(raw)}

# Invert "X unblocked Y" into "Y waits on X".
# 'full' = Y waits for X to finish; 'rmeta' = Y only waits for X's frontend (codegen start).
preds = {i: [] for i in units}
for u in units.values():
    for v in u['unblocked_units']:
        preds[v].append((u['i'], 'full'))
    for v in u['unblocked_rmeta_units']:
        preds[v].append((u['i'], 'rmeta'))

def rmeta_offset(u):
    for name, s in (u['sections'] or []):
        if name == 'codegen':
            return s['start']
    return u['duration']

def is_aws(u):
    if u['name'].startswith('aws-lc'):
        return True
    return u['name'] in ('rustls', 'rustls-webpki') and any('aws' in f for f in u['features'])

def run(remove):
    dur = lambda u: 0.0 if remove(u) else u['duration']
    memo = {}
    def start(i):
        if i in memo: return memo[i]
        best, via = 0.0, None
        for p, kind in preds[i]:
            pu = units[p]
            t = start(p)[0] + (0.0 if remove(pu) else (rmeta_offset(pu) if kind == 'rmeta' else pu['duration']))
            if t > best: best, via = t, p
        memo[i] = (best, via)
        return memo[i]
    end_unit = max(units, key=lambda i: start(i)[0] + dur(units[i]))
    total = start(end_unit)[0] + dur(units[end_unit])
    path, i = [], end_unit
    while i is not None:
        u = units[i]
        path.append(f"{u['name']}{u['target']} {dur(u):.1f}s")
        i = start(i)[1]
    return total, path[::-1]

for label, rm in [("baseline", lambda u: False), ("no aws-lc", is_aws)]:
    total, path = run(rm)
    print(f"\n{label}: {total:.0f}s")
    print("  " + "\n  -> ".join(path))
