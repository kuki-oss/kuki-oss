import re, json, sys
from collections import Counter
h = open(sys.argv[1]).read()
units = json.loads(re.search(r'const UNIT_DATA = (\[.*?\n\]);', h, re.S).group(1))
blockers = {u['i']: [] for u in units}
for u in units:
    for v in u.get('unblocked_units', []) + u.get('unblocked_rmeta_units', []):
        blockers[v].append(u['i'])
by_i = {u['i']: u for u in units}
hist = Counter(min(len(b), 5) for b in blockers.values())
print("recorded blockers per unit (5 = 5 or more):", dict(sorted(hist.items())))
for name in sys.argv[2:]:
    for u in units:
        if u['name'] == name:
            names = [by_i[b]['name'] for b in blockers[u['i']]]
            print(f"{name} {u['target']}: {len(names)} recorded blockers -> {names}")
