import re, json, sys
for path in sys.argv[1:]:
    h = open(path).read()
    units = json.loads(re.search(r'const UNIT_DATA = (\[.*?\n\]);', h, re.S).group(1))
    wall = max(u['start'] + u['duration'] for u in units)
    work = sum(u['duration'] for u in units)
    count = lambda n: sum(1 for u in units if u['name'] == n and u['target'] == '')
    print(f"\n{path.split('/')[-1]}")
    print(f"  wall {wall:.0f}s  work {work:.0f}s  units {len(units)}  work/wall {work/wall:.1f} cores")
    print(f"  aws-lc units {sum(1 for u in units if u['name'].startswith('aws-lc'))}  rustls {count('rustls')}  rustls-webpki {count('rustls-webpki')}  sqlx-core {count('sqlx-core')}  sqlx-postgres {count('sqlx-postgres')}")
    for u in sorted(units, key=lambda u: -u['duration'])[:5]:
        print(f"  {u['name']} {u['target']} {u['duration']:.1f}s")
