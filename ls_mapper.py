#!/opt/conda/default/bin/python

# James Kelly, 8/2020
# Large-Star mapper from finding ConnectedComponents in Two-Phase algorithm (Kiveras, et al. 2014)

import sys

for line in sys.stdin:
    line = line.strip()
    u, v = line.split()
    if u and v:
        print(u + '\t' +  v)
        if u != v:
            print(v + '\t' +  u)



