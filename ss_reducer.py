#!/opt/conda/default/bin/python

# Large-Star Reducer from Two-Phase algorithm for connected components (Kiveras, et al. 2014)
#from operator import itemgetter
import sys

for line in sys.stdin:
    line = line.strip()
    u, neighbors = line.split('\t')
    u_val = int(u)
    min_val = u_val
    new_neighs = neighbors.split()
    if new_neighs[0] != u:
        new_neighs.append(u)
    for v in new_neighs:
        if int(v) < min_val:
            min_val = int(v)
    for v in new_neighs:
        if (v != str(min_val)) or (u == v):
            print(v + '\t' +  str(min_val))

