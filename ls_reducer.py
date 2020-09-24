#!/opt/conda/default/bin/python

# Large-Star Reducer from Two-Phase algorithm for connected components (Kiveras, et al. 2014)
#from operator import itemgetter
import sys

for line in sys.stdin:
    line = line.strip()
    u, neighbors = line.split('\t')
    u_val = int(u)
    #print(u + '\t' + neighbors)
    neighbors = [int(v) for v in neighbors.split()]
    m = min(neighbors + [u_val])
    for v in neighbors:
        if v >= u_val:
            print(str(v) + '\t' + str(m))
    




