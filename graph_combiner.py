#!/opt/conda/default/bin/python

# James Kelly, 8/2020
# Combiner utility for creating neighbor lists (adjacency list graph representation)

import sys

# replaces ''.join() for lists in python 3.8 (in order to run in 2.7)


def join_list(l, sep=" "):
    lstring = ""
    i = 0
    for n in l:
        if i != 0:
            lstring += sep + n
        else:
            lstring += n
        i += 1
    return lstring




if __name__ == '__main__':
    current_node = None
    neighbors = list()
    u = None
    for line in sys.stdin:
        line = line.strip()
        u, v = line.split('\t')
        if current_node == u:
            if v not in neighbors:
                neighbors.append(v)
        else:
            if current_node:
                nstring = join_list(neighbors)
                print(current_node + '\t' + nstring)
            neighbors = [v]
            current_node = u
    if u is not None:
        if current_node == u:
            nstring = join_list(neighbors)
            print(current_node + '\t' + nstring)



