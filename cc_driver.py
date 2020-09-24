# main driver program for ConnectedComponents
import sys
import os


"""
def remove_dups(out_file):
    found_table = dict()
    f = open(out_file, 'r')
    for line in f.readlines():
        x, y = line.split()
        if x in found_table.keys():
            if y not in found_table[x]:
                found_table[x].append(y)
        else:
            found_table[x] = [y]
    f.close()
    f = open(out_file, 'w')
    for x in found_table.keys():
        for y in found_table[x]:
            f.write(x + '\t' + y + '\n')
    f.close()
"""






if __name__ == '__main__':
    
    converged = False
    max_iters = 10
    diff_lens = 1
    i = 0
    base_file = sys.argv[1]
    in_file = base_file
    while (i < max_iters) and not converged:
        file_label = '1' if (i % 2 == 0) else '2'
        slide = -4 if (i == 0) else -5
        out_file = in_file[:slide] + file_label + ".txt"
        os.system('python2 ConnectedComp.py ' + in_file + ' > ' + out_file)
        
        #remove duplicates
        #remove_dups(out_file)
        
        os.system('python2 CC_Converge_EvalMR.py ' + out_file +  ' > graph_lens.txt')
        in_file = out_file
        new_lens = 0

        with open('graph_lens.txt') as f:
            for line in f.readlines():
                neighbor_len = line.strip().split()[1]
                new_lens += int(neighbor_len)
        if diff_lens == new_lens:
            converged = True
        else:
            diff_lens = new_lens
        i += 1
        print(str(i) + '\n\n')
