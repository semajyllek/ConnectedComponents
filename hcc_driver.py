# main Hadoop driver program for ConnectedComponents
import sys
import os



if __name__ == '__main__':
    
    converged = False
    max_iters = 7
    diff = -1
    i = 0
    in_file = sys.argv[1]
    old_lens = None
    while (i < max_iters) and not converged:
        os.system('hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
                -files gs://graph_bucket1/ls_mapper.py,gs://graph_bucket1/graph_combiner.py  \
                -mapper ls_mapper.py   \
                -reducer graph_combiner.py  \
                -input ' + in_file + '  -output gs://graph_bucket1/graph_output' 
                )

        if i > 0:
            os.system('gsutil -m rm -r gs://graph_bucket1/graph_output4')


        os.system('hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
                -files gs://graph_bucket1/ls_reducer.py  \
                -mapper "/bin/sh -c \"cat\""  \
                -reducer ls_reducer.py  \
                -input gs://graph_bucket1/graph_output  \
                -output gs://graph_bucket1/graph_output2' 
                )



        os.system('hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar  \
                -files gs://graph_bucket1/ss_mapper.py,gs://graph_bucket1/graph_combiner.py  \
                -mapper ss_mapper.py  \
                -reducer graph_combiner.py  \
                -input gs://graph_bucket1/graph_output2  \
                -output gs://graph_bucket1/graph_output3' 
                )



        os.system('hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar  \
                -files gs://graph_bucket1/ss_reducer.py  \
                -mapper "/bin/sh -c \"cat\""  \
                -reducer ss_reducer.py  \
                -input gs://graph_bucket1/graph_output3  \
                -output gs://graph_bucket1/graph_output4' 
                )


        
        in_file = 'gs://graph_bucket1/graph_output4'
        os.system('gsutil -m rm -r gs://graph_bucket1/graph_output')
        os.system('gsutil -m rm -r gs://graph_bucket1/graph_output2')
        os.system('gsutil -m rm -r gs://graph_bucket1/graph_output3')
       


        # evaluate for convergence by difference in sum of neighbor lengths
        os.system('hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
                -files gs://graph_bucket1/graph_combiner.py  \
                -mapper "/bin/sh -c \"cat\""  \
                -reducer graph_combiner.py  \
                -input gs://graph_bucket1/graph_output4  \
                -output gs://graph_bucket1/graph_lens1'
                )

        os.system('hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
                -files gs://graph_bucket1/length_mapper.py  \
                -mapper "/bin/sh -c \"cat\""  \
                -reducer length_mapper.py  \
                -input gs://graph_bucket1/graph_lens1  \
                -output gs://graph_bucket1/graph_lens2'
                )
    
        
        os.system('gsutil cat gs://graph_bucket1/graph_lens2/* > graph_lens2.txt')
        os.system('gsutil cat gs://graph_bucket1/graph_lens1/* > graph_lens1.txt')
        new_lens = {}
        
        
        with open('graph_lens2.txt') as f:
            for line in f.readlines():
                pair = line.strip().split()
                new_lens[pair[0]] = pair[1]
        f.close()
        

        if old_lens:
            print(len(old_lens))
            print(len(new_lens))
            diff = 0
            for k in old_lens.keys():
                if old_lens[k] != new_lens[k]:
                    diff += 1
        
        if diff == 0:
            converged = True
        old_lens = new_lens
    
        os.system('gsutil -m rm -r gs://graph_bucket1/graph_lens1')
        os.system('gsutil -m rm -r gs://graph_bucket1/graph_lens2')
    
        i += 1
        print('\n\n' + 'Iteration: ' + str(i) +  '\tDiff: ' + str(diff) + '\n\n')
    


    # get components
    comp_roots = []
    with open('graph_lens1.txt') as f:
        for line in f.readlines():
            neighbor = int(line.strip().split()[1])
            if neighbor not in comp_roots:
                comp_roots.append(neighbor)

    num_comp = len(comp_roots)
    print("Number of components: " + str(num_comp))


    # save component roots for testing
    f = open('final_hadoop_comps.txt', 'w')
    for r in sorted(comp_roots):
        f.write(str(r) + '\n')
    f.close()









