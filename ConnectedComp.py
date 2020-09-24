from mrjob.job import MRJob
from mrjob.step import MRStep

class ConnectedComp(MRJob):
    def ls_mapper(self, b, line):
        u, v = line.split()     
        yield u, v
        if v != u:
            yield v, u
   
    
    def ls_reducer(self, u, neighbors):
        u_val = int(u)
        min_val = u_val
        for n in neighbors:
            new_neighs = n  # to get from generator
        for v in new_neighs:
            if int(v) < min_val:
                min_val = int(v)
        for v in new_neighs:  # to assign each neighbor strictly greater to min
            if v > u_val:
                if v != str(min_val):
                    yield v, str(min_val)



    def graph_combiner(self, u, neighbors):
        v_set = list()
        for v in neighbors:
            if v not in v_set:
                v_set.append(v)
        yield u, v_set 


    def ss_mapper(self, u, v):
        if int(v) <= int(u):
            yield u, v
        else:
            yield v, u


    def ss_reducer(self, u, neighbors): 
        u_val = int(u)
        min_val = u_val
        new_neighs = list()
        for v in neighbors:   # TODO(sloppy): to get value from generator, only 1x 
            new_neighs = v
        new_neighs.append(u)
        for v in new_neighs:  # to get min val
            if int(v) < min_val:
                min_val = int(v)
        
        for v in new_neighs:  # to assign each neighbor strictly greater to min
            if v != str(min_val):
                yield int(v), min_val

        
    def steps(self):
        return [MRStep(mapper=self.ls_mapper, reducer=self.graph_combiner),
                MRStep(reducer=self.ls_reducer),
                MRStep(mapper=self.ss_mapper, reducer=self.graph_combiner),
                MRStep(reducer=self.ss_reducer)]
        


if __name__=='__main__':
    ConnectedComp.run()
