from mrjob.job import MRJob
from mrjob.step import MRStep
class CC_Len_MR(MRJob):
    def dummy_mapper(self, _, line):
        u, v = line.split()
        yield u, v
    
    
    def graph_combiner(self, u, neighbors):
        v_set = set()
        for v in neighbors:
            v_set |= set(v)
        yield u, list(v_set)
    def map_lengths(self, u, neighbors):
        yield u, len(neighbors)
    def reduce_lengths(self, u, l):
        yield u, sum(l)
    def steps(self):
        return [MRStep(mapper=self.dummy_mapper, reducer=self.graph_combiner),
                MRStep(mapper=self.map_lengths)]
if __name__== '__main__':
    CC_Len_MR.run()
