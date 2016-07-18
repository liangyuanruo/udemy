# -*- coding: utf-8 -*-
"""
Created on Sun Jul 17 20:08:28 2016

@author: yuanruoliang
"""

from mrjob.job import MRJob

class MRRatingCounter(MRJob):
    def mapper(self, key, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield rating, 1
        
    def reducer(self, rating, occurences):
        yield rating, sum(occurences)
        
if __name__ == '__main__':
    MRRatingCounter.run()
    
    #To execute, run in terminal console:
    #!python RatingCounter.py data/ml-100k/u.data