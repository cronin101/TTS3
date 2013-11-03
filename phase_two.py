#!/usr/bin/python

# -*- coding: utf8 -*-

from phase_one import DocumentRetrieval
from document_scorer import CosineScorer
from evaluator import DupeReader
from output import TuplePrinter

class CosineFilter:
  def __init__(self, dupes):
    def score(tup):
      prefix = '/afs/inf.ed.ac.uk/user/s11/s1157979/Public/train/'

      a, b = tup
      doc1 = DocumentRetrieval(prefix + a)
      doc2 = DocumentRetrieval(prefix + b)
      return CosineScorer(doc1, doc2).score()

    overlap_scores = [(t, score(t)) for t in dupes]
    mean_score = sum(score for (t, score) in overlap_scores) / float(len(overlap_scores))
    standard_dev = (sum(((score - mean_score) ** 2) for (t, score) in overlap_scores) / float(len(overlap_scores))) ** 0.5
    print mean_score
    print overlap_scores
    #cutoff = float(mean_score + 4.0) / 5.0
    cutoff = 0.9992
    print cutoff
    self.dupes = set(t for (t, score) in overlap_scores if score >= cutoff)

def main():
  phase_one_dupes = DupeReader(['./dupes']).dupes
  TuplePrinter('./filtered', CosineFilter(phase_one_dupes).dupes).dump()

if __name__ == "__main__":
  main()
