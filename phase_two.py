#!/usr/bin/python

# -*- coding: utf8 -*-

from phase_one import IndexRetrieval, DocumentRetrieval
from document_scorer import CosineScorer
from evaluator import DupeReader
from output import TuplePrinter
from multiprocessing import Pool

def score(tup):
  a, b = tup
  doc1 = DocumentRetrieval(a)
  doc2 = DocumentRetrieval(b)
  return (tup, CosineScorer(doc1, doc2).score())

class CosineFilter:
  def __init__(self, dupes):

    overlap_scores = Pool().map(score, dupes)#[(t, score(t)) for t in dupes]
    mean_score = sum(score for (t, score) in overlap_scores) / float(len(overlap_scores))
    standard_dev = (sum((score - mean_score) ** 2 for (t, score) in overlap_scores) / float(len(overlap_scores))) ** 0.5
    cutoff = mean_score
    self.dupes = map(lambda (a, b): (a.split('/')[-1], b.split('/')[-1]), set(t for (t, score) in overlap_scores if score >= cutoff))

def main(location="./phase_two_out"):
  phase_one_dupes = DupeReader(['./phase_one_out']).dupes
  phase_two_dupes = CosineFilter(phase_one_dupes).dupes
  TuplePrinter(location, phase_two_dupes).dump()

if __name__ == "__main__":
  main()
