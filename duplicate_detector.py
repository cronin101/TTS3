#!/usr/bin/python

# -*- coding: utf8 -*-

import codecs
import string
import unicodedata
import hashlib
from itertools import repeat, izip, combinations
from operator import add, itemgetter
from os import linesep
from multiprocessing import Pool
from counter import Counter
import sys

num_chunks = int(sys.argv[1])
chunks_needed = int(sys.argv[2])
hash_length = 512

words = []

def normalise_tuple(tup):
  a, b = tup
  mi = min(a, b)
  ma = max(a,b)
  return (mi, ma)

class DocumentSimHasher:
  def __init__(self, doc, num_chunks):
    self.num_chunks = num_chunks
    self.fingerprints = ((1 if bit == '1' else -1 for bit in f_print) for f_print in doc.fingerprints)

  def fingerprint_sum(self):
    result = ''.join(('1' if bit > 0 else '0' for bit in (sum (e) for e in izip(*self.fingerprints))))
    # Return fingerprint sum in parts
    chunk_size = hash_length / self.num_chunks
    chunks = [result[i:i+chunk_size] for i in range(0, len(result), chunk_size)]
    return chunks

class DocumentRetrieval:
  def __init__(self, document_path):
    with codecs.open(document_path, encoding='utf-8') as document:
      contents = document.read()
    contents = contents.strip()

    # Unicode normalisation *magic*
    normalised = unicodedata.normalize('NFD', contents).lower()

    # Remove 'Punctuation', 'Mark', 'Separator' and 'Other' unicode categories, and 'Math symbols' (<==>
    def should_drop(token):
      _category = unicodedata.category
      return _category(token)[0] in ['P', 'M', 'Z', 'C'] or _category(token) in ['Sm']

    self.tokens = [token for token in normalised if not should_drop(token)]
    words.extend(self.tokens)
    print_cache = {}
    def fingerprint(token):
      try:
        return print_cache[token]
      except KeyError:
        print_cache[token] = bin(int(hashlib.sha512(str(ord(token))).hexdigest(), 16))[2:].zfill(hash_length)
        return print_cache[token]

    self.fingerprints = [fingerprint(token) for token in self.tokens]

    self.contents = u''.join(self.tokens)
    self.filename = document_path.split('/')[-1]

class IndexRetrieval:
  def __init__(self, index_path):
    with open(index_path, 'r') as index:
      index = index.readlines()
    print "Index contains " + str(len(index)) + " documents."
    self.index = [DocumentRetrieval(path.strip()) for path in index]

def compute_simhash_chunk_tuple(doc):
  return (doc, DocumentSimHasher(doc, num_chunks).fingerprint_sum())

class SimHashDupeDetector:
  def __init__(self, read_documents, num_chunks):
    self.dupes = set([])
    buckets = {}
    for i in xrange(num_chunks):
      buckets[i] = {}

    p = Pool()
    for doc, simhash_chunks in p.map(compute_simhash_chunk_tuple, list(read_documents)):
      for i in xrange(num_chunks):
        bucket = buckets[i]
        chunk = simhash_chunks[i]
        bucket[chunk] = bucket.get(chunk, []) + [doc.filename]

    dupe_count = Counter([])
    for i in xrange(num_chunks):
      for c, bucket in buckets[i].iteritems():
        if len(bucket) > 1:
          dupe_count.update(map(normalise_tuple, combinations(bucket, 2)))
          #self.dupes.update(izip(repeat(bucket[0]), bucket[1:]))

    self.dupes.update(t for (t, c) in dupe_count.iteritems() if c >= chunks_needed)

class ExactDupeDetector:
  def __init__(self, read_documents):
    self.dupes = set([])
    seen = {}
    for document in read_documents:
      name = document.filename
      contents = document.contents
      seen[contents] = seen.get(contents, []) + [name]

    for c, bucket in seen.iteritems():
      if len(bucket) > 1:
        self.dupes.update(map(normalise_tuple, combinations(bucket, 2)))

class TuplePrinter:
  def __init__(self, filename, tuple_set):
    self.filename = filename
    self.tuple_set = tuple_set

  def dump(self):
    with open(self.filename, 'w') as output:
      output.write(linesep.join(' '.join([a,b]) for (a, b) in self.tuple_set) + linesep)

class TruthReader:
  def __init__(self, truth_files):
    self.dupes = set([])

    for truth_file in truth_files:
      with open(truth_file) as truth:
        self.dupes.update(normalise_tuple(tuple(line.strip().split(' '))) for line in truth.readlines())

def main():
  truth_files = [
    '/afs/inf.ed.ac.uk/user/s11/s1157979/Public/truth/t1.dup',
    '/afs/inf.ed.ac.uk/user/s11/s1157979/Public/truth/t2.dup',
    '/afs/inf.ed.ac.uk/user/s11/s1157979/Public/truth/t3.dup'
  ]
  truth = TruthReader(truth_files).dupes
  index = IndexRetrieval('./files.index').index
  dupes = ExactDupeDetector(index).dupes | SimHashDupeDetector(index, num_chunks).dupes
  TuplePrinter('./dupes', dupes).dump()

  #for w, c in sorted(Counter(words).items(), key=itemgetter(1)):
  #  print unicodedata.category(w) + " " + w.encode('utf-8') + " " + str(c)

  real_dupes = len(truth)
  reported_dupes = len(dupes)
  true_positives = len(truth & dupes)
  false_positives = len(dupes - truth)
  false_negatives = len(truth - dupes)

  print "True_P:" + str(true_positives) + " False_P: " + str(false_positives) + " False_N: " + str(false_negatives)

  recall = true_positives / float(real_dupes)
  precision = true_positives / float(true_positives + false_positives)
  f_1 = (2 * recall * precision) / (recall + precision)

  print "Requiring " + str(chunks_needed) + " out of " + str(num_chunks)+ " chunks of length " + str(hash_length/num_chunks) + " => Precision: " + str(precision) + " Recall: " + str(recall) + " F1: " + str(f_1)

if __name__ == '__main__':
  main()
