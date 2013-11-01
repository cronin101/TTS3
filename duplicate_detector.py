#!/usr/bin/python

# -*- coding: utf8 -*-

import codecs
import string
import unicodedata
import hashlib
from itertools import repeat, izip
from operator import add
from os import linesep
from multiprocessing import Pool

num_chunks = 4

class DocumentSimHasher:
  def __init__(self, retrieved_document, num_chunks):
    self.num_chunks = num_chunks
    self.fingerprints = ((1 if bit == '1' else -1 for bit in f_print) for f_print in retrieved_document.fingerprints)

  def fingerprint_sum(self):
    result = ''.join(('1' if bit > 0 else '0' for bit in (sum (e) for e in izip(*self.fingerprints))))
    # Return fingerprint sum in parts
    chunk_size = 224 / self.num_chunks
    return [result[i:i+chunk_size] for i in range(0, len(result), chunk_size)]

class DocumentRetrieval:
  def __init__(self, document_path):
    with codecs.open(document_path, encoding='utf-8') as document:
      contents = document.read()
    contents = contents.strip()

    # Unicode normalisation *magic*
    _category = unicodedata.category
    normalised = unicodedata.normalize('NFD', contents).lower()

    # Remove 'Punctuation', 'Mark', 'Separator' and 'Other' unicode categories, and 'Math symbols' (<==>
    self.tokens = [token for token in normalised if not(_category(token)[0] in ['P', 'M', 'Z', 'C'] or _category(token) in ['Sm'])]
    print_cache = {}
    def fingerprint(token):
      try:
        return print_cache[token]
      except KeyError:
        print_cache[token] = bin(int(hashlib.sha224(str(ord(token))).hexdigest(), 16))[2:].zfill(224)
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

    for i in xrange(num_chunks):
      for c, bucket in buckets[i].iteritems():
        if len(bucket) > 1:
          self.dupes.update(izip(repeat(bucket[0]), bucket[1:]))


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
        self.dupes.update(izip(repeat(bucket[0]), bucket[1:]))

class TuplePrinter:
  def __init__(self, filename, tuple_set):
    with open(filename, 'w') as output:
      output.write(linesep.join(' '.join([a,b]) for (a, b) in tuple_set) + linesep)


def main():
  index = IndexRetrieval('./files.index').index
  dupes = ExactDupeDetector(index).dupes | SimHashDupeDetector(index, num_chunks).dupes
  TuplePrinter('./dupes', dupes)


if __name__ == '__main__':
  main()
