# -*- coding: utf8 -*-

from counter import Counter

class CosineScorer:
  def __init__(self, doc1, doc2):
    self.w1 = Counter(doc1.tokens)
    self.w2 = Counter(doc2.tokens)
    self.w  = set(doc1.tokens) | set(doc2.tokens)

  def score(self):
    w1          = self.w1
    w2          = self.w2
    w           = self.w
    numerator   = sum(w1[word] * w2[word] for word in w)
    denominator = (sum(w1[word] ** 2 for word in w) ** 0.5) * (sum(w2[word] ** 2 for word in w) ** 0.5)

    return float(numerator) / float(denominator)


