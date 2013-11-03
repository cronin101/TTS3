import sys
from tuple_normaliser import normalise_tuple

class DupeReader:
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

  truth = DupeReader(truth_files).dupes
  detected = DupeReader([sys.argv[1]]).dupes

  real_dupes = len(truth)
  reported_dupes = len(detected)
  true_positives = len(truth & detected)
  false_positives = len(detected - truth)
  false_negatives = len(truth - detected)

  print "True_P:" + str(true_positives) + " False_P: " + str(false_positives) + " False_N: " + str(false_negatives)

  recall = true_positives / float(real_dupes)
  precision = true_positives / float(true_positives + false_positives)
  f_1 = (2 * recall * precision) / (recall + precision)

  print "Precision: " + str(precision) + " Recall: " + str(recall) + " F1: " + str(f_1)

if __name__ == '__main__':
  main()
