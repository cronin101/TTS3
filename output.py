from os import linesep
import string

class TuplePrinter:
  def __init__(self, filename, tuple_set):
    self.filename = filename
    self.tuple_set = tuple_set

  def dump(self):
    with open(self.filename, 'w') as output:
      output.write(linesep.join(' '.join([a,b]) for (a, b) in self.tuple_set) + linesep)


