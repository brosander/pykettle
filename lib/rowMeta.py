#!/usr/bin/env python

# InputControlCode
ROW_META = 1
ROW = 2

class RowMeta(object):
  def __init__(self, valueMetas = []):
    self.valueMetas = valueMetas
    
  def writeMeta(self, output):
    output.write_byte(ROW_META)
    output.write_int(len(self.valueMetas))
    for valueMeta in self.valueMetas:
      valueMeta.writeMeta(output)

  def writeRow(self, output, row):
    output.write_byte(ROW)
    for idx, val in enumerate(row):
      self.valueMetas[idx].writeObject(output, val)
