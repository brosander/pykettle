#!/usr/bin/env python

import argparse
import os
import socket
import sys

os.sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))

from rowMeta import RowMeta
from valueMeta import valueMetaFactory
from thirdParty.pydocs.unicode_csv import unicode_csv_reader
from thirdParty.python_java_datastream.data_output_stream import DataOutputStream
from thirdParty.python_java_datastream.data_input_stream import DataInputStream

# InputControlCode
STOP = 0

# OutputControlCode
SUCCESS = 0
ERROR = 1

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='This script will send rows to a Kettle Transformation', usage='stream2kettle [options]', formatter_class = argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('-a', '--address', help = 'Address to connect to', default = '127.0.0.1')
  parser.add_argument('-p', '--port', type=int,help = 'Port to connect to', default = 9001)
  parser.add_argument('-c', '--colTypes', help = 'Column types specified name:type,...')
  parser.add_argument('-t', '--defaultType', help = 'Type to assume', default = 'string')
  args = parser.parse_args()

  reader = unicode_csv_reader(iter(sys.stdin.readline, ''))

  socket = socket.socket()
  socket.connect((args.address, args.port))
  socketFile = socket.makefile()
  dataOutputStream = DataOutputStream(socketFile)
  dataInputStream = DataInputStream(socketFile)

  colTypeDict = {}
  if args.colTypes:
    for colType in args.colTypes.split(','):
      colTypeDict[colType.split(':')[0]] = colType.split(':')[1]

  first = True
  for row in reader:
    if first:
      first = False
      valueMetas = []
      for val in row:
        if val in colTypeDict:
          valueMetas.append(valueMetaFactory.create(colTypeDict[val], val))
        else:
          valueMetas.append(valueMetaFactory.create(args.defaultType, val))
      rowMeta = RowMeta(valueMetas)
      rowMeta.writeMeta(dataOutputStream)
      socketFile.flush()
    else:
      rowMeta.writeRow(dataOutputStream, row)
      socketFile.flush()
  
  dataOutputStream.write_byte(STOP)
  socketFile.flush()
  result = dataInputStream.read_byte()
  if result == SUCCESS:
    print('Success!')
  else:
    print(dataInputStream.read_utf())
