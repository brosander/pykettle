#!/usr/bin/python

import argparse
import os
import socket
import sys

os.sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))

from thirdParty.pydocs.unicode_csv import unicode_csv_reader
from thirdParty.python_java_datastream.data_output_stream import DataOutputStream
from thirdParty.python_java_datastream.data_input_stream import DataInputStream

# InputControlCode
STOP = 0
ROW_META = 1
ROW = 2

# OutputControlCode
SUCCESS = 0
ERROR = 1

# ValueMetaInterface types
TYPE_STRING = 2

# ValueMetaInterface storage types
TYPE_NORMAL = 0

# ValueMetaInterface trim types
TRIM_TYPE_NONE = 0

def writeString(dataOutputStream, string):
  #print('Writing: ' + string)
  chars = string.encode('utf-8')
  dataOutputStream.write_int(len(chars))
  for char in chars:
    dataOutputStream.write_byte(ord(char))

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='This script will send rows to a Kettle Transformation', usage='stream2kettle [options]', formatter_class = argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('-a', '--address', help = 'Address to connect to', default = '127.0.0.1')
  parser.add_argument('-p', '--port', type=int,help = 'Port to connect to', default = 9001)
  args = parser.parse_args()

  reader = unicode_csv_reader(iter(sys.stdin.readline, ''))

  socket = socket.socket()
  socket.connect((args.address, args.port))
  socketFile = socket.makefile()
  dataOutputStream = DataOutputStream(socketFile)
  dataInputStream = DataInputStream(socketFile)

  first = True
  for row in reader:
    if first:
      first = False
      #print('Writing header row: ' + str(row))
      dataOutputStream.write_byte(ROW_META)
      dataOutputStream.write_int(len(row))
      for val in row:
        dataOutputStream.write_int(TYPE_STRING) # Type
        dataOutputStream.write_int(TYPE_NORMAL) # Storage type
        writeString(dataOutputStream, val) # Name
        dataOutputStream.write_int(-1) # length
        dataOutputStream.write_int(-1) # precision
        writeString(dataOutputStream,'From Python') # origin
        writeString(dataOutputStream,'') # comments
        writeString(dataOutputStream,'') # conversion mask
        writeString(dataOutputStream,'.') # decimal symbol
        writeString(dataOutputStream,',') # grouping symbol
        writeString(dataOutputStream,'$') # currency symbol
        dataOutputStream.write_int(TRIM_TYPE_NONE) # trim type
        dataOutputStream.write_boolean(False) # case insensitive
        dataOutputStream.write_boolean(False) # sorted descending
        dataOutputStream.write_boolean(False) # output padding enabled
        dataOutputStream.write_boolean(False) # date format lenient
        writeString(dataOutputStream,'') # date format locale
        writeString(dataOutputStream,'') # date format time zone
        dataOutputStream.write_boolean(False) # lenient string to number
        socketFile.flush()
    else:
      #print('Writing row: ' + str(row))
      dataOutputStream.write_byte(ROW)
      for val in row:
        dataOutputStream.write_boolean(False) # Null value
        writeString(dataOutputStream,val) # Value
        socketFile.flush()
  
  dataOutputStream.write_byte(STOP)
  socketFile.flush()
  result = dataInputStream.read_byte()
  if result == SUCCESS:
    print('Success!')
  else:
    print(dataInputStream.read_utf())
