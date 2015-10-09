#!/usr/bin/env python

# InputControlCode
STOP = 0
ROW_META = 1
ROW = 2

# OutputControlCode
SUCCESS = 0
ERROR = 1

# ValueMetaInterface types
TYPE_STRING = 2
TYPE_INTEGER = 5

# ValueMetaInterface storage types
TYPE_NORMAL = 0

# ValueMetaInterface trim types
TRIM_TYPE_NONE = 0

class ValueMetaFactory(object):
  def __init__(self):
    self.factories = {}

  def create(self, metaType, name):
    return self.factories[metaType](name)

valueMetaFactory = ValueMetaFactory()

class ValueMetaBase(object):
  def __init__(self, typeInt, name):
    self.typeInt = typeInt
    self.storageType = TYPE_NORMAL
    self.name = name
    self.length = -1
    self.precision = -1
    self.origin = 'From Python'
    self.comments = ''
    self.conversionMask = ''
    self.decimalSymbol = '.'
    self.groupingSymbol = ','
    self.currencySymbol = '$'
    self.trimType = TRIM_TYPE_NONE
    self.caseInsensitive = False
    self.sortedDescending = False
    self.outputPaddingEnabled = False
    self.dateFormatLenient = False
    self.dateFormatLocale = ''
    self.dateFormatTimeZone = ''
    self.lenientStringToNumber = False

  def writeString(self, dataOutputStream, string):
    chars = string.encode('utf-8')
    dataOutputStream.write_int(len(chars))
    for char in chars:
      dataOutputStream.write_byte(ord(char))

  def writeMeta(self, output):
    output.write_int(self.typeInt)
    output.write_int(self.storageType)
    self.writeString(output, self.name)
    output.write_int(self.length)
    output.write_int(self.precision)
    self.writeString(output, self.origin)
    self.writeString(output, self.comments)
    self.writeString(output, self.conversionMask)
    self.writeString(output, self.decimalSymbol)
    self.writeString(output, self.groupingSymbol)
    self.writeString(output, self.currencySymbol)
    output.write_int(self.trimType)
    output.write_boolean(self.caseInsensitive)
    output.write_boolean(self.sortedDescending)
    output.write_boolean(self.outputPaddingEnabled)
    output.write_boolean(self.dateFormatLenient)
    self.writeString(output, self.dateFormatLocale)
    self.writeString(output, self.dateFormatTimeZone) # date format time zone
    output.write_boolean(self.lenientStringToNumber) # lenient string to number

  def writeObject(self, output, val):
    if val is None:
      output.write_boolean(True) # Null value
    else:
      output.write_boolean(False) # Null value
      self._writeObjectImpl(output, val)
      
class ValueMetaInteger(ValueMetaBase):
  def __init__(self, name):
    super(ValueMetaInteger, self).__init__(TYPE_INTEGER, name)

  def _writeObjectImpl(self, output, val):
    output.write_long(long(val))

class ValueMetaString(ValueMetaBase):
  def __init__(self, name):
    super(ValueMetaString, self).__init__(TYPE_STRING, name)

  def _writeObjectImpl(self, output, val):
    self.writeString(output, val)

valueMetaFactory.factories[TYPE_INTEGER] = lambda name: ValueMetaInteger(name)
valueMetaFactory.factories['integer'] = valueMetaFactory.factories[TYPE_INTEGER]
valueMetaFactory.factories['int'] = valueMetaFactory.factories[TYPE_INTEGER]

valueMetaFactory.factories[TYPE_STRING] = lambda name: ValueMetaString(name)
valueMetaFactory.factories['string'] = valueMetaFactory.factories[TYPE_STRING]
valueMetaFactory.factories['str'] = valueMetaFactory.factories[TYPE_STRING]
