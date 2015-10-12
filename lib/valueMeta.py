#!/usr/bin/env python

# ValueMetaInterface types
TYPE_NUMBER = 1
TYPE_STRING = 2
TYPE_DATE = 3
TYPE_BOOLEAN = 4
TYPE_INTEGER = 5
TYPE_BIGNUMBER = 6
TYPE_BINARY = 8

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
      
class ValueMetaNumber(ValueMetaBase):
  def __init__(self, name):
    super(ValueMetaNumber, self).__init__(TYPE_NUMBER, name)

  def _writeObjectImpl(self, output, val):
    output.write_double(output, float(val))

class ValueMetaString(ValueMetaBase):
  def __init__(self, name):
    super(ValueMetaString, self).__init__(TYPE_STRING, name)

  def _writeObjectImpl(self, output, val):
    self.writeString(output, str(val))

#Expects to write object as milliseconds since epoch (long)
class ValueMetaDate(ValueMetaBase):
  def __init__(self, name):
    super(ValueMetaDate, self).__init__(TYPE_DATE, name)

  def _writeObjectImpl(self, output, val):
    output.write_long(output, long(val))

class ValueMetaBoolean(ValueMetaBase):
  def __init__(self, name):
    super(ValueMetaBoolean, self).__init__(TYPE_BOOLEAN, name)

  def _writeObjectImpl(self, output, val):
    output.write_boolean(output, True if val else False)

class ValueMetaInteger(ValueMetaBase):
  def __init__(self, name):
    super(ValueMetaInteger, self).__init__(TYPE_INTEGER, name)

  def _writeObjectImpl(self, output, val):
    output.write_long(long(val))

#Serializes number of arbitrary size, precision as string to be used in Java's BigDecimal constructor
class ValueMetaBigNumber(ValueMetaBase):
  def __init__(self, name):
    super(ValueMetaBigNumber, self).__init__(TYPE_BIGNUMBER, name)

  def _writeObjectImpl(self, output, val):
   self.writeString(str(val))

#Writes byte array
class ValueMetaBinary(ValueMetaBase):
  def __init__(self, name):
    super(ValueMetaBinary, self).__init__(TYPE_BINARY, name)

  def _writeObjectImpl(self, output, val):
    output.write_int(len(val))
    for byte in val:
      output.write_byte(byte)

valueMetaFactory.factories[TYPE_NUMBER] = lambda name: ValueMetaNumber(name)
valueMetaFactory.factories['number'] = valueMetaFactory.factories[TYPE_NUMBER]
valueMetaFactory.factories['num'] = valueMetaFactory.factories[TYPE_NUMBER]

valueMetaFactory.factories[TYPE_STRING] = lambda name: ValueMetaString(name)
valueMetaFactory.factories['string'] = valueMetaFactory.factories[TYPE_STRING]
valueMetaFactory.factories['str'] = valueMetaFactory.factories[TYPE_STRING]

valueMetaFactory.factories[TYPE_DATE] = lambda name: ValueMetaDate(name)
valueMetaFactory.factories['date'] = valueMetaFactory.factories[TYPE_DATE]

valueMetaFactory.factories[TYPE_BOOLEAN] = lambda name: ValueMetaBoolean(name)
valueMetaFactory.factories['boolean'] = valueMetaFactory.factories[TYPE_BOOLEAN]
valueMetaFactory.factories['bool'] = valueMetaFactory.factories[TYPE_BOOLEAN]

valueMetaFactory.factories[TYPE_INTEGER] = lambda name: ValueMetaInteger(name)
valueMetaFactory.factories['integer'] = valueMetaFactory.factories[TYPE_INTEGER]
valueMetaFactory.factories['int'] = valueMetaFactory.factories[TYPE_INTEGER]

valueMetaFactory.factories[TYPE_BIGNUMBER] = lambda name: ValueMetaBigNumber(name)
valueMetaFactory.factories['bigNumber'] = valueMetaFactory.factories[TYPE_BIGNUMBER]
valueMetaFactory.factories['bigNum'] = valueMetaFactory.factories[TYPE_BIGNUMBER]

valueMetaFactory.factories[TYPE_BINARY] = lambda name: ValueMetaBinary(name)
valueMetaFactory.factories['binary'] = valueMetaFactory.factories[TYPE_BINARY]
valueMetaFactory.factories['bin'] = valueMetaFactory.factories[TYPE_BINARY]
