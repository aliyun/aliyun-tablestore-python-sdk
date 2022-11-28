# automatically generated by the FlatBuffers compiler, do not modify

# namespace: dataprotocol

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class RLEStringValues(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = RLEStringValues()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsRLEStringValues(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # RLEStringValues
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # RLEStringValues
    def Array(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.String(a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 4))
        return ""

    # RLEStringValues
    def ArrayLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # RLEStringValues
    def ArrayIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        return o == 0

    # RLEStringValues
    def IndexMapping(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(flatbuffers.number_types.Int32Flags, a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 4))
        return 0

    # RLEStringValues
    def IndexMappingAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.Int32Flags, o)
        return 0

    # RLEStringValues
    def IndexMappingLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # RLEStringValues
    def IndexMappingIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        return o == 0

def RLEStringValuesStart(builder): builder.StartObject(2)
def Start(builder):
    return RLEStringValuesStart(builder)
def RLEStringValuesAddArray(builder, array): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(array), 0)
def AddArray(builder, array):
    return RLEStringValuesAddArray(builder, array)
def RLEStringValuesStartArrayVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def StartArrayVector(builder, numElems):
    return RLEStringValuesStartArrayVector(builder, numElems)
def RLEStringValuesAddIndexMapping(builder, indexMapping): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(indexMapping), 0)
def AddIndexMapping(builder, indexMapping):
    return RLEStringValuesAddIndexMapping(builder, indexMapping)
def RLEStringValuesStartIndexMappingVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def StartIndexMappingVector(builder, numElems):
    return RLEStringValuesStartIndexMappingVector(builder, numElems)
def RLEStringValuesEnd(builder): return builder.EndObject()
def End(builder):
    return RLEStringValuesEnd(builder)
try:
    from typing import List
except:
    pass

class RLEStringValuesT(object):

    # RLEStringValuesT
    def __init__(self):
        self.array = None  # type: List[str]
        self.indexMapping = None  # type: List[int]

    @classmethod
    def InitFromBuf(cls, buf, pos):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, 0)
        rlestringValues = RLEStringValues()
        rlestringValues.Init(buf, pos+n)
        return cls.InitFromObj(rlestringValues)

    @classmethod
    def InitFromObj(cls, rlestringValues):
        x = RLEStringValuesT()
        x._UnPack(rlestringValues)
        return x

    # RLEStringValuesT
    def _UnPack(self, rlestringValues):
        if rlestringValues is None:
            return
        if not rlestringValues.ArrayIsNone():
            self.array = []
            for i in range(rlestringValues.ArrayLength()):
                self.array.append(rlestringValues.Array(i))
        if not rlestringValues.IndexMappingIsNone():
            if np is None:
                self.indexMapping = []
                for i in range(rlestringValues.IndexMappingLength()):
                    self.indexMapping.append(rlestringValues.IndexMapping(i))
            else:
                self.indexMapping = rlestringValues.IndexMappingAsNumpy()

    # RLEStringValuesT
    def Pack(self, builder):
        if self.array is not None:
            arraylist = []
            for i in range(len(self.array)):
                arraylist.append(builder.CreateString(self.array[i]))
            RLEStringValuesStartArrayVector(builder, len(self.array))
            for i in reversed(range(len(self.array))):
                builder.PrependUOffsetTRelative(arraylist[i])
            array = builder.EndVector()
        if self.indexMapping is not None:
            if np is not None and type(self.indexMapping) is np.ndarray:
                indexMapping = builder.CreateNumpyVector(self.indexMapping)
            else:
                RLEStringValuesStartIndexMappingVector(builder, len(self.indexMapping))
                for i in reversed(range(len(self.indexMapping))):
                    builder.PrependInt32(self.indexMapping[i])
                indexMapping = builder.EndVector()
        RLEStringValuesStart(builder)
        if self.array is not None:
            RLEStringValuesAddArray(builder, array)
        if self.indexMapping is not None:
            RLEStringValuesAddIndexMapping(builder, indexMapping)
        rlestringValues = RLEStringValuesEnd(builder)
        return rlestringValues
