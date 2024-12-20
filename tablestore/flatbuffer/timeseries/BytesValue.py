# automatically generated by the FlatBuffers compiler, do not modify

# namespace: timeseries

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class BytesValue(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = BytesValue()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsBytesValue(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # BytesValue
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # BytesValue
    def Value(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(flatbuffers.number_types.Int8Flags, a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 1))
        return 0

    # BytesValue
    def ValueAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.Int8Flags, o)
        return 0

    # BytesValue
    def ValueLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # BytesValue
    def ValueIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        return o == 0

def BytesValueStart(builder):
    builder.StartObject(1)

def Start(builder):
    BytesValueStart(builder)

def BytesValueAddValue(builder, value):
    builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(value), 0)

def AddValue(builder, value):
    BytesValueAddValue(builder, value)

def BytesValueStartValueVector(builder, numElems):
    return builder.StartVector(1, numElems, 1)

def StartValueVector(builder, numElems):
    return BytesValueStartValueVector(builder, numElems)

def BytesValueEnd(builder):
    return builder.EndObject()

def End(builder):
    return BytesValueEnd(builder)
