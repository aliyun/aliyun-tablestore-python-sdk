# automatically generated by the FlatBuffers compiler, do not modify

# namespace: timeseries

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class FlatBufferRowGroup(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = FlatBufferRowGroup()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsFlatBufferRowGroup(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # FlatBufferRowGroup
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # FlatBufferRowGroup
    def MeasurementName(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # FlatBufferRowGroup
    def FieldNames(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.String(a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 4))
        return ""

    # FlatBufferRowGroup
    def FieldNamesLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # FlatBufferRowGroup
    def FieldNamesIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        return o == 0

    # FlatBufferRowGroup
    def FieldTypes(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(flatbuffers.number_types.Int8Flags, a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 1))
        return 0

    # FlatBufferRowGroup
    def FieldTypesAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.Int8Flags, o)
        return 0

    # FlatBufferRowGroup
    def FieldTypesLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # FlatBufferRowGroup
    def FieldTypesIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        return o == 0

    # FlatBufferRowGroup
    def Rows(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from timeseries.FlatBufferRowInGroup import FlatBufferRowInGroup
            obj = FlatBufferRowInGroup()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # FlatBufferRowGroup
    def RowsLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # FlatBufferRowGroup
    def RowsIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        return o == 0

def FlatBufferRowGroupStart(builder):
    builder.StartObject(4)

def Start(builder):
    FlatBufferRowGroupStart(builder)

def FlatBufferRowGroupAddMeasurementName(builder, measurementName):
    builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(measurementName), 0)

def AddMeasurementName(builder, measurementName):
    FlatBufferRowGroupAddMeasurementName(builder, measurementName)

def FlatBufferRowGroupAddFieldNames(builder, fieldNames):
    builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(fieldNames), 0)

def AddFieldNames(builder, fieldNames):
    FlatBufferRowGroupAddFieldNames(builder, fieldNames)

def FlatBufferRowGroupStartFieldNamesVector(builder, numElems):
    return builder.StartVector(4, numElems, 4)

def StartFieldNamesVector(builder, numElems):
    return FlatBufferRowGroupStartFieldNamesVector(builder, numElems)

def FlatBufferRowGroupAddFieldTypes(builder, fieldTypes):
    builder.PrependUOffsetTRelativeSlot(2, flatbuffers.number_types.UOffsetTFlags.py_type(fieldTypes), 0)

def AddFieldTypes(builder, fieldTypes):
    FlatBufferRowGroupAddFieldTypes(builder, fieldTypes)

def FlatBufferRowGroupStartFieldTypesVector(builder, numElems):
    return builder.StartVector(1, numElems, 1)

def StartFieldTypesVector(builder, numElems):
    return FlatBufferRowGroupStartFieldTypesVector(builder, numElems)

def FlatBufferRowGroupAddRows(builder, rows):
    builder.PrependUOffsetTRelativeSlot(3, flatbuffers.number_types.UOffsetTFlags.py_type(rows), 0)

def AddRows(builder, rows):
    FlatBufferRowGroupAddRows(builder, rows)

def FlatBufferRowGroupStartRowsVector(builder, numElems):
    return builder.StartVector(4, numElems, 4)

def StartRowsVector(builder, numElems):
    return FlatBufferRowGroupStartRowsVector(builder, numElems)

def FlatBufferRowGroupEnd(builder):
    return builder.EndObject()

def End(builder):
    return FlatBufferRowGroupEnd(builder)
