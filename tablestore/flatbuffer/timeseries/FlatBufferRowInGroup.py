# automatically generated by the FlatBuffers compiler, do not modify

# namespace: timeseries

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class FlatBufferRowInGroup(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = FlatBufferRowInGroup()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsFlatBufferRowInGroup(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # FlatBufferRowInGroup
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # FlatBufferRowInGroup
    def DataSource(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # FlatBufferRowInGroup
    def Tags(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # FlatBufferRowInGroup
    def Time(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int64Flags, o + self._tab.Pos)
        return 0

    # FlatBufferRowInGroup
    def FieldValues(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            x = self._tab.Indirect(o + self._tab.Pos)
            from timeseries.FieldValues import FieldValues
            obj = FieldValues()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # FlatBufferRowInGroup
    def MetaCacheUpdateTime(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(12))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Uint32Flags, o + self._tab.Pos)
        return 0

    # FlatBufferRowInGroup
    def TagList(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(14))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from timeseries.Tag import Tag
            obj = Tag()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # FlatBufferRowInGroup
    def TagListLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(14))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # FlatBufferRowInGroup
    def TagListIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(14))
        return o == 0

def FlatBufferRowInGroupStart(builder):
    builder.StartObject(6)

def Start(builder):
    FlatBufferRowInGroupStart(builder)

def FlatBufferRowInGroupAddDataSource(builder, dataSource):
    builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(dataSource), 0)

def AddDataSource(builder, dataSource):
    FlatBufferRowInGroupAddDataSource(builder, dataSource)

def FlatBufferRowInGroupAddTags(builder, tags):
    builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(tags), 0)

def AddTags(builder, tags):
    FlatBufferRowInGroupAddTags(builder, tags)

def FlatBufferRowInGroupAddTime(builder, time):
    builder.PrependInt64Slot(2, time, 0)

def AddTime(builder, time):
    FlatBufferRowInGroupAddTime(builder, time)

def FlatBufferRowInGroupAddFieldValues(builder, fieldValues):
    builder.PrependUOffsetTRelativeSlot(3, flatbuffers.number_types.UOffsetTFlags.py_type(fieldValues), 0)

def AddFieldValues(builder, fieldValues):
    FlatBufferRowInGroupAddFieldValues(builder, fieldValues)

def FlatBufferRowInGroupAddMetaCacheUpdateTime(builder, metaCacheUpdateTime):
    builder.PrependUint32Slot(4, metaCacheUpdateTime, 0)

def AddMetaCacheUpdateTime(builder, metaCacheUpdateTime):
    FlatBufferRowInGroupAddMetaCacheUpdateTime(builder, metaCacheUpdateTime)

def FlatBufferRowInGroupAddTagList(builder, tagList):
    builder.PrependUOffsetTRelativeSlot(5, flatbuffers.number_types.UOffsetTFlags.py_type(tagList), 0)

def AddTagList(builder, tagList):
    FlatBufferRowInGroupAddTagList(builder, tagList)

def FlatBufferRowInGroupStartTagListVector(builder, numElems):
    return builder.StartVector(4, numElems, 4)

def StartTagListVector(builder, numElems):
    return FlatBufferRowInGroupStartTagListVector(builder, numElems)

def FlatBufferRowInGroupEnd(builder):
    return builder.EndObject()

def End(builder):
    return FlatBufferRowInGroupEnd(builder)
