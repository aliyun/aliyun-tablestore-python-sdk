import six

from tablestore.error import *
from .timeseries.DataType import *
from .timeseries import FlatBufferRowGroup
from .timeseries import FlatBufferRowInGroup
from .timeseries import FlatBufferRows
from .timeseries import FieldValues
from .timeseries import Tag
from .timeseries import BytesValue

import flatbuffers


def get_column_val_by_tp(timeseries_table_name, timeseries_rows):
    builder = flatbuffers.Builder()

    row_group_offs = []
    for index in range(len(timeseries_rows)):
        row = timeseries_rows[index]
        off = build_row_to_row_group_offset(row, builder, timeseries_table_name)
        row_group_offs.append(off)

    row_group_vector_offs = build_row_group_vectors(row_group_offs, builder)
    FlatBufferRows.FlatBufferRowsStart(builder)
    FlatBufferRows.FlatBufferRowsAddRowGroups(builder, row_group_vector_offs)
    rows_offset = FlatBufferRows.FlatBufferRowsEnd(builder)
    builder.Finish(rows_offset)

    return builder.Output()


def build_row_to_row_group_offset(timeseries_row, builder, timeseries_table_name):
    field_value_types = []
    field_name_offs = []
    long_values = []
    bool_values = []
    double_values = []
    str_value_offs = []
    binary_value_offs = []

    sorted_keys = sorted(timeseries_row.fields.keys())
    for key in sorted_keys:
        value = timeseries_row.fields[key]
        field_name_offs.append(builder.CreateString(key))
        if isinstance(value, bool):
            field_value_types.append(DataType.BOOLEAN)
            bool_values.append(value)
        elif isinstance(value, int):
            field_value_types.append(DataType.LONG)
            long_values.append(value)
        elif isinstance(value, six.text_type) or isinstance(value, six.binary_type):
            field_value_types.append(DataType.STRING)
            if isinstance(value, six.text_type):
                value = value.encode('utf-8')
            str_value_offs.append(builder.CreateString(value, 'utf-8'))
        elif isinstance(value, bytearray):
            field_value_types.append(DataType.BINARY)
            i = build_byte_value_vectors(value, builder)
            builder.StartObject(1)
            BytesValue.BytesValueAddValue(builder, i)
            binary_value_offs.append(BytesValue.BytesValueEnd(builder))
        elif isinstance(value, float):
            field_value_types.append(DataType.DOUBLE)
            double_values.append(value)
        else:
            raise OTSClientError("Unsupported column type: " + str(type(value)))


    if len(long_values) != 0:
        long_vector_off = build_long_value_vectors(long_values, builder)
    if len(bool_values) != 0:
        bool_vector_off = build_bool_value_vectors(bool_values, builder)
    if len(double_values) != 0:
        double_vector_off = build_double_value_vectors(double_values, builder)
    if len(str_value_offs) != 0:
        string_vector_off = build_string_value_vectors(str_value_offs, builder)
    if len(binary_value_offs) != 0:
        binary_vector_off = build_field_value_byte_value_vectors(binary_value_offs, builder)

    FieldValues.FieldValuesStart(builder)
    if len(long_values) != 0:
        FieldValues.AddLongValues(builder, long_vector_off)
    if len(bool_values) != 0:
        FieldValues.AddBoolValues(builder, bool_vector_off)
    if len(double_values) != 0:
        FieldValues.AddDoubleValues(builder, double_vector_off)
    if len(str_value_offs) != 0:
        FieldValues.AddStringValues(builder, string_vector_off)
    if len(binary_value_offs) != 0:
        FieldValues.AddBinaryValues(builder, binary_vector_off)
    field_value_off = FieldValues.FieldValuesEnd(builder)

    datasource = timeseries_row.timeseries_key.data_source
    if datasource is None:
        datasource = ""
    tags = build_tags_vectors(timeseries_row.timeseries_key.tags, builder)
    datasourceoff = builder.CreateString(datasource)
    tagsoff = builder.CreateString("")
    FlatBufferRowInGroup.FlatBufferRowInGroupStart(builder)
    FlatBufferRowInGroup.FlatBufferRowInGroupAddDataSource(builder, datasourceoff)
    FlatBufferRowInGroup.FlatBufferRowInGroupAddTags(builder, tagsoff)
    FlatBufferRowInGroup.FlatBufferRowInGroupAddFieldValues(builder, field_value_off)
    FlatBufferRowInGroup.FlatBufferRowInGroupAddTime(builder, timeseries_row.time_in_us)

    FlatBufferRowInGroup.FlatBufferRowInGroupAddTagList(builder, tags)

    rowsoffvector = build_row_in_group_vectors([FlatBufferRowInGroup.End(builder)], builder)

    field_name_vectoroffs = build_field_name_vectors(field_name_offs, builder)
    field_type_vectoroffs = build_field_type_vectors(field_value_types, builder)

    if timeseries_row.timeseries_key.measurement_name is None:
        timeseries_row.timeseries_key.measurement_name = ""
    measureoffs = builder.CreateString(timeseries_row.timeseries_key.measurement_name)

    FlatBufferRowGroup.FlatBufferRowGroupStart(builder)
    FlatBufferRowGroup.FlatBufferRowGroupAddMeasurementName(builder, measureoffs)
    FlatBufferRowGroup.FlatBufferRowGroupAddFieldNames(builder, field_name_vectoroffs)
    FlatBufferRowGroup.FlatBufferRowGroupAddFieldTypes(builder, field_type_vectoroffs)
    FlatBufferRowGroup.FlatBufferRowGroupAddRows(builder, rowsoffvector)

    return FlatBufferRowGroup.End(builder)


def build_field_value_byte_value_vectors(binary_value_offs, builder):
    FieldValues.FieldValuesStartBinaryValuesVector(builder, len(binary_value_offs))
    for index in range(len(binary_value_offs) - 1, -1, -1):
        builder.PrependUOffsetTRelative(binary_value_offs[index])
    return builder.EndVector(len(binary_value_offs))


def build_string_value_vectors(str_value_offs, builder):
    FieldValues.FieldValuesStartStringValuesVector(builder, len(str_value_offs))
    for index in range(len(str_value_offs)-1, -1, -1):
        builder.PrependUOffsetTRelative(str_value_offs[index])
    return builder.EndVector(len(str_value_offs))


def build_double_value_vectors(double_values, builder):
    FieldValues.FieldValuesStartDoubleValuesVector(builder, len(double_values))
    for index in range(len(double_values)-1, -1, -1):
        builder.PrependFloat64(double_values[index])
    return builder.EndVector(len(double_values))


def build_bool_value_vectors(bool_values, builder):
    FieldValues.FieldValuesStartBoolValuesVector(builder, len(bool_values))
    for index in range(len(bool_values)-1, -1, -1):
        builder.PrependBool(bool_values[index])
    return builder.EndVector(len(bool_values))


def build_long_value_vectors(long_values, builder):
    FieldValues.FieldValuesStartLongValuesVector(builder, len(long_values))
    for index in range(len(long_values)-1, -1, -1):
        builder.PrependInt64(long_values[index])
    return builder.EndVector(len(long_values))


def build_byte_value_vectors(bytearrays, builder):
    BytesValue.BytesValueStartValueVector(builder, len(bytearrays))
    for index in range(len(bytearrays)-1, -1, -1):
        builder.PrependByte(bytearrays[index])
    return builder.EndVector(len(bytearrays))


def build_row_group_vectors(row_groups, builder):
    FlatBufferRows.FlatBufferRowsStartRowGroupsVector(builder, len(row_groups))
    for index in range(len(row_groups)-1, -1, -1):
        builder.PrependUOffsetTRelative(row_groups[index])
    return builder.EndVector(len(row_groups))


def build_field_type_vectors(field_value_types, builder):
    FlatBufferRowGroup.FlatBufferRowGroupStartFieldTypesVector(builder, len(field_value_types))
    for index in range(len(field_value_types)-1, -1, -1):
        builder.PrependByte(field_value_types[index])

    return builder.EndVector(len(field_value_types))


def build_field_name_vectors(fieldnames_offs, builder):
    FlatBufferRowGroup.FlatBufferRowGroupStartFieldNamesVector(builder, len(fieldnames_offs))
    for index in range(len(fieldnames_offs) - 1, -1, -1):
        builder.PrependUOffsetTRelative(fieldnames_offs[index])

    return builder.EndVector(len(fieldnames_offs))


def build_row_in_group_vectors(row_in_group_offs, builder):
    FlatBufferRowGroup.FlatBufferRowGroupStartRowsVector(builder, len(row_in_group_offs))
    for index in range(len(row_in_group_offs)-1, -1, -1):
        builder.PrependUOffsetTRelative(row_in_group_offs[index])

    return builder.EndVector(len(row_in_group_offs))


def build_tags_vectors(tags, builder):
    tag_offs = []
    sorted_keys = sorted(tags.keys())
    for key in sorted_keys:
        value = tags[key]
        key_off = builder.CreateString(key)
        value_off = builder.CreateString(value)
        Tag.TagStart(builder)
        Tag.TagAddName(builder, key_off)
        Tag.TagAddValue(builder, value_off)
        tag_offs.append(Tag.End(builder))

    FlatBufferRowInGroup.FlatBufferRowInGroupStartTagListVector(builder, len(tag_offs))
    for index in range(len(tag_offs)-1, -1, -1):
        builder.PrependUOffsetTRelative(tag_offs[index])

    return builder.EndVector(len(tag_offs))
