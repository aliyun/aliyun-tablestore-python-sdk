# -*- coding: utf8 -*-

import six
from enum import IntEnum
from tablestore.protobuf import timeseries_pb2


class MetaQueryCompositeOperator(IntEnum):
    OP_AND = 1
    OP_OR = 2
    OP_NOT = 3

    def to_pb(self):
        if self == MetaQueryCompositeOperator.OP_AND:
            return timeseries_pb2.MetaQueryCompositeOperator.OP_AND
        elif self == MetaQueryCompositeOperator.OP_OR:
            return timeseries_pb2.MetaQueryCompositeOperator.OP_OR
        elif self == MetaQueryCompositeOperator.OP_NOT:
            return timeseries_pb2.MetaQueryCompositeOperator.OP_NOT


class MetaQuerySingleOperator(IntEnum):
    OP_EQUAL = 1
    OP_NOT_EQUAL = 2
    OP_GREATER_THAN = 3
    OP_GREATER_EQUAL = 4
    OP_LESS_THAN = 5
    OP_LESS_EQUAL = 6
    OP_PREFIX = 7

    def to_pb(self):
        if self == MetaQuerySingleOperator.OP_EQUAL:
            return timeseries_pb2.MetaQuerySingleOperator.OP_EQUAL
        elif self == MetaQuerySingleOperator.OP_GREATER_THAN:
            return timeseries_pb2.MetaQuerySingleOperator.OP_GREATER_THAN
        elif self == MetaQuerySingleOperator.OP_GREATER_EQUAL:
            return timeseries_pb2.MetaQuerySingleOperator.OP_GREATER_EQUAL
        elif self == MetaQuerySingleOperator.OP_LESS_THAN:
            return timeseries_pb2.MetaQuerySingleOperator.OP_LESS_THAN
        elif self == MetaQuerySingleOperator.OP_LESS_EQUAL:
            return timeseries_pb2.MetaQuerySingleOperator.OP_LESS_EQUAL
        elif self == MetaQuerySingleOperator.OP_PREFIX:
            return timeseries_pb2.MetaQuerySingleOperator.OP_PREFIX


class MeasurementMetaQueryCondition(object):
    def __init__(self, operator: MetaQuerySingleOperator, value: str):
        self.operator = operator
        self.value = value

    def get_type(self):
        return timeseries_pb2.MetaQueryConditionType.MEASUREMENT_CONDITION


class DataSourceMetaQueryCondition(object):
    def __init__(self, operator: MetaQuerySingleOperator, value: str):
        self.operator = operator
        self.value = value

    def get_type(self):
        return timeseries_pb2.MetaQueryConditionType.SOURCE_CONDITION


class TagMetaQueryCondition(object):
    def __init__(self, operator: MetaQuerySingleOperator, tag_name: str, value: str):
        self.operator = operator
        self.tag_name = tag_name
        self.value = value

    def get_type(self):
        return timeseries_pb2.MetaQueryConditionType.TAG_CONDITION


class UpdateTimeMetaQueryCondition(object):
    def __init__(self, operator: MetaQuerySingleOperator, time_in_us: int):
        self.operator = operator
        self.time_in_us = time_in_us

    def get_type(self):
        return timeseries_pb2.MetaQueryConditionType.UPDATE_TIME_CONDITION


class AttributeMetaQueryCondition(object):
    def __init__(self, operator: MetaQuerySingleOperator, attribute_name: str, value: str):
        self.operator = operator
        self.attribute_name = attribute_name
        self.value = value

    def get_type(self):
        return timeseries_pb2.MetaQueryConditionType.ATTRIBUTE_CONDITION


class CompositeMetaQueryCondition(object):
    def __init__(self, operator: MetaQueryCompositeOperator, sub_conditions: list):
        self.operator = operator
        self.subConditions = sub_conditions

    def get_type(self):
        return timeseries_pb2.MetaQueryConditionType.COMPOSITE_CONDITION



