# -*- coding: utf8 -*-

class PrimaryKey(object):
    def __init__(self):
        self.pks = []

    def add_primary_key(self, pk):
        self.pks.append(pk)

    def get_primary_keys(self):
        return self.pks

    def get_primary_key_size(self):
        return len(self.pks)

    def get_primary_key(self, index):
        return self.pks[index]

class PrimaryKeyColumn(object):
    def __init(self, name, value):
        self.name = name
        self.value = value

    def get_name(self, name):
        self.name = name

    def get_value(self, value):
        self.value = value

class PrimaryKeyValue(object):
    def __init__(self, pk_type, value):
        self.type = pk_type
        self.value = value

    def get_type(self):
        return self.type

    def get_value(self):
        return self.value
        
