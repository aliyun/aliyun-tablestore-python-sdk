# -*- coding: utf8 -*-

class Const(object):
    class ConstError(TypeError): 
        pass
    class ConstCaseError(ConstError):
        pass

    def __setattr__(self, name, value):
        if name in self.__dict__.keys():
            raise self.ConstError("can't change const.%s" % name)

        self.__dict__[name] = value

import sys
sys.modules[__name__] = Const()
