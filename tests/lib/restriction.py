# -*- coding: utf8 -*-

MaxInstanceNameLength = 16           # Instance名字长度上限 
MaxTableNameLength = 255             # Table名字长度上限
MaxColumnNameLength = 255            # Column名字长度上限
MaxInstanceCountForUser = 5          # 一个帐号包含的Instance数上限
MaxTableCountForInstance = 10        # 一个Instance包含的表个数上限
MaxPKColumnNum = 4                   # 主键包含的列数上限
MaxPKStringValueLength = 1024        # String类型列大小上限（主键列）
MaxNonPKStringValueLength = 64 * 1024# String类型列大小上限（非主键列）
MaxBinaryValueLength = 64 * 1024     # Binary类型列值大小上限
MaxColumnCountForRow = 100           # 一行中列的个数上限
MaxColumnDataSizeForRow = 1024 * 1024 # 一行中列的总大小上限
MaxReadWriteCapacityUnit = 5000      # 表上的Capacity Unit上限
MinReadWriteCapacityUnit = 0         # 表上的Capacity Unit下限
MaxRowCountForMultiGetRow = 100      # MultiGetRow一次操作的行数上限
MaxRowCountForMultiWriteRow = 200    # MultiWriteRow一次操作的行数上限
MaxRowCountForGetRange = 5000        # Query一次返回的行数上限
MaxDataSizeForGetRange = 1024 * 1024 # Query一次返回的数据大小上限
MaxCUReduceTimeLimit = 4             # CU下调次数上限

CUUpdateTimeLongest = 60             #UpdateTableCU最长响应时间，单位s

CURestoreTimeInSec = 1               # 这个不是限制项，内置变量，CU的回血时间
