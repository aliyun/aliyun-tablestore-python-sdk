#!/bin/bash

# The shell script works as expected when run on MacOS.

# 第一部分: 设置目录
DIR=$(cd $(dirname \$0); pwd)
cd ${DIR}

# 第二部分: 移动 tablestore/flatbuffer/dataprotocol 目录
if [ -d "tablestore/flatbuffer/dataprotocol" ]; then
    mv tablestore/flatbuffer/dataprotocol ./
else
    echo "Directory tablestore/flatbuffer/dataprotocol does not exist."
fi

# 第三部分: 移动 protobuf/py3 目录下的文件
if [ -d "tablestore/protobuf/py3" ]; then
    shopt -s nullglob
    for file in tablestore/protobuf/py3/*.py; do
        if [[ "$file" != *"__init__.py" ]]; then
            mv "$file" tablestore/protobuf/
        fi
    done
else
    echo "Directory tablestore/protobuf/py3 does not exist."
fi

# 第四部分: 替换 decoder.py 文件内的内容
if [ -f "tablestore/decoder.py" ]; then
    sed -i '' 's/tablestore\.flatbuffer\.dataprotocol/dataprotocol/g' tablestore/decoder.py
else
    echo "File tablestore/decoder.py does not exist."
fi

# 第五部分: 替换 flat_buffer_decoder.py 文件内的内容
if [ -f "tablestore/flatbuffer/flat_buffer_decoder.py" ]; then
    sed -i '' 's/tablestore\.flatbuffer\.dataprotocol/dataprotocol/g' tablestore/flatbuffer/flat_buffer_decoder.py
    sed -i '' 's/\.dataprotocol/dataprotocol/g' tablestore/flatbuffer/flat_buffer_decoder.py
else
    echo "File tablestore/flatbuffer/flat_buffer_decoder.py does not exist."
fi

echo "Script execution completed."
