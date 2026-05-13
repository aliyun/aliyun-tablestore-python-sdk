#!/bin/bash
set -e

# Print color
YELLOW='\033[33m'
GREEN='\033[32m'
RED='\033[31m'
RESET='\033[0m'

binary_linux="bin/protoc-25.0-linux-x86_64/protoc"
binary_osx="bin/protoc-25.0-osx-universal_binary/protoc"

# Check the current system
system=$(uname -s | tr '[:upper:]' '[:lower:]')
arch=$(uname -m | tr '[:upper:]' '[:lower:]')

# Determine the appropriate binary file
if [ "$system" == "linux" ] && [ "$arch" == "x86_64" ]; then
    binary=$binary_linux
    echo "use linux "
elif [ "$system" == "darwin" ]; then
    binary=$binary_osx
    echo "use osx"
else
    echo -e "${RED}当前系统既不是linux-x86_64，也不是osx。请到https://github.com/protocolbuffers/protobuf/releases/tag/ 下载可执行文件${RESET}"
    exit 1
fi

chmod +x "$binary"

echo "protoc 版本是: $(./$binary --version)"


check_and_replace_import_path_in_pb_py() {
    proto_file=$1
    py_file=$2
    # Check if the py file exists
    if [ -e "$py_file" ]; then
        # If the proto contains import, modify the import path in the generated py file
        if grep -q '^import ' "$proto_file"; then
            cur_sec=$(date '+%s')
            tmp_file=/tmp/temp${cur_sec}.py

            # Modify "import xxx_pb2" to "import tablestore.protobuf.xxx_pb2"
            sed 's/^import \([a-zA-Z0-9_]*\)_pb2/import tablestore.protobuf.\1_pb2/g' "$py_file" > "$tmp_file"
            # Modify "from xxx_pb2 import" to "from tablestore.protobuf.xxx_pb2 import"
            sed 's/^from \([a-zA-Z0-9_]*\)_pb2 import/from tablestore.protobuf.\1_pb2 import/g' "$tmp_file" > "$py_file"
            
            rm -f $tmp_file
            echo -e "${YELLOW}检测到${proto_file}中有import，已修改对应的py文件${py_file}中的import路径${RESET}"
        fi
    else
        echo -e "${RED}错误：${py_file}文件不存在${RESET}"
    fi
}

./$binary --proto_path=tablestore/protobuf/ --python_out=tablestore/protobuf/ tablestore/protobuf/table_store.proto 
./$binary --proto_path=tablestore/protobuf/ --python_out=tablestore/protobuf/ tablestore/protobuf/table_store_filter.proto 
./$binary --proto_path=tablestore/protobuf/ --python_out=tablestore/protobuf/ tablestore/protobuf/search.proto 
./$binary --proto_path=tablestore/protobuf/ --python_out=tablestore/protobuf/ tablestore/protobuf/timeseries.proto
./$binary --proto_path=tablestore/protobuf/ --python_out=tablestore/protobuf/ tablestore/protobuf/global_table.proto

check_and_replace_import_path_in_pb_py tablestore/protobuf/table_store.proto tablestore/protobuf/table_store_pb2.py
check_and_replace_import_path_in_pb_py tablestore/protobuf/table_store_filter.proto tablestore/protobuf/table_store_filter_pb2.py
check_and_replace_import_path_in_pb_py tablestore/protobuf/search.proto tablestore/protobuf/search_pb2.py
check_and_replace_import_path_in_pb_py tablestore/protobuf/timeseries.proto tablestore/protobuf/timeseries_pb2.py
check_and_replace_import_path_in_pb_py tablestore/protobuf/global_table.proto tablestore/protobuf/global_table_pb2.py

echo -e "${GREEN}所有命令执行完毕。${RESET}"