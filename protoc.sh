#!/bin/bash

set -e

# 打印颜色
YELLOW='\033[33m'
GREEN='\033[32m'
RED='\033[31m'
RESET='\033[0m'

CheckAndReplaceImportPathInPbPy() {
    proto_file=$1
    py_file=$2
    # 检查是否存在py文件
    if [ -e "$py_file" ]; then
        # 如果proto中包含import，修改生成py文件中的import路径
        if grep -q '^import ' "$proto_file"; then
            cur_sec=$(date '+%s')
            tmp_file=/tmp/temp${cur_sec}.py

            # 修改"import xxx_pb2"为"import tablestore.protobuf.xxx_pb2"
            sed 's/^import \([a-zA-Z0-9_]*\)_pb2/import tablestore.protobuf.\1_pb2/g' "$py_file" > "$tmp_file"
            # 修改"from xxx_pb2 import"为"from tablestore.protobuf.xxx_pb2 import"
            sed 's/^from \([a-zA-Z0-9_]*\)_pb2 import/from tablestore.protobuf.\1_pb2 import/g' "$tmp_file" > "$py_file"
            
            rm -f $tmp_file
            echo -e "${YELLOW}检测到${proto_file}中有import，已修改对应的py文件${py_file}中的import路径${RESET}"
        fi
    else
        echo -e "${RED}错误：${py_file}文件不存在${RESET}"
    fi
}

DIR=$(cd $(dirname $0); pwd)
cd ${DIR}

# 定义Protobuf的版本号
PROTOC_VERSION="25.0"

rm -rf protoc-${PROTOC_VERSION}
mkdir -p protoc-${PROTOC_VERSION}

wget -P protoc-${PROTOC_VERSION} https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip
wget -P protoc-${PROTOC_VERSION} https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-osx-universal_binary.zip

unzip protoc-${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip -d protoc-${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64
unzip protoc-${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-osx-universal_binary.zip -d protoc-${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-osx-universal_binary

# 定义目录和文件列表
folder="protoc-${PROTOC_VERSION}"

# 二进制文件
binary_linux="protoc-${PROTOC_VERSION}-linux-x86_64/bin/protoc"
binary_osx="protoc-${PROTOC_VERSION}-osx-universal_binary/bin/protoc"

# proto文件列表
proto_files=($(find ./tablestore -type f -name "*.proto"))

# 检查当前系统
system=$(uname -s | tr '[:upper:]' '[:lower:]')
arch=$(uname -m | tr '[:upper:]' '[:lower:]')

# 判断合适的二进制文件
if [ "$system" == "linux" ] && [ "$arch" == "x86_64" ]; then
    binary=$binary_linux
elif [ "$system" == "darwin" ]; then
    binary=$binary_osx
else
    echo -e "${RED}当前系统既不是linux-x86_64，也不是osx。请到https://github.com/protocolbuffers/protobuf/releases/tag/v${PROTOC_VERSION}下载可执行文件。${RESET}"
    exit 1
fi

# 1. 复制proto文件到protoc目录
for proto_file in "${proto_files[@]}"; do
    cp "$proto_file" "$folder"
done

cd protoc-${PROTOC_VERSION}

# 设置文件的可执行权限
chmod +x "$binary"

# 2. 编译proto文件并将生成的文件放在protoc目录
for proto_file in "${proto_files[@]}"; do
    proto_file=$(basename "$proto_file")
    proto_base=$(basename "$proto_file" .proto)

    command="./$binary $proto_file --python_out=../tablestore/protobuf/py3"
    echo "执行命令: $command"
    $command
    py_file=../tablestore/protobuf/py3/${proto_base}_pb2.py
    CheckAndReplaceImportPathInPbPy $proto_file $py_file
done

cd ..
rm -rf protoc-${PROTOC_VERSION}

echo -e "${GREEN}所有命令执行完毕。${RESET}"
