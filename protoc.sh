#!/bin/bash

DIR=$(cd $(dirname $0); pwd)
cd ${DIR}

# 定义Protobuf的版本号
PROTOC_VERSION="25.0"

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
    echo "当前系统既不是linux-x86_64，也不是osx。请到https://github.com/protocolbuffers/protobuf/releases/tag/v${PROTOC_VERSION}下载可执行文件。"
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
    proto_base=$(basename "$proto_file")

    command="./$binary $proto_base --python_out=../tablestore/protobuf/py3"
    echo "执行命令: $command"
    $command
    if [ $? -ne 0 ]; then
        echo "执行命令失败: $command"
        exit 1
    fi
done

cd ..
rm -r protoc-${PROTOC_VERSION}

echo "所有命令执行完毕。"
