#!/bin/bash

DIR=$(cd $(dirname \$0); pwd)
cd ${DIR}

# 遍历所有.py文件并运行unittest
for file in tests/*_test.py; do
    if [ -f "$file" ]; then
        echo "Running tests in $file..."
        python -m unittest "$file"
    else
        echo "No .py files found in tests directory."
    fi
done
