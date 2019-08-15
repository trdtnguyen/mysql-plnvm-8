#!/bin/bash
#This script build cscope files and build ctags Library
MYSQL_HOME=/home/vldb/mysql-plnvm-8

cd $MYSQL_HOME
find -name "*.h" -o -name "*.hpp" -o -name "*.i" -o -name "*.ic" -o -name "*.in" -o -name "*.c" -o -name "*.cc" -o -name "*.cpp" > cscope.files
#find -name "*.h" -o -name "*.hpp" -o -name "*.i" -o -name "*.ic" -o -name "*.in" -o -name "*.c" -o -name "*.cc" -o -name "*.cpp" -o -name "*.cmake" -o -name "*.txt" > cscope.files
cscope -b -q -k
ctags -L cscope.files
