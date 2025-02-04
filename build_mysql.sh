#!/bin/bash 
MYSQL_HOME=/home/vldb/mysql-plnvm-8

#debug mode
IS_DEBUG=0
#IS_DEBUG=1

#Next paper configs PL-NVM ##############################
#BUILD_NAME=""
#BUILD_NAME="-DUNIV_TRACE_RECOVERY_TIME" #Original with tracing recovery time
#BUILD_NAME="-DUNIV_TRACE_FLUSH_TIME -DUNIV_SKIPLOG"
#BUILD_NAME="-DUNIV_PMEMOBJ_WAL"
#BUILD_NAME="-DUNIV_PMEMOBJ_WAL -DUNIV_PMEMOBJ_WAL_ELR -DUNIV_TRACE_FLUSH_TIME"
#BUILD_NAME="-DUNIV_NVM_LOG -DUNIV_PMEMOBJ_WAL -DUNIV_TRACE_FLUSH_TIME"

#Benchmark########
#BUILD_NAME="-DUNIV_SKIPLOG"
BUILD_NAME="-DUNIV_PMEMOBJ_PERSIST -DUNIV_PMEMOBJ_PPL_STAT -DUNIV_PMEMOBJ_PAGE_LOG -DUNIV_PMEMOBJ_PART_PL -DUNIV_PMEMOBJ_PL -DUNIV_TRACE_RECOVERY_TIME" #Stable
###################

#### Debug
#BUILD_NAME="-DUNIV_UNDO_DEBUG -DUNIV_PMEMOBJ_PERSIST -DUNIV_PMEMOBJ_PPL_STAT -DUNIV_PMEMOBJ_PAGE_LOG -DUNIV_PMEMOBJ_PART_PL -DUNIV_PMEMOBJ_PL -DUNIV_TRACE_RECOVERY_TIME"
####



echo "BUILD MODE: $BUILD_NAME with debug=$IS_DEBUG"
cd $MYSQL_HOME/bld
cmake .. -DWITH_DEBUG=$IS_DEBUG  -DCMAKE_C_FLAGS="$BUILD_NAME -DUNIV_AIO_IMPROVE" -DCMAKE_CXX_FLAGS="$BUILD_NAME -DUNIV_AIO_IMPROVE" -DDOWNLOAD_BOOST=1 -DWITH_BOOST=/root/boost
#cmake .. -DWITH_DEBUG=$IS_DEBUG  -DCMAKE_C_FLAGS="$BUILD_NAME" -DCMAKE_CXX_FLAGS="$BUILD_NAME" -DDOWNLOAD_BOOST=1 -DWITH_BOOST=/root/boost
make install -j48
