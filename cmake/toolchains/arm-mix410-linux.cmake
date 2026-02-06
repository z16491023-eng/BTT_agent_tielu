# 目标系统
set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR arm)

# --- 1) 指定交叉编译器（绝对路径，避免被本机 gcc/g++ 抢走） ---
# toolchains/arm-mix410-linux.cmake
set(CMAKE_C_COMPILER   /opt/linux/x86-arm/arm-mix410-linux/bin/arm-mix410-linux-gcc)
set(CMAKE_CXX_COMPILER /opt/linux/x86-arm/arm-mix410-linux/bin/arm-mix410-linux-g++)


# 交叉查找行为
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)