cmake_minimum_required(VERSION 3.20)
# ↑ 声明 CMake 最低版本。版本越高特性越全，行为越一致。

project(BttAgent LANGUAGES C CXX)
# ↑ 定义工程名，并启用 C/C++。你后面要用 C++ 写核心逻辑，这里必须有 CXX。

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# ↑ 统一编译标准，避免不同模块用不同标准导致 ABI/语法问题。

# 输出目录（可选）
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(EXT_INC_DIR ${CMAKE_SOURCE_DIR}/external/include)
set(EXT_LIB_DIR ${CMAKE_SOURCE_DIR}/external/lib)


add_subdirectory(apps)
# ↑ 统一第三方头文件与库路径（你 external 的真实结构就是 include + lib）。

# list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/modules)
# include(ThirdParty)
# ↑ 告诉 CMake 去 cmake/ 目录找自定义模块，然后 include ThirdParty.cmake
#    ThirdParty 的作用是：把 external/lib/*.a 变成“imported targets”。

# add_subdirectory(libs)

# ↑ 进入子目录执行各自的 CMakeLists.txt：
#    libs 生成静态库/目标；apps 生成可执行文件。

# → CMakeLists.txt 是主入口；.cmake 是可复用模块。
# → toolchain 文件是命令行指定，不是自动发现。-DCMAKE_TOOLCHAIN_FILE=cmake/toolchains/arm-mix410-linux.cmake
# → include(XXX) 通过 CMAKE_MODULE_PATH 找 XXX.cmake。
