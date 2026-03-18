
add_executable(btt_core
  btt-core/main.cpp
  ${CMAKE_SOURCE_DIR}/libs/core/core_app.cpp
  ${CMAKE_SOURCE_DIR}/libs/net/tcp_client.cpp
  ${CMAKE_SOURCE_DIR}/libs/proto/btt_proto.cpp
  ${CMAKE_SOURCE_DIR}/libs/utils/log.cpp
)

add_executable(upgraded
  upgraded/main.cpp
  upgraded/upgraded.cpp
  ${CMAKE_SOURCE_DIR}/libs/utils/log.cpp
)

# 关键：让 #include "libs/xxx/yyy.hpp" 能找到
target_include_directories(btt_core PRIVATE
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/external/include
  ${CMAKE_SOURCE_DIR}/include
)

target_include_directories(upgraded PRIVATE
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/include
)

find_package(Threads REQUIRED)

target_link_libraries(upgraded PRIVATE Threads::Threads)

# 1) 外部静态库列表
set(EXT_MEDIA_LIBS
  ${EXT_LIB_DIR}/libhsdk_media_3520.a
  ${EXT_LIB_DIR}/libss_mpi.a
  ${EXT_LIB_DIR}/libss_voice_engine.a
  ${EXT_LIB_DIR}/libsecurec.a
  ${EXT_LIB_DIR}/libss_upvqe.a
  ${EXT_LIB_DIR}/libss_dnvqe.a
  ${EXT_LIB_DIR}/libss_nnie.a
  ${EXT_LIB_DIR}/libss_ive.a
  ${EXT_LIB_DIR}/libss_hdmi.a
  ${EXT_LIB_DIR}/libhgs_misc.a
)
target_link_libraries(btt_core PRIVATE Threads::Threads atomic
  ${EXT_MEDIA_LIBS}
   dl 
)
