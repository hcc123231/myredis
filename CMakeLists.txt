cmake_minimum_required(VERSION 3.17)#指定cmake的最低版本
project(myredis LANGUAGES C CXX VERSION 1.0.0)#定项目名称以及使用的编程语言以及项目的版本
set(CMAKE_CXX_STANDARD 17)#指定C++标准的版本,如果不完全支持也是不会报错的,或者不支持指定版本也只会尝试更低版本而不报错
set(CMAKE_CXX_STANDARD_REQUIRED on)#强制要求编译器支持c++的17版本,不然会报错
set(CMAKE_CXX_EXTENSIONS off)#关闭编译器提供的c++扩展,这些扩展都是非标准,对于跨平台的代码来说可能会出问题,set也是一个创建变量的命令,如果变量已存在(内置变量也是已存在的状态)那么只会更新变量的值,如果变量不存在那么会创建变量并且赋值
option(MYREDIS_ENABLE_WARNINGS "enable compiler warnings" ON)#option这个命令就是在创建一个变量,并且这个变量是一个布尔类型的选项,第二个参数是这个变量的提示内容,第三个变量是变量的值
find_package(Threads REQUIRED)#用于查找并加载系统中已安装的软件包,第一个参数是包名,REQUIRED是可选参数,表示如果找不到指定包就报错
set(CMAKE_BUILD_TYPE Debug)
if(MYREDIS_ENABLE_WARNINGS)
    #根据编译器的不同，添加对应编译器的警告选项
    if(CMAKE_CXX_COMPILE_ID MATCHES "Clang" OR CMAKE_CXX_COMPILE_ID STREQUAL "GNU")#这里的matches是正则表达式,只要CMAKE_CXX_COMPILE_ID中有"Clang"条件就会成立,而STREQUAL是字符串完全相等的比较
        add_compile_options(-Wall -Wextra -Wpedantic -Wconversion)
    elseif(CMAKE_CXX_COMPILE_ID STREQUAL "MSVC")
        add_compile_options(/W4)
    endif()
endif()

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_options(-O3 -DNDEBUG)
endif()
set(SOURCES src/aof.cpp src/config_loader.cpp src/kv.cpp src/main.cpp src/rdb.cpp src/replica_client.cpp src/resp.cpp src/server.cpp)
add_executable(redis_server ${SOURCES})
target_compile_definitions( redis_server PRIVATE $<$<CONFIG:Debug>:MYREDIS_DEBUG=1>)
target_include_directories(redis_server PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(redis_server PRIVATE Threads::Threads)




