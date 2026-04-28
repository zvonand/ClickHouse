set (DEFAULT_LIBS "-nodefaultlibs")

# Builtins are built as a regular cmake target in contrib/compiler-rt-cmake/
# and linked via global-libs. No execute_process needed.

set (DEFAULT_LIBS "${DEFAULT_LIBS} -lc -lm -lrt -lpthread")

message(STATUS "Default libraries: ${DEFAULT_LIBS}")

set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})

add_library(Threads::Threads INTERFACE IMPORTED)
set_target_properties(Threads::Threads PROPERTIES INTERFACE_LINK_LIBRARIES pthread)

include (cmake/unwind.cmake)
include (cmake/cxx.cmake)
