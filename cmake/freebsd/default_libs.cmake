set (DEFAULT_LIBS "-nodefaultlibs")

# Always build compiler-rt from source to avoid depending on the host system's compiler-rt.
include (cmake/build_clang_builtin.cmake)
build_clang_builtin(${CMAKE_CXX_COMPILER_TARGET} BUILTINS_LIBRARY)

set (DEFAULT_LIBS "${DEFAULT_LIBS} ${BUILTINS_LIBRARY} -lc -lm -lrt -lpthread")

message(STATUS "Default libraries: ${DEFAULT_LIBS}")

set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})

add_library(Threads::Threads INTERFACE IMPORTED)
set_target_properties(Threads::Threads PROPERTIES INTERFACE_LINK_LIBRARIES pthread)

include (cmake/unwind.cmake)
include (cmake/cxx.cmake)
