# Set standard, system and compiler libraries explicitly.
# This is intended for more control of what we are linking.

set (DEFAULT_LIBS "-nodefaultlibs")

# All compiler-rt runtimes (builtins, sanitizers, XRay) are built as regular
# cmake targets in contrib/compiler-rt-cmake/. Their .a files are linked here
# with --whole-archive so compiler-generated calls always resolve.
#
# We pass the .a paths via CMAKE_EXE_LINKER_FLAGS rather than via cmake target
# names because $<LINK_LIBRARY:WHOLE_ARCHIVE,...> doesn't survive the
# $<TARGET_PROPERTY:global-libs,INTERFACE_LINK_LIBRARIES> indirection used in
# global-group (see CMakeLists.txt around line 437). The build-order
# dependency on the compiler-rt targets is established in
# contrib/compiler-rt-cmake/CMakeLists.txt by adding clang_rt_builtins to
# global-libs.
set (COMPILER_RT_DIR "${CMAKE_BINARY_DIR}/contrib/compiler-rt-cmake")
set (BUILTINS_LIBRARY "${COMPILER_RT_DIR}/libclang_rt_builtins.a")

set (SANITIZER_RUNTIMES "")
if (SANITIZE STREQUAL "address" OR SANITIZE STREQUAL "address,undefined")
    # When ASan and UBSan are combined, the ASan runtime covers UBSan too.
    # ubsan_standalone must NOT be added here — it shares sanitizer_common
    # symbols with asan and causes duplicate symbol errors.
    set (SANITIZER_RUNTIMES
        "${COMPILER_RT_DIR}/libclang_rt_asan_static.a"
        "${COMPILER_RT_DIR}/libclang_rt_asan.a"
        "${COMPILER_RT_DIR}/libclang_rt_asan_cxx.a"
    )
elseif (SANITIZE STREQUAL "memory")
    set (SANITIZER_RUNTIMES
        "${COMPILER_RT_DIR}/libclang_rt_msan.a"
        "${COMPILER_RT_DIR}/libclang_rt_msan_cxx.a"
    )
elseif (SANITIZE STREQUAL "thread")
    set (SANITIZER_RUNTIMES
        "${COMPILER_RT_DIR}/libclang_rt_tsan.a"
        "${COMPILER_RT_DIR}/libclang_rt_tsan_cxx.a"
    )
elseif (SANITIZE STREQUAL "undefined")
    set (SANITIZER_RUNTIMES
        "${COMPILER_RT_DIR}/libclang_rt_ubsan_standalone.a"
        "${COMPILER_RT_DIR}/libclang_rt_ubsan_standalone_cxx.a"
    )
endif()
if (SANITIZE)
    # Tell clang not to inject its own (host-system) sanitizer runtime — we provide ours.
    list (APPEND SANITIZER_RUNTIMES "-fno-sanitize-link-runtime")
endif()
if (ENABLE_XRAY)
    list (APPEND SANITIZER_RUNTIMES
        "-fno-xray-link-deps"
        "${COMPILER_RT_DIR}/libclang_rt_xray.a"
    )
endif()
string (REPLACE ";" " " SANITIZER_RUNTIMES "${SANITIZER_RUNTIMES}")

option (ENABLE_LLVM_LIBC_MATH "Use math from llvm-libc instead of glibc" ON)
if (NOT (ARCH_AMD64 OR ARCH_AARCH64))
    set(ENABLE_LLVM_LIBC_MATH OFF)
endif()

if (ENABLE_LLVM_LIBC_MATH)
    link_directories("${CMAKE_BINARY_DIR}/contrib/libllvmlibc-cmake")

    if (ARCH_AMD64)
        if (X86_ARCH_LEVEL VERSION_LESS 2)
            # Compat mode: single library, no dispatch
            target_link_libraries(global-libs INTERFACE libllvmlibc)
            set (DEFAULT_LIBS "${DEFAULT_LIBS} -llibllvmlibc")
        else()
            # Dispatch mode: v2/v3 variants with runtime CPU detection
            target_link_libraries(global-libs INTERFACE llvmlibc_dispatch libllvmlibc_x86_64_v2 libllvmlibc_x86_64_v3)
            set (DEFAULT_LIBS "${DEFAULT_LIBS} -lllvmlibc_dispatch -llibllvmlibc_x86_64_v2 -llibllvmlibc_x86_64_v3")
        endif()
    elseif (ARCH_AARCH64)
        target_link_libraries(global-libs INTERFACE libllvmlibc)
        set (DEFAULT_LIBS "${DEFAULT_LIBS} -llibllvmlibc")
    endif()
endif()

if (OS_ANDROID)
    # pthread and rt are included in libc
    set (DEFAULT_LIBS "${DEFAULT_LIBS} -lc -lm -ldl")
elseif (USE_MUSL)
    set (DEFAULT_LIBS "${DEFAULT_LIBS} -static -lc")
else ()
    set (DEFAULT_LIBS "${DEFAULT_LIBS} -lc -lm -lrt -lpthread -ldl")
endif ()

message(STATUS "Default libraries: ${DEFAULT_LIBS}")
message(STATUS "Builtins library: ${BUILTINS_LIBRARY}")
if (SANITIZER_RUNTIMES)
    message(STATUS "Sanitizer/XRay runtimes: ${SANITIZER_RUNTIMES}")
endif()

# Link all compiler-rt runtimes with --whole-archive so the linker keeps every
# object — compiler-generated calls (builtins, sanitizer interceptors, XRay
# trampolines) may not be referenced by any object file directly.
# global-libs already adds the cmake targets for build-order dependency.
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--whole-archive ${BUILTINS_LIBRARY} ${SANITIZER_RUNTIMES} -Wl,--no-whole-archive")

# Other libraries go last
set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})

add_library(Threads::Threads INTERFACE IMPORTED)
set_target_properties(Threads::Threads PROPERTIES INTERFACE_LINK_LIBRARIES pthread)

include (cmake/unwind.cmake)
include (cmake/cxx.cmake)

if (NOT OS_ANDROID)
    if (NOT USE_MUSL)
        disable_dummy_launchers_if_needed()
        # Our compatibility layer doesn't build under Android, many errors in musl.
        add_subdirectory(base/glibc-compatibility)
        enable_dummy_launchers_if_needed()
    endif ()
    add_subdirectory(base/harmful)
endif ()
