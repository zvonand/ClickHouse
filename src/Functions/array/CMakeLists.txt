include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
include(CMakePrintHelpers)

add_headers_and_sources(clickhouse_functions_array .)

# createInternalFunctionHasOverloadResolver, FunctionArrayResize,  FunctionEmptyArrayToSingle
# Used by ArrayExistsToHasPass.cpp and ArrayJoinAction.cpp
set(DBMS_ARRAY_FUNCTIONS
    arrayResize.cpp
    has.cpp
    emptyArrayToSingle.cpp
)
extract_into_parent_list(clickhouse_functions_array_sources dbms_sources ${DBMS_ARRAY_FUNCTIONS})
extract_into_parent_list(clickhouse_functions_headers dbms_headers arrayResize.h has.h emptyArrayToSingle.h)

add_library(clickhouse_functions_array OBJECT ${clickhouse_functions_array_sources} ${clickhouse_functions_array_headers})
target_link_libraries(clickhouse_functions_array PRIVATE dbms clickhouse_functions_gatherutils ch_contrib::stringzilla)

if (TARGET ch_contrib::vectorscan)
    target_link_libraries(clickhouse_functions_array PRIVATE ch_contrib::vectorscan)
endif()

# Libcxx hardening means out-of-bounds checks which causes noticeable degradations for some workflows.
# TUs where libcxx hardening needs to be disabled are specified here.
set_source_files_properties(array.cpp PROPERTIES COMPILE_FLAGS "-U_LIBCPP_HARDENING_MODE -D_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_NONE")

# On x86-64-v3 the SLP vectorizer widens the accumulator loop in arrayNorm to
# 256-bit YMM, which is slower for LinfNorm with BFloat16/Float32 and causes
# register spills for sum-based norms.  Compile this file at v2 level to keep
# the 128-bit codegen that matches master's performance.
#
# Same pattern applies to arrayDistance.cpp: at v3 the SLP vectorizer collapses the
# 8-way `states[VEC_SIZE]` accumulator into a single 256-bit YMM and uses
# `vpmovzxwd` + `vpslld` for the BF16->Float32 expansion. The result is fewer
# instructions but a longer per-iteration latency chain on the BF16 paths of
# `L1Distance`/`LinfDistance`/`LpDistance`/`cosineDistance` (no AVX-512
# specialisation) - IPC drops from 3.40 to 1.89 and the BF16 distance queries
# regress ~7-12%. Function-level `__attribute__((target("...")))` on the L2
# AVX-512 specialisations overrides this file flag, so those keep their
# Sapphire Rapids `_mm512_cvtpbh_ps` codegen.
if (ARCH_AMD64 AND X86_ARCH_LEVEL VERSION_GREATER_EQUAL 3)
    set_source_files_properties(arrayNorm.cpp PROPERTIES COMPILE_FLAGS "-march=x86-64-v2")
    set_source_files_properties(arrayDistance.cpp PROPERTIES COMPILE_FLAGS "-march=x86-64-v2")
endif()

if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options(clickhouse_functions_array PRIVATE "-g1")
endif()
