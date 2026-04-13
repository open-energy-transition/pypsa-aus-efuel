#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "highs::highs" for configuration "Release"
set_property(TARGET highs::highs APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(highs::highs PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "C;CXX"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libhighs.a"
  )

list(APPEND _cmake_import_check_targets highs::highs )
list(APPEND _cmake_import_check_files_for_highs::highs "${_IMPORT_PREFIX}/lib/libhighs.a" )

# Import target "highs::OpenBLAS" for configuration "Release"
set_property(TARGET highs::OpenBLAS APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(highs::OpenBLAS PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "ASM;C"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libopenblas.a"
  )

list(APPEND _cmake_import_check_targets highs::OpenBLAS )
list(APPEND _cmake_import_check_files_for_highs::OpenBLAS "${_IMPORT_PREFIX}/lib/libopenblas.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
