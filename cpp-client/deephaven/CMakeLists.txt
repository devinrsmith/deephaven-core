cmake_minimum_required(VERSION 3.14)

if(TARGET client)
    # The library has already been built (i.e. through some
    # other path in the build graph).
    return()
endif()

project(deephaven)

set(deephaven_VERSION 43.0)
set(CMAKE_CXX_STANDARD 17)

if(WIN32)
    add_compile_definitions(WIN32_LEAN_AND_MEAN NOMINMAX)
endif()

# for CMAKE_INSTALL_{dir}
include(GNUInstallDirs)

# To enable address sanitizer, add `-DSANITIZE_ADDRESS=ON` to
# the list of options you pass for running cmake.
option(SANITIZE_ADDRESS "Enable address sanitizer" "OFF")
if(${SANITIZE_ADDRESS} STREQUAL "ON")
  add_compile_options("-fsanitize=address")
  link_libraries("-fsanitize=address")
  # Sanitizer instrumentation makes GCC's -Wmaybe-uninitialized produce false
  # positives (e.g. in protobuf's headers), which -Werror would turn into errors.
  add_compile_options("$<$<CXX_COMPILER_ID:GNU>:-Wno-maybe-uninitialized>")
endif()

option(DHCORE_ONLY "Only build dhcore, skip rest" OFF)

# Build dhclient as a static library (and dhcore only as dhcore_static), linked
# against the static Arrow/Arrow Flight targets, so programs using the client can
# be linked fully statically. Requires static builds of all dependencies.
option(DH_STATIC_CLIENT "Build a static dhclient against static dependencies" OFF)

add_subdirectory(dhcore)
if(NOT DHCORE_ONLY)
    add_subdirectory(dhclient)
    add_subdirectory(tests)
    add_subdirectory(examples)
endif()

install(DIRECTORY dhcore/include/public/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )

if(NOT DHCORE_ONLY)
    install(DIRECTORY dhclient/include/public/
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        )

    if(DH_STATIC_CLIENT)
      set(DH_INSTALL_TARGETS dhclient dhcore_static dhclient_tests)
    else()
      set(DH_INSTALL_TARGETS dhclient dhcore dhcore_static dhclient_tests)
    endif()
    install(TARGETS ${DH_INSTALL_TARGETS}
        EXPORT deephavenTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        )

      include(CMakePackageConfigHelpers)
      write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/deephaven/deephavenConfigVersion.cmake"
        VERSION ${deephaven_VERSION}
        COMPATIBILITY AnyNewerVersion
        )

      export(EXPORT deephavenTargets
        FILE "${CMAKE_CURRENT_BINARY_DIR}/deephaven/deephavenTargets.cmake"
        NAMESPACE deephaven::
        )
      configure_file(cmake/deephavenConfig.cmake
        "${CMAKE_CURRENT_BINARY_DIR}/deephaven/deephavenConfig.cmake"
        COPYONLY
        )

      set(ConfigPackageLocation ${CMAKE_INSTALL_LIBDIR}/cmake/deephaven)
      install(EXPORT deephavenTargets
        FILE
        deephavenTargets.cmake
        NAMESPACE
        deephaven::
        DESTINATION
        ${ConfigPackageLocation}
        )
      install(
        FILES
        cmake/deephavenConfig.cmake
        "${CMAKE_CURRENT_BINARY_DIR}/deephaven/deephavenConfigVersion.cmake"
        DESTINATION
        ${ConfigPackageLocation}
        COMPONENT
        Devel
        )
      
endif()
