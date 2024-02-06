add_subdirectory(statement)

add_library(
  bustub_binder
  OBJECT
  bind_create.cpp
  bind_insert.cpp
  bind_select.cpp
  bind_variable.cpp
  binder.cpp
  bound_statement.cpp
  fmt_impl.cpp
  node_tag_to_string.cpp
  transformer.cpp
)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bustub_binder>
  PARENT_SCOPE)
