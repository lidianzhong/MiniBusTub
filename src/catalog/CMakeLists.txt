add_library(
        bustub_catalog
        OBJECT
        column.cpp
        schema.cpp
        table_generator.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bustub_catalog>
        PARENT_SCOPE)