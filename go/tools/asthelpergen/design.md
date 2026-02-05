# AST Helper Generator Design

## Overview

The `asthelpergen` package is a code generation tool that automatically creates helper methods for Abstract Syntax Tree (AST) implementations. Given a root interface, it generates various AST manipulation utilities including cloning, equality comparison, visiting, and rewriting functions.

## Architecture

### Core Components

#### 1. Main Generator (`astHelperGen`)
- **Purpose**: Central orchestrator that coordinates the code generation process
- **Key Fields**:
  - `namedIface`: The root interface for which helpers are generated
  - `gens`: Collection of specialized generators (clone, equals, visit, rewrite, etc.)
  - `todo`: Queue of types discovered during generation that need processing
  - `_scope`: Type scope for finding implementations

#### 2. Generator Interface System
The system uses a plugin-like architecture with two key interfaces:

**`generatorSPI`** (Service Provider Interface):
- Provides services to individual generators
- `addType()`: Adds newly discovered types to the processing queue
- `findImplementations()`: Discovers all types implementing a given interface
- `scope()`: Access to the type system scope

**`generator`** Interface:
- Implemented by each specialized generator
- Handles different type categories (struct, pointer, slice, basic, interface)
- Generates the final output file

### Specialized Generators

#### 1. Clone Generator (`cloneGen`)
- **Purpose**: Creates deep clone methods for AST nodes
- **Output**: `ast_clone.go`
- **Key Features**:
  - Recursive cloning with type dispatch
  - Exclusion list support for types that shouldn't be cloned
  - Handles pointers, slices, structs, and interfaces

#### 2. Equals Generator (`equalsGen`)
- **Purpose**: Creates deep equality comparison methods
- **Output**: `ast_equals.go`
- **Key Features**:
  - Custom comparator support via `Comparator` struct
  - Field-by-field comparison for structs
  - Type-safe equality checks with nil handling

#### 3. Visit Generator (`visitGen`)
- **Purpose**: Creates visitor pattern methods for AST traversal
- **Output**: `ast_visit.go`
- **Key Features**:
  - Pre-order traversal of AST nodes
  - Error propagation during traversal
  - Support for `Visitable` interface with custom visit logic

#### 4. Rewrite Generator (`rewriteGen`)
- **Purpose**: Creates AST rewriting/transformation methods
- **Output**: `ast_rewrite.go`
- **Key Features**:
  - Pre and post-order hooks for transformations
  - Path collection for tracking node locations
  - Safe node replacement with type checking

#### 5. Path Generator (`pathGen`)
- **Purpose**: Creates path enumeration for AST nodes
- **Output**: `ast_path.go`
- **Key Features**:
  - Generates constants for field paths
  - Supports path-based navigation

#### 6. Copy-on-Write Generator (`cowGen`)
- **Purpose**: Creates copy-on-write functionality
- **Output**: `ast_copy_on_rewrite.go`
- **Key Features**:
  - Efficient AST modification with minimal copying
  - Shared immutable structures where possible

## Code Generation Process

### 1. Discovery Phase
1. Load packages using `go/packages`
2. Find the root interface type
3. Initialize generators with configuration options
4. Create the main `astHelperGen` instance

### 2. Type Analysis Phase
1. Start with the root interface in the `todo` queue
2. For each type in the queue:
   - Determine its underlying type (struct, pointer, slice, etc.)
   - Call appropriate method on each generator
   - Generators may add new types to the queue
3. Continue until all discovered types are processed

### 3. Code Generation Phase
1. Each generator creates its output file
2. Files are formatted and optimized
3. License headers and generation comments are added

### 4. Output and Verification
1. Generated files are returned as a map of file paths to content
2. Optional verification against existing files on disk
3. Files can be written to disk or verified for CI/CD

## Type System Integration

### Type Discovery
The generator uses Go's type system to:
- Find all types implementing the root interface
- Discover field types that need helper methods
- Handle complex type relationships (pointers, slices, nested structs)

### Type Dispatch
Each generator implements methods for different type categories:
- `interfaceMethod`: Handles interface types with type switching
- `structMethod`: Handles struct types with field iteration
- `ptrToStructMethod`: Handles pointer-to-struct types
- `sliceMethod`: Handles slice types with element processing
- `basicMethod`: Handles basic types (int, string, etc.)

## Configuration and Options

### Clone Options
- `Exclude`: List of type patterns to exclude from cloning

### Equals Options  
- `AllowCustom`: List of types that can have custom comparators

### Main Options
- `Packages`: Go packages to analyze
- `RootInterface`: Fully qualified name of the root interface

## Integration Points

### Command Line Interface (`main/main.go`)
- Uses `spf13/pflag` for command-line argument parsing
- Supports verification mode for CI/CD pipelines
- Integrates with Vitess codegen utilities

### Testing Integration
- Integration tests verify generated code matches expectations
- Test helpers in `integration/` package provide real-world examples
- Comprehensive type coverage including edge cases

## Key Design Patterns

### 1. Plugin Architecture
Multiple generators implement the same interface, allowing easy extension with new generator types.

### 2. Type-Driven Generation
The generation process is driven by Go's type system, ensuring type safety and correctness.

### 3. Incremental Discovery
Types are discovered incrementally as generators process fields and references, ensuring complete coverage.

### 4. Template-Free Generation
Uses `dave/jennifer` for programmatic Go code generation instead of text templates, providing better type safety and refactoring support.

## Performance Considerations

- **Lazy Type Discovery**: Types are only processed when referenced
- **Deduplication**: Each type is processed only once using the `alreadyDone` map
- **Efficient Code Generation**: Jennifer library provides efficient Go code generation
- **Minimal Dependencies**: Generated code has minimal runtime dependencies

## Error Handling

- **Package Loading Errors**: Detailed error reporting for missing packages
- **Type Resolution Errors**: Clear messages when interfaces/types cannot be found
- **Generation Errors**: Panic on unexpected type scenarios with helpful context
- **Verification Errors**: File comparison errors for CI/CD integration

## Future Extensibility

The architecture supports easy addition of new generators:
1. Implement the `generator` interface
2. Add to the generator list in `GenerateASTHelpers()`
3. Follow established patterns for type dispatch and code generation

This design provides a robust, extensible foundation for AST helper code generation while maintaining type safety and performance.