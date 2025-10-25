# Test Suite for DBS2JSON

This directory contains comprehensive test coverage for the dbs2json project.

## Test Structure

### Unit Tests

- **`test_database.py`** - Tests for database extraction functionality
  - SQLite database extraction
  - Plist file extraction
  - Error handling and edge cases

- **`test_processor.py`** - Tests for the main database processor
  - File discovery
  - Content processing
  - Binary data handling
  - Sorting and filtering

- **`test_file_detector.py`** - Tests for file type detection
  - SQLite file detection
  - Plist file detection
  - Encrypted SQLite detection
  - Magic byte verification

- **`test_helpers.py`** - Tests for utility functions
  - Path sanitization
  - Date formatting
  - Dictionary flattening for CSV export

- **`test_json_exporter.py`** - Tests for JSON export functionality
  - Single file export
  - Large dataset export (multiple files)
  - Error handling and fallbacks

- **`test_csv_exporter.py`** - Tests for CSV export functionality
  - SQLite table export
  - Plist data export
  - Special character handling

- **`test_args.py`** - Tests for command line argument parsing
  - Argument validation
  - Default values
  - Error handling

### Integration Tests

- **`test_integration.py`** - End-to-end workflow tests
  - Complete processing pipeline
  - Mixed file types
  - Error scenarios
  - Performance with large datasets

## Test Fixtures

The `conftest.py` file provides common test fixtures:

- `temp_dir` - Temporary directory for tests
- `sample_sqlite_db` - Sample SQLite database with test data
- `sample_plist_file` - Sample plist file with various data types
- `encrypted_sqlite_file` - Simulated encrypted SQLite file
- `sample_key_files_data` - Sample processed data structure
- `complex_nested_dict` - Complex nested dictionary for testing

## Running Tests

### Prerequisites

Install the required testing dependencies:

```bash
pip install pytest pytest-mock
```

### Basic Test Execution

Run all tests:
```bash
pytest
```

Run tests with verbose output:
```bash
pytest -v
```

Run specific test file:
```bash
pytest tests/test_database.py
```

Run specific test class:
```bash
pytest tests/test_database.py::TestExtractSqliteToDict
```

Run specific test method:
```bash
pytest tests/test_database.py::TestExtractSqliteToDict::test_extract_valid_sqlite_db
```

### Test Categories

Run only unit tests:
```bash
pytest -m unit
```

Run only integration tests:
```bash
pytest -m integration
```

Skip slow tests:
```bash
pytest -m "not slow"
```

### Coverage Report

Generate coverage report:
```bash
pip install pytest-cov
pytest --cov=. --cov-report=html
```

Generate coverage report in terminal:
```bash
pytest --cov=. --cov-report=term-missing
```

### Test Output Formats

Generate JSON test results:
```bash
pytest --json-report=test-results.json
```

Generate JUnit XML for CI systems:
```bash
pytest --junit-xml=test-results.xml
```

## Test Data

### Sample SQLite Database Structure

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    age INTEGER
);

CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    price REAL,
    description TEXT
);
```

### Sample Plist Content

```python
{
    "name": "Test Application",
    "version": "1.0.0",
    "settings": {
        "theme": "dark",
        "notifications": True,
        "max_connections": 10
    },
    "users": [
        {"id": 1, "name": "Alice", "role": "admin"},
        {"id": 2, "name": "Bob", "role": "user"}
    ]
}
```

## Best Practices

### Writing New Tests

1. **Use descriptive test names** that clearly indicate what is being tested
2. **Follow the AAA pattern**: Arrange, Act, Assert
3. **Test one thing per test** - avoid testing multiple scenarios in one test
4. **Use fixtures** for common test data and setup
5. **Mock external dependencies** to ensure tests are isolated
6. **Test both happy path and error conditions**
7. **Include edge cases** and boundary conditions

### Test Organization

- Group related tests in classes
- Use meaningful test method names
- Add docstrings to explain complex test scenarios
- Use parametrized tests for similar test cases with different inputs

### Example Test Structure

```python
class TestFeature:
    """Test cases for feature X."""

    def test_feature_normal_case(self, fixture):
        """Test feature works correctly with normal input."""
        # Arrange
        # Act
        # Assert

    def test_feature_edge_case(self, fixture):
        """Test feature handles edge cases correctly."""
        # Arrange
        # Act
        # Assert

    def test_feature_error_case(self, fixture):
        """Test feature handles errors gracefully."""
        # Arrange
        # Act
        # Assert
```

## Debugging Tests

### Running Tests in Debug Mode

```bash
pytest --pdb
```

### Stopping at First Failure

```bash
pytest -x
```

### Running Tests with Maximum Verbosity

```bash
pytest -vv -s
```

### Print Local Variables on Failure

```bash
pytest -l
```

## Continuous Integration

The tests are designed to work well with CI/CD systems:

- Use JUnit XML output for test results
- Coverage reports for code quality metrics
- Tests are fast and isolated
- No external dependencies required for testing

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure you're running tests from the project root directory
2. **Permission Errors**: Make sure temporary directories can be created
3. **Database Locks**: Ensure test databases are properly closed after tests
4. **File Path Issues**: Use absolute paths or proper relative paths in tests

### Test Performance

- Tests use temporary directories that are automatically cleaned up
- Database operations use in-memory or temporary files
- Tests are designed to run quickly and independently

## Contributing

When adding new features:

1. Write tests for the new functionality
2. Ensure all tests pass before submitting
3. Maintain or improve test coverage
4. Follow the existing test patterns and conventions
5. Add appropriate fixtures if needed