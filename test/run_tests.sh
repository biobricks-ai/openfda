#!/bin/bash

# Test runner script for json2parquet.py
# This script provides convenient ways to run the test suite

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}JSON to Parquet Test Suite${NC}"
echo "================================="

# Function to run tests with nix develop
run_test() {
    echo -e "\n${YELLOW}$1${NC}"
    echo "-----------------------------------"
    nix develop --command python3 -m pytest $2 $3
}

# Check command line arguments
case "${1:-all}" in
    "all")
        echo "Running all tests..."
        run_test "All Tests" "test/" "-v"
        ;;
    "core")
        echo "Running core functionality tests..."
        run_test "Core Tests" "test/test_json2parquet.py" "-v"
        ;;
    "extended")
        echo "Running extended tests..."
        run_test "Extended Tests" "test/test_json2parquet_extended.py" "-v"
        ;;
    "unit")
        echo "Running unit tests only..."
        run_test "Unit Tests" "test/test_json2parquet.py::TestSafeConvertToString test/test_json2parquet.py::TestFlattenComplexData" "-v"
        ;;
    "integration")
        echo "Running integration tests..."
        run_test "Integration Tests" "test/test_json2parquet.py::TestConvertJsonToParquet test/test_json2parquet.py::TestJson2ParquetScript" "-v"
        ;;
    "performance")
        echo "Running performance tests..."
        run_test "Performance Tests" "test/test_json2parquet_extended.py::TestPerformance" "-v"
        ;;
    "edge")
        echo "Running edge case tests..."
        run_test "Edge Case Tests" "test/test_json2parquet_extended.py::TestEdgeCases" "-v"
        ;;
    "error")
        echo "Running error handling tests..."
        run_test "Error Handling Tests" "test/test_json2parquet_extended.py::TestErrorHandling" "-v"
        ;;
    "quick")
        echo "Running a quick smoke test..."
        run_test "Quick Smoke Test" "test/test_json2parquet.py::TestSafeConvertToString::test_simple_string test/test_json2parquet.py::TestJson2ParquetScript::test_openfda_structure" "-v"
        ;;
    "help"|"-h"|"--help")
        echo "Usage: $0 [test_type]"
        echo ""
        echo "Available test types:"
        echo "  all         - Run all tests (default)"
        echo "  core        - Run core functionality tests"
        echo "  extended    - Run extended tests"
        echo "  unit        - Run unit tests only"
        echo "  integration - Run integration tests"
        echo "  performance - Run performance tests"
        echo "  edge        - Run edge case tests"
        echo "  error       - Run error handling tests"
        echo "  quick       - Run a quick smoke test"
        echo "  help        - Show this help message"
        echo ""
        echo "Examples:"
        echo "  $0              # Run all tests"
        echo "  $0 core         # Run core tests only"
        echo "  $0 quick        # Run quick smoke test"
        exit 0
        ;;
    *)
        echo -e "${RED}Error: Unknown test type '$1'${NC}"
        echo "Use '$0 help' to see available options."
        exit 1
        ;;
esac

echo -e "\n${GREEN}Test run completed!${NC}" 