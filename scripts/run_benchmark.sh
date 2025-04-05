#!/bin/bash

# MetricMind - Dremio Query Benchmarking System
# Benchmark automation script

# Load environment variables
if [ -f .env ]; then
    source .env
else
    echo "Error: .env file not found. Please create one based on .env.example"
    exit 1
fi

# Configuration
ITERATIONS=${ITERATIONS:-3}
WAIT_TIME=${WAIT_TIME:-60}
SCENARIO=${SCENARIO:-"default"}
LOG_LEVEL=${LOG_LEVEL:-"INFO"}
RESULTS_DIR="results"
JMETER_TEST_PLAN="jmeter/dremio_benchmark.jmx"
LOG_FILE="benchmark.log"
API_URL=${API_URL:-"http://localhost:8000"}
DD_API_KEY=${DD_API_KEY:-""}
DD_APP_KEY=${DD_APP_KEY:-""}
DD_ENABLED=${DD_ENABLED:-"false"}

# Create results directory with timestamp
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULTS_DIR="${RESULTS_DIR}/${TIMESTAMP}"
mkdir -p "${RESULTS_DIR}"

# Logging function
log() {
    local level=$1
    shift
    local message=$*
    local timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    
    # Check if log level is enabled
    if [[ "$LOG_LEVEL" == "DEBUG" ]] || 
       [[ "$LOG_LEVEL" == "INFO" && "$level" != "DEBUG" ]] || 
       [[ "$LOG_LEVEL" == "WARN" && "$level" == "WARN" ]] || 
       [[ "$LOG_LEVEL" == "ERROR" && "$level" == "ERROR" ]]; then
        echo "[${timestamp}] [${level}] ${message}" | tee -a "${LOG_FILE}"
    fi
}

# Function to check if FastAPI service is running
check_fastapi_service() {
    log "INFO" "Checking if FastAPI service is running..."
    
    if curl -s "${API_URL}/health" > /dev/null; then
        log "INFO" "FastAPI service is running"
        return 0
    else
        log "ERROR" "FastAPI service is not running. Please start it with: python -m MetricMind.app.main"
        return 1
    fi
}

# Function to run a single benchmark iteration
run_iteration() {
    local iteration=$1
    local scenario=$2
    local output_file="${RESULTS_DIR}/iteration_${iteration}_${scenario}.json"
    
    log "INFO" "Running iteration ${iteration}/${ITERATIONS} for scenario: ${scenario}"
    
    # Run JMeter test plan
    log "INFO" "Executing JMeter test plan: ${JMETER_TEST_PLAN}"
    jmeter -n -t "${JMETER_TEST_PLAN}" -l "${RESULTS_DIR}/jmeter_${iteration}_${scenario}.jtl" -e -o "${RESULTS_DIR}/jmeter_report_${iteration}_${scenario}"
    
    if [ $? -ne 0 ]; then
        log "ERROR" "JMeter test plan execution failed"
        return 1
    fi
    
    # Run comparison query
    log "INFO" "Executing comparison query"
    curl -s "${API_URL}/compare/query" -o "${output_file}"
    
    if [ $? -ne 0 ]; then
        log "ERROR" "Comparison query execution failed"
        return 1
    fi
    
    # Extract metrics using jq
    if command -v jq &> /dev/null; then
        log "INFO" "Extracting metrics from results"
        
        # Extract execution times
        dremio1_time=$(jq -r '.dremio1.execution_time' "${output_file}")
        dremio2_time=$(jq -r '.dremio2.execution_time' "${output_file}")
        
        # Extract row counts
        dremio1_rows=$(jq -r '.dremio1.row_count' "${output_file}")
        dremio2_rows=$(jq -r '.dremio2.row_count' "${output_file}")
        
        # Extract payload sizes
        dremio1_payload=$(jq -r '.dremio1.payload_size' "${output_file}")
        dremio2_payload=$(jq -r '.dremio2.payload_size' "${output_file}")
        
        # Extract match status
        match=$(jq -r '.match' "${output_file}")
        
        # Send metrics to Datadog if enabled
        if [[ "$DD_ENABLED" == "true" && -n "$DD_API_KEY" && -n "$DD_APP_KEY" ]]; then
            log "INFO" "Sending metrics to Datadog"
            
            # Current timestamp in milliseconds
            timestamp=$(date +%s%3N)
            
            # Send metrics using curl
            curl -s -X POST "https://api.datadoghq.com/api/v1/series" \
                -H "Content-Type: application/json" \
                -H "DD-API-KEY: ${DD_API_KEY}" \
                -H "DD-APPLICATION-KEY: ${DD_APP_KEY}" \
                -d "{
                    \"series\": [
                        {
                            \"metric\": \"metricmind.dremio1.execution_time\",
                            \"points\": [[${timestamp}, ${dremio1_time}]],
                            \"type\": \"gauge\",
                            \"tags\": [\"scenario:${scenario}\", \"iteration:${iteration}\"]
                        },
                        {
                            \"metric\": \"metricmind.dremio2.execution_time\",
                            \"points\": [[${timestamp}, ${dremio2_time}]],
                            \"type\": \"gauge\",
                            \"tags\": [\"scenario:${scenario}\", \"iteration:${iteration}\"]
                        },
                        {
                            \"metric\": \"metricmind.dremio1.row_count\",
                            \"points\": [[${timestamp}, ${dremio1_rows}]],
                            \"type\": \"gauge\",
                            \"tags\": [\"scenario:${scenario}\", \"iteration:${iteration}\"]
                        },
                        {
                            \"metric\": \"metricmind.dremio2.row_count\",
                            \"points\": [[${timestamp}, ${dremio2_rows}]],
                            \"type\": \"gauge\",
                            \"tags\": [\"scenario:${scenario}\", \"iteration:${iteration}\"]
                        },
                        {
                            \"metric\": \"metricmind.dremio1.payload_size\",
                            \"points\": [[${timestamp}, ${dremio1_payload}]],
                            \"type\": \"gauge\",
                            \"tags\": [\"scenario:${scenario}\", \"iteration:${iteration}\"]
                        },
                        {
                            \"metric\": \"metricmind.dremio2.payload_size\",
                            \"points\": [[${timestamp}, ${dremio2_payload}]],
                            \"type\": \"gauge\",
                            \"tags\": [\"scenario:${scenario}\", \"iteration:${iteration}\"]
                        },
                        {
                            \"metric\": \"metricmind.comparison.match\",
                            \"points\": [[${timestamp}, ${match}]],
                            \"type\": \"gauge\",
                            \"tags\": [\"scenario:${scenario}\", \"iteration:${iteration}\"]
                        }
                    ]
                }"
        else
            log "WARN" "Datadog metrics disabled or missing API keys"
        fi
        
        log "INFO" "Iteration ${iteration} completed successfully"
        return 0
    else
        log "ERROR" "jq is not installed. Please install it to extract metrics."
        return 1
    fi
}

# Function to run a specific scenario
run_scenario() {
    local scenario=$1
    
    log "INFO" "Running scenario: ${scenario}"
    
    case $scenario in
        "simple")
            log "INFO" "Running simple query scenario"
            for i in $(seq 1 $ITERATIONS); do
                run_iteration $i "simple"
                sleep $WAIT_TIME
            done
            ;;
        "complex")
            log "INFO" "Running complex query scenario"
            for i in $(seq 1 $ITERATIONS); do
                run_iteration $i "complex"
                sleep $WAIT_TIME
            done
            ;;
        "compare")
            log "INFO" "Running comparison scenario"
            for i in $(seq 1 $ITERATIONS); do
                run_iteration $i "compare"
                sleep $WAIT_TIME
            done
            ;;
        "full")
            log "INFO" "Running full benchmark scenario"
            for i in $(seq 1 $ITERATIONS); do
                run_iteration $i "simple"
                sleep $WAIT_TIME
                run_iteration $i "complex"
                sleep $WAIT_TIME
                run_iteration $i "compare"
                sleep $WAIT_TIME
            done
            ;;
        "default")
            log "INFO" "Running default scenario (simple + compare)"
            for i in $(seq 1 $ITERATIONS); do
                run_iteration $i "simple"
                sleep $WAIT_TIME
                run_iteration $i "compare"
                sleep $WAIT_TIME
            done
            ;;
        *)
            log "ERROR" "Unknown scenario: ${scenario}"
            return 1
            ;;
    esac
    
    return 0
}

# Function to generate summary report
generate_report() {
    log "INFO" "Generating summary report"
    
    # Create report file
    REPORT_FILE="${RESULTS_DIR}/summary_report.md"
    echo "# MetricMind Benchmark Summary Report" > "${REPORT_FILE}"
    echo "Generated on: $(date)" >> "${REPORT_FILE}"
    echo "" >> "${REPORT_FILE}"
    
    # Collect all JSON results
    JSON_FILES=($(find "${RESULTS_DIR}" -name "iteration_*.json"))
    
    if [ ${#JSON_FILES[@]} -eq 0 ]; then
        log "ERROR" "No results found to generate report"
        echo "## Error: No results found" >> "${REPORT_FILE}"
        return 1
    fi
    
    # Calculate statistics
    log "INFO" "Calculating statistics"
    
    # Initialize arrays
    dremio1_times=()
    dremio2_times=()
    dremio1_rows=()
    dremio2_rows=()
    dremio1_payloads=()
    dremio2_payloads=()
    matches=()
    
    # Extract data from JSON files
    for file in "${JSON_FILES[@]}"; do
        dremio1_times+=($(jq -r '.dremio1.execution_time' "$file"))
        dremio2_times+=($(jq -r '.dremio2.execution_time' "$file"))
        dremio1_rows+=($(jq -r '.dremio1.row_count' "$file"))
        dremio2_rows+=($(jq -r '.dremio2.row_count' "$file"))
        dremio1_payloads+=($(jq -r '.dremio1.payload_size' "$file"))
        dremio2_payloads+=($(jq -r '.dremio2.payload_size' "$file"))
        matches+=($(jq -r '.match' "$file"))
    done
    
    # Calculate averages
    dremio1_avg_time=$(echo "${dremio1_times[@]}" | awk '{ sum=0; for(i=1;i<=NF;i++) sum+=$i; print sum/NF; }')
    dremio2_avg_time=$(echo "${dremio2_times[@]}" | awk '{ sum=0; for(i=1;i<=NF;i++) sum+=$i; print sum/NF; }')
    dremio1_avg_rows=$(echo "${dremio1_rows[@]}" | awk '{ sum=0; for(i=1;i<=NF;i++) sum+=$i; print sum/NF; }')
    dremio2_avg_rows=$(echo "${dremio2_rows[@]}" | awk '{ sum=0; for(i=1;i<=NF;i++) sum+=$i; print sum/NF; }')
    dremio1_avg_payload=$(echo "${dremio1_payloads[@]}" | awk '{ sum=0; for(i=1;i<=NF;i++) sum+=$i; print sum/NF; }')
    dremio2_avg_payload=$(echo "${dremio2_payloads[@]}" | awk '{ sum=0; for(i=1;i<=NF;i++) sum+=$i; print sum/NF; }')
    
    # Calculate match percentage
    match_count=0
    for match in "${matches[@]}"; do
        if [[ "$match" == "true" ]]; then
            match_count=$((match_count + 1))
        fi
    done
    match_percentage=$(echo "scale=2; $match_count / ${#matches[@]} * 100" | bc)
    
    # Write summary to report
    echo "## Summary Statistics" >> "${REPORT_FILE}"
    echo "" >> "${REPORT_FILE}"
    echo "| Metric | Dremio 1 | Dremio 2 | Difference |" >> "${REPORT_FILE}"
    echo "|--------|----------|----------|------------|" >> "${REPORT_FILE}"
    echo "| Avg Execution Time (s) | ${dremio1_avg_time} | ${dremio2_avg_time} | $(echo "scale=2; ${dremio2_avg_time} - ${dremio1_avg_time}" | bc) |" >> "${REPORT_FILE}"
    echo "| Avg Row Count | ${dremio1_avg_rows} | ${dremio2_avg_rows} | $(echo "scale=2; ${dremio2_avg_rows} - ${dremio1_avg_rows}" | bc) |" >> "${REPORT_FILE}"
    echo "| Avg Payload Size (bytes) | ${dremio1_avg_payload} | ${dremio2_avg_payload} | $(echo "scale=2; ${dremio2_avg_payload} - ${dremio1_avg_payload}" | bc) |" >> "${REPORT_FILE}"
    echo "| Result Match Percentage | - | - | ${match_percentage}% |" >> "${REPORT_FILE}"
    echo "" >> "${REPORT_FILE}"
    
    # Add JMeter summary
    echo "## JMeter Test Results" >> "${REPORT_FILE}"
    echo "" >> "${REPORT_FILE}"
    echo "JMeter reports are available in the following directories:" >> "${REPORT_FILE}"
    echo "" >> "${REPORT_FILE}"
    for i in $(seq 1 $ITERATIONS); do
        echo "- [Iteration ${i} JMeter Report](${RESULTS_DIR}/jmeter_report_${i}_${SCENARIO}/index.html)" >> "${REPORT_FILE}"
    done
    echo "" >> "${REPORT_FILE}"
    
    # Add conclusion
    echo "## Conclusion" >> "${REPORT_FILE}"
    echo "" >> "${REPORT_FILE}"
    if (( $(echo "$dremio1_avg_time < $dremio2_avg_time" | bc -l) )); then
        echo "Dremio 1 performed better than Dremio 2 in terms of execution time." >> "${REPORT_FILE}"
    else
        echo "Dremio 2 performed better than Dremio 1 in terms of execution time." >> "${REPORT_FILE}"
    fi
    echo "" >> "${REPORT_FILE}"
    if (( $(echo "$match_percentage < 100" | bc -l) )); then
        echo "There were differences in the query results between Dremio 1 and Dremio 2." >> "${REPORT_FILE}"
    else
        echo "The query results were identical between Dremio 1 and Dremio 2." >> "${REPORT_FILE}"
    fi
    echo "" >> "${REPORT_FILE}"
    
    log "INFO" "Summary report generated: ${REPORT_FILE}"
    return 0
}

# Main function
main() {
    log "INFO" "Starting MetricMind benchmark"
    log "INFO" "Results will be saved to: ${RESULTS_DIR}"
    
    # Check if FastAPI service is running
    if ! check_fastapi_service; then
        log "ERROR" "Cannot proceed with benchmark. FastAPI service is not running."
        return 1
    fi
    
    # Run the specified scenario
    if ! run_scenario $SCENARIO; then
        log "ERROR" "Failed to run scenario: ${SCENARIO}"
        return 1
    fi
    
    # Generate summary report
    if ! generate_report; then
        log "ERROR" "Failed to generate summary report"
        return 1
    fi
    
    log "INFO" "Benchmark completed successfully"
    log "INFO" "Summary report: ${RESULTS_DIR}/summary_report.md"
    return 0
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -i|--iterations)
            ITERATIONS="$2"
            shift 2
            ;;
        -w|--wait)
            WAIT_TIME="$2"
            shift 2
            ;;
        -s|--scenario)
            SCENARIO="$2"
            shift 2
            ;;
        -l|--log-level)
            LOG_LEVEL="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  -i, --iterations NUM    Number of iterations to run (default: 3)"
            echo "  -w, --wait SECONDS      Wait time between iterations in seconds (default: 60)"
            echo "  -s, --scenario NAME     Scenario to run: simple, complex, compare, full, default (default: default)"
            echo "  -l, --log-level LEVEL   Log level: DEBUG, INFO, WARN, ERROR (default: INFO)"
            echo "  -h, --help              Show this help message"
            exit 0
            ;;
        *)
            log "ERROR" "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Run main function
main
exit $? 