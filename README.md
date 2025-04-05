# MetricMind

A comprehensive benchmarking system for comparing Dremio instances using TPC-DS queries.

## Features

- FastAPI-based REST API for query execution and comparison
- TPC-DS data generation and upload automation
- JMeter-based load testing
- Automated benchmark execution with configurable scenarios
- Detailed performance metrics collection
- Datadog integration for metrics visualization
- Comprehensive reporting system

## Prerequisites

- Python 3.8+
- Apache JMeter
- Dremio instances (source and target)
- HDFS cluster
- Datadog account (optional)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/RaoruNaren219/MetricMind.git
cd MetricMind
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Copy the environment configuration:
```bash
cp .env.example .env
```

5. Update the `.env` file with your configuration:
```env
# Dremio Configuration
DREMIO1_HOST=your-dremio1-host
DREMIO1_PORT=9047
DREMIO1_USERNAME=your-username
DREMIO1_PASSWORD=your-password
DREMIO1_TOKEN=your-token

DREMIO2_HOST=your-dremio2-host
DREMIO2_PORT=9047
DREMIO2_USERNAME=your-username
DREMIO2_PASSWORD=your-password
DREMIO2_TOKEN=your-token

# HDFS Configuration
HDFS_HOST=your-hdfs-host
HDFS_PORT=8020
HDFS_USER=your-hdfs-user
HDFS_PATH=/path/to/data

# Datadog Configuration (Optional)
DD_API_KEY=your-api-key
DD_APP_KEY=your-app-key
DD_ENABLED=true
```

## Project Structure

```
MetricMind/
├── app/                    # FastAPI application
│   └── main.py            # Main application file
├── scripts/               # Utility scripts
│   ├── generate_tpcds_data.py  # TPC-DS data generation
│   └── run_benchmark.sh   # Benchmark automation
├── jmeter/               # JMeter test plans
│   └── dremio_benchmark.jmx
├── config/               # Configuration files
├── data/                 # Data directory
└── requirements.txt      # Python dependencies
```

## Usage

1. Generate and upload TPC-DS data:
```bash
python scripts/generate_tpcds_data.py --scale-factor 1
```

2. Start the FastAPI application:
```bash
python -m MetricMind.app.main
```

3. Run benchmarks:
```bash
./scripts/run_benchmark.sh --scenario full --iterations 3
```

## API Endpoints

- `GET /health` - Health check endpoint
- `POST /query/simple` - Execute simple TPC-DS query
- `POST /query/complex` - Execute complex TPC-DS query
- `POST /compare/query` - Compare query results between Dremio instances

## Metrics Collected

- Query execution time
- Row count
- Payload size
- Result match percentage
- JMeter performance metrics

## Visualization

Metrics are automatically sent to Datadog (if enabled) and can be visualized using:
- Time series graphs
- Heat maps
- Comparison dashboards

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request 