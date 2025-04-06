# MetricMind

MetricMind is a benchmarking tool for comparing Dremio instances using TPC-DS queries. It provides a comprehensive framework for generating test data, executing queries, and analyzing performance differences between Dremio deployments.

## Features

- **TPC-DS Data Generation**: Generate TPC-DS test data with configurable scale factors
- **Query Execution**: Execute TPC-DS queries against multiple Dremio instances
- **Performance Comparison**: Compare execution times, resource usage, and data consistency
- **HDFS Integration**: Upload generated data to HDFS and register as Dremio sources
- **REST API**: Expose functionality through a FastAPI-based REST API
- **Asynchronous Operations**: Efficient handling of long-running tasks
- **Detailed Metrics**: Comprehensive performance metrics and statistics
- **Graceful Cancellation**: Support for cancelling ongoing operations
- **Resource Monitoring**: Track memory and CPU usage during query execution

## Installation

### Prerequisites

- Python 3.8+
- Dremio instances
- HDFS cluster
- TPC-DS data generator (dsdgen)

### Setup

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/metricmind.git
   cd metricmind
   ```

2. Create a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

4. Configure the application:
   - Copy `.env.example` to `.env`
   - Update the configuration values in `.env`

## Usage

### Running the API

```
python -m metricmind.api.app
```

The API will be available at `http://localhost:8000`.

### API Endpoints

- `POST /api/v1/benchmark`: Run a benchmark comparison between two Dremio instances
- `POST /api/v1/generate-data`: Generate TPC-DS test data
- `GET /api/v1/health`: Health check endpoint

### Command Line

Generate TPC-DS data:
```
python -m metricmind.cli generate-data --scale 1.0
```

Run benchmarks:
```
python -m metricmind.cli benchmark --iterations 3
```

## Configuration

The application can be configured through environment variables or a `.env` file. See `.env.example` for available options.

## Project Structure

```
metricmind/
├── metricmind/
│   ├── api/
│   │   ├── __init__.py
│   │   ├── app.py
│   │   └── routes.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── data_generator.py
│   │   └── query_executor.py
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   └── logger.py
│   └── __init__.py
├── tests/
├── .env.example
├── .gitignore
├── README.md
├── requirements.txt
└── setup.py
```

## Development

### Running Tests

```
pytest
```

### Code Style

```
black metricmind
flake8 metricmind
mypy metricmind
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- TPC-DS benchmark suite
- Dremio
- FastAPI
- aiohttp 