# llama-stream

[![PyPI version](https://img.shields.io/pypi/v/llama_stream.svg)](https://pypi.org/project/llama_stream/)
[![License](https://img.shields.io/github/license/llamasearchai/llama-stream)](https://github.com/llamasearchai/llama-stream/blob/main/LICENSE)
[![Python Version](https://img.shields.io/pypi/pyversions/llama_stream.svg)](https://pypi.org/project/llama_stream/)
[![CI Status](https://github.com/llamasearchai/llama-stream/actions/workflows/llamasearchai_ci.yml/badge.svg)](https://github.com/llamasearchai/llama-stream/actions/workflows/llamasearchai_ci.yml)

**Llama Stream (llama-stream)** is a toolkit for real-time data stream processing within the LlamaSearch AI ecosystem. It enables the ingestion, transformation, analysis, and routing of continuous data streams from various sources.

## Key Features

- **Stream Processing Engine:** Core logic for handling data streams (`main.py`, `core.py`).
- **Data Ingestion:** Connectors for reading data from sources like Kafka, Kinesis, WebSockets, or files.
- **Data Transformation:** Tools for filtering, mapping, aggregating, or enriching data in real-time.
- **Real-time Analysis (Potential):** Could integrate AI models for anomaly detection, classification, or feature extraction on streaming data.
- **Data Routing/Output:** Connectors for sending processed data to destinations like databases, message queues, or other services.
- **Configurable:** Allows defining stream sources, processing steps, transformations, and output sinks (`config.py`).

## Installation

```bash
pip install llama-stream
# Or install directly from GitHub for the latest version:
# pip install git+https://github.com/llamasearchai/llama-stream.git
```

## Usage

*(Usage examples demonstrating how to define and run stream processing pipelines will be added here.)*

```python
# Placeholder for Python client usage
# from llama_stream import StreamProcessor, StreamConfig, Source, Sink, Transform

# config = StreamConfig.load("stream_config.yaml")
# processor = StreamProcessor(config)

# # Define pipeline components (potentially loaded from config)
# kafka_source = Source(type="kafka", topic="raw_events", brokers="...")
# enrich_transform = Transform(func=lambda data: add_user_info(data))
# filter_transform = Transform(func=lambda data: data['value'] > 100)
# db_sink = Sink(type="database", table="processed_events", connection_uri="...")

# # Build and run the pipeline
# processor.add_source(kafka_source)
# processor.add_transform(enrich_transform)
# processor.add_transform(filter_transform)
# processor.add_sink(db_sink)

# processor.start()
```

## Architecture Overview

```mermaid
graph TD
    A[Data Source (Kafka, File, API)] --> B{Ingestion Interface};
    B --> C{Stream Processing Core (main.py, core.py)};

    subgraph Processing Steps
        D[Transform 1 (e.g., Filter)]
        E[Transform 2 (e.g., Enrich)]
        F[Transform 3 (e.g., Aggregate)]
        G[AI Analysis (Optional)]
    end

    C -- Pipes Data Through --> D;
    D --> E;
    E --> F;
    F --> G;
    G --> H{Output Interface};
    H --> I[Data Sink (DB, Kafka, File)];

    J[Configuration (config.py)] -- Defines Pipeline --> C;
    J -- Configures --> B; J -- Configures --> D; J -- Configures --> E;
    J -- Configures --> F; J -- Configures --> G; J -- Configures --> H;

    style C fill:#f9f,stroke:#333,stroke-width:2px
    style I fill:#ccf,stroke:#333,stroke-width:1px
```

1.  **Ingestion:** Data enters the system from configured sources.
2.  **Core Processing:** The main engine manages the flow of data through the defined pipeline.
3.  **Transformations:** Data passes through a series of configurable transformation steps (filtering, enrichment, aggregation, AI analysis).
4.  **Output:** Processed data is sent to configured output sinks.
5.  **Configuration:** Defines the sources, sinks, and sequence of transformations in the processing pipeline.

## Configuration

*(Details on configuring data sources (Kafka topics, file paths, API endpoints), transformation steps, AI model integration, output sinks (database connections, Kafka topics), processing parallelism, error handling, etc., will be added here.)*

## Development

### Setup

```bash
# Clone the repository
git clone https://github.com/llamasearchai/llama-stream.git
cd llama-stream

# Install in editable mode with development dependencies
pip install -e ".[dev]"
```

### Testing

```bash
pytest tests/
```

### Contributing

Contributions are welcome! Please refer to [CONTRIBUTING.md](CONTRIBUTING.md) and submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---
*Developed by lalamasearhc.*
