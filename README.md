# llama-stream

**L**ightweight **L**atency-**A**ware **M**LX-**A**ccelerated Stream Processing

## Installation

```bash
pip install -e .
```

## Usage

```python
from llama_stream import LlamaStreamClient

# Initialize the client
client = LlamaStreamClient(api_key="your-api-key")
result = client.query("your query")
print(result)
```

## Features

- Fast and efficient
- Easy to use API
- Comprehensive documentation
- Asynchronous support

## Development

### Setup

```bash
# Clone the repository
git clone https://github.com/nikjois/llama-stream.git
cd llama-stream

# Install development dependencies
pip install -e ".[dev]"
```

### Testing

```bash
pytest
```

## License

MIT

## Author

Nik Jois (nikjois@llamasearch.ai)
