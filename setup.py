#!/usr/bin/env python3
"""
Llama Stream - Python package for LlamaSearch AI
"""

from setuptools import setup, find_packages

setup(
    name="llama_stream",
    version="0.1.0",
    description="**L**ightweight **L**atency-**A**ware **M**LX-**A**ccelerated Stream Processing",
    long_description="""# llama-stream

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
""",
    long_description_content_type="text/markdown",
    author="Nik Jois",
    author_email="nikjois@llamasearch.ai",
    url="https://github.com/llamasearchai/llama-stream",
    project_urls={
        "Documentation": "https://github.com/llamasearchai/llama-stream",
        "Bug Tracker": "https://github.com/llamasearchai/llama-stream/issues",
        "Source Code": "https://github.com/llamasearchai/llama-stream",
    },
    packages='src.llama_stream',
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
)
