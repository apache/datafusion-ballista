# PyBallista

Minimal Python client for Ballista.

The goal of this project is to provide a way to run SQL against a Ballista cluster from Python and collect results.

The goal is not to provide the full DataFrame API. This could be added later if there is sufficient interest
from maintainers, and should just be a thin wrapper around the DataFusion Python bindings.

This project is versioned and released independently from the main Ballista project and is intentionally not 
part of the default Cargo workspace so that it doesn't cause overhead for maintainers of the main Ballista codebase.

## Example Usage

```python
from pyballista import SessionContext

# Connect to Ballista scheduler
ctx = SessionContext("localhost", 50050)

# Execute query
results = ctx.sql("SELECT 1")
```

## Building

```shell
python3 -m venv venv
source venv/bin/activate
maturin develop
```