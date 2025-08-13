# Baseball Query

Backend management for a personal baseball stats database.  
This library provides helpers for building SQL queries, fetching data asynchronously and processing complex baseball metrics.


## Usage

Instantiate `BaseballQueryClient` to create query builders and fetch results.

```python
from baseball_query import BaseballQueryClient
import asyncio

async def main():
    client = BaseballQueryClient()
    await client.initialize()

    # Build a totals query for a batter
    builder = await client.create_query(
        metrics=["hits", "home_runs"],
        player_type="batter"
    )
    builder.add_filters({"year": "2023"})
    builder.order_by("hits")

    # Fetch a pandas DataFrame with processed metrics
    df = await client.fetch_data(builder)
    print(df.head())

    await client.close()

asyncio.run(main())
```

