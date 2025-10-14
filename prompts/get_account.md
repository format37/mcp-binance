# Test Prompt for binance_get_account Tool

This prompt is designed to test the `binance_get_account` MCP tool and verify that the agent can successfully read and analyze the CSV output.

## Test Prompt

```
Please call the binance_get_account tool and tell me:
1. How many assets I have with non-zero balance
2. What is my largest holding by total amount (asset name and amount)
3. What is my smallest holding by total amount (asset name and amount)
```

## Expected Agent Behavior

The agent should:
1. Call the `binance_get_account()` tool
2. Receive a CSV file path in the response
3. Use the `py_eval` tool to read and analyze the CSV file
4. Extract the requested information from the data
5. Provide clear answers to all three questions

## Example py_eval Code

The agent might use code similar to this:

```python
import pandas as pd
import os

# Load the most recent account CSV
files = [f for f in os.listdir(CSV_PATH) if f.startswith('account_') and f.endswith('.csv')]
if files:
    latest_file = sorted(files)[-1]
    df = pd.read_csv(f'{CSV_PATH}/{latest_file}')

    print(f"Total assets with non-zero balance: {len(df)}")
    print(f"\nLargest holding: {df.iloc[0]['asset']} = {df.iloc[0]['total']}")
    print(f"Smallest holding: {df.iloc[-1]['asset']} = {df.iloc[-1]['total']}")
```

## Success Criteria

The test is successful if the agent:
- ✓ Successfully calls the binance_get_account tool
- ✓ Correctly reads the CSV file using py_eval
- ✓ Provides accurate answers to all three questions
- ✓ Shows understanding of the data structure (asset, free, locked, total columns)
