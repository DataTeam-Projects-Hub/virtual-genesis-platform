# Virtual Genesis Price Analyzer

A Python script that analyzes Genesis token prices in USDC by fetching real-time Virtual/USDC prices from Uniswap V3 pool and processing swap transactions from MongoDB.

## Features

- **Real-time Price Fetching**: Gets Virtual/USDC prices from Uniswap V3 pool on Base network
- **Block Fallback Mechanism**: Automatically tries nearby blocks if the exact block fails
- **MongoDB Integration**: Processes swap transactions from multiple collections
- **CSV Export**: Saves analysis results to `genesis_prices_analysis.csv`
- **Progress Tracking**: Shows detailed progress and statistics
- **Configurable Settings**: Easy to modify retry limits and output verbosity

## Configuration

Edit the configuration section in `virtual_genesis_price_analyzer.py`:

```python
# Analysis Settings
MAX_BLOCK_RETRIES = 5  # Maximum blocks to try when fetching price
VERBOSE_OUTPUT = False  # Set to True for detailed output
```

## How It Works

1. **Connects to MongoDB** and finds all collections ending with `_swap`
2. **For each transaction**:
   - Fetches Virtual/USDC price from Uniswap V3 pool at the transaction's block
   - If the exact block fails, tries nearby blocks (up to `MAX_BLOCK_RETRIES`)
   - Calculates Genesis token prices using Virtual as intermediary
   - Converts to USDC prices
3. **Exports results** to CSV with columns:
   - `collection_name`: MongoDB collection name
   - `block_number`: Blockchain block number
   - `tx_hash`: Transaction hash
   - `genesis_token_name`: Name of the Genesis token
   - `virtual_usdc_price`: Virtual token price in USDC
   - `genesis_virtual_price`: Genesis token price in Virtual
   - `genesis_usdc_price`: Genesis token price in USDC

## Block Skipping Solution

The script now handles block skipping issues by:

- **Automatic Retry**: If the exact block fails, it tries the next 5 blocks
- **Graceful Fallback**: Uses the first successful block found
- **Progress Tracking**: Shows how many blocks were skipped
- **Success Rate**: Displays overall success percentage

## Output Example

```
🚀 Virtual Genesis Price Analyzer
==================================================
📡 MongoDB: virtualgenesisdata.wbeqoft.mongodb.net/
🔗 Base RPC: https://base-mainnet.g.alchemy.com/v2/***
🏊 VIRTUAL/USDC Pool: 0x529d2863a1521d0b57db028168fde2e97120017c
⚙️ Max block retries: 5
🔊 Verbose mode: OFF
==================================================
📂 Found 2 swap collections
🔄 Starting analysis...

📂 Processing aikat_swap (105 transactions)
✅ Completed aikat_swap
📂 Processing jarvis_swap (101 transactions)
✅ Completed jarvis_swap

==================================================
📊 ANALYSIS SUMMARY
==================================================
📂 Collections processed: 2/2
🔄 Total transactions: 206
✅ Successful transactions: 206
⚠️ Skipped blocks: 0
📈 Success rate: 100.0%
💾 Results saved to: genesis_prices_analysis.csv
==================================================
```

## Requirements

- Python 3.9+
- MongoDB connection
- Base network RPC access
- Required packages (see `requirements.txt`)

## Installation

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Configure MongoDB and RPC settings
4. Run: `python virtual_genesis_price_analyzer.py`

## Troubleshooting

- **High skipped blocks**: Increase `MAX_BLOCK_RETRIES`
- **Verbose debugging**: Set `VERBOSE_OUTPUT = True`
- **Connection issues**: Check MongoDB URI and RPC URL
- **Price calculation errors**: Verify pool addresses and token decimals

