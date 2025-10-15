# Binance MCP Server

MCP server providing Binance API access for building AI-powered cryptocurrency trading agents through the Model Context Protocol.

## Architecture

**CSV-First Data Design**

All tools return structured CSV files instead of raw JSON responses, encouraging systematic data analysis workflows:

- **Output**: Every tool saves results to CSV files in the server's `data/mcp-binance/` folder
- **Analysis**: CSV files are analyzed using the `py_eval` tool with pandas/numpy pre-loaded
- **Pattern**: Fetch data → Receive CSV path → Analyze with Python → Make decisions
- **Benefits**: Promotes data-driven reasoning, enables complex multi-step analysis, supports historical tracking

**Modular Design**

Each tool is implemented in a separate file (`backend/binance_tools/*.py`) with comprehensive parameter documentation, minimizing token usage and improving maintainability.

## Available Tools

### Market Data (Read-Only)
- `binance_get_ticker` - 24hr price change statistics
- `binance_get_orderbook` - Current bid/ask order book
- `binance_get_recent_trades` - Recent market trades
- `binance_get_price` - Latest price for symbol(s)
- `binance_get_book_ticker` - Best bid/ask prices
- `binance_get_avg_price` - Average price over time window

### Account Management (Read-Only)
- `binance_get_account` - Portfolio balances with USDT valuations
- `binance_get_open_orders` - Currently active orders
- `binance_spot_trade_history` - Executed trade history with P&L data

### Spot Trading
- `binance_spot_market_order` - Execute market buy/sell orders
- `binance_spot_limit_order` - Place limit orders (GTC/IOC/FOK)
- `binance_spot_oco_order` - One-Cancels-Other orders (take-profit + stop-loss)
- `binance_cancel_order` - Cancel individual or all orders

### Futures Trading
- `binance_set_futures_leverage` - Configure leverage for symbol
- `binance_manage_futures_positions` - Open/close/modify futures positions
- `binance_calculate_liquidation_risk` - Calculate liquidation prices and risk

### Analysis & Risk Management
- `binance_calculate_spot_pnl` - Profit/loss analysis with fee tracking
- `trading_notes` - Save and retrieve trading decisions/observations
- `py_eval` - Execute Python code with pandas/numpy for data analysis

## Typical Workflow

```python
# 1. Fetch account data
binance_get_account()
# Returns: "✓ Data saved to CSV\nFile: account_a1b2c3d4.csv..."

# 2. Analyze the CSV file with Python
py_eval("""
import pandas as pd
df = pd.read_csv('data/mcp-binance/account_a1b2c3d4.csv')

# Calculate portfolio metrics
total_value = df['value_usdt'].sum()
top_holdings = df.nlargest(5, 'value_usdt')[['asset', 'value_usdt']]

print(f"Total Portfolio: ${total_value:,.2f}")
print("\nTop 5 Holdings:")
print(top_holdings.to_string(index=False))
""")

# 3. Make data-driven trading decisions based on analysis
```

## Setup

### Requirements
- Docker and Docker Compose
- Binance API credentials (spot and/or futures enabled)

### Environment Configuration

Create `.env.local` file:

```bash
# Binance API Credentials
BINANCE-API-KEY=your_api_key_here
BINANCE-API-SECRET=your_api_secret_here

# MCP Configuration
MCP_NAME=binance
MCP_TOKENS=your_secure_token_here
MCP_REQUIRE_AUTH=true
PORT=8010

# Optional: Sentry error tracking
SENTRY_DSN=your_sentry_dsn
CONTAINER_NAME=mcp-binance-local
```

### Deployment

```bash
# Local development
./compose.local.sh

# Service runs on http://localhost:8010/binance/
```

## Security Notes

- **API Permissions**: Configure Binance API keys with appropriate restrictions
- **Read-Only Tools**: All market data and account info tools are read-only
- **Trading Tools**: Clearly marked in tool descriptions; require explicit action
- **Token Authentication**: Required for production use (MCP_REQUIRE_AUTH=true)
- **Risk Management**: Always analyze data before executing trades

## CSV Data Persistence

All CSV files are stored in `data/mcp-binance/` with unique identifiers, enabling:
- Historical portfolio tracking
- Performance analysis over time
- Audit trails for trading decisions
- Systematic backtesting workflows

## Tool Documentation

Each tool includes comprehensive inline documentation with:
- Detailed parameter descriptions and constraints
- Return value schemas with data types
- Use cases and examples
- Risk warnings for trading operations
- Python analysis suggestions

Run tools without parameters to see full documentation.

## License

MIT License - See LICENSE file for details.
