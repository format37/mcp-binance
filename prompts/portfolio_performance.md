# Portfolio Performance Test

Analyze my portfolio performance over the last 30 days. Compare my actual trading results against a buy-and-hold benchmark. Show me the equity curves and key metrics.

## Expected Result
- An inline chart image showing actual trading vs benchmark portfolio performance
- Trades table CSV with all trades and historical prices attached
- Equity curves CSV with daily portfolio values (actual vs benchmark)
- Performance metrics CSV with returns, P/L, drawdowns, trade statistics
- A markdown performance report with detailed analysis

## How It Works
- Uses a fixed $3,000 initial capital baseline
- Both portfolios start with same allocation (33% BTC, 33% ETH, 34% USDT)
- Actual portfolio tracks your real spot trades (BTCUSDT, ETHUSDT)
- Benchmark rebalances to target weights on each trade date
- Smart initialization prevents negative holdings

## Verification Steps
1. Call the `binance_portfolio_performance` tool with `days=30`
2. Verify the response includes an inline chart image preview
3. Verify the response includes 3 CSV files:
   - `portfolio_trades_*.csv` - Trades table with historical prices
   - `portfolio_equity_*.csv` - Daily equity curves
   - `portfolio_metrics_*.csv` - Performance metrics
4. Verify the markdown report shows:
   - Initial allocation details
   - Actual portfolio performance (return %, P/L, max drawdown)
   - Benchmark performance (return %, P/L, max drawdown)
   - Outperformance analysis
   - Trade statistics and recent trades table
5. If MCP_PUBLIC_BASE_URL is configured, verify the download URL is accessible

## Example Queries
- `binance_portfolio_performance(days=30)` - Last 30 days analysis
- `binance_portfolio_performance(days=7)` - Last week analysis
- `binance_portfolio_performance(days=60)` - Last 2 months analysis
