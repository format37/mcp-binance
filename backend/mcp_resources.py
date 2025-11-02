import logging

logger = logging.getLogger(__name__)


def register_mcp_resources(local_mcp_instance, safe_name):
    """Register MCP resources (documentation, etc.)"""

    @local_mcp_instance.resource(
        f"{safe_name}://portfolio-report-guide",
        name="Portfolio Comparison Report Guide",
        description="Guide for generating event-based portfolio comparison reports with interactive analysis",
        mime_type="text/markdown"
    )
    def get_portfolio_report_guide() -> str:
        """Portfolio comparison report generation guide with py_eval examples."""
        return """# Portfolio Comparison Report Guide

## Overview

Generate comprehensive portfolio performance reports comparing actual trading vs buy-and-hold strategy using event-based analysis with real P2P and deposit history.

## Quick Start - Generate Full Report

```python
# Step 1: Run the portfolio comparison script
# This generates CSV files with equity curves and metrics
import subprocess
result = subprocess.run(
    ['python3', '/path/to/examples/report/portfolio_comparison_v2.py'],
    capture_output=True,
    text=True,
    cwd='/path/to/project'
)
print(result.stdout)
```

## Using py_eval for Analysis

### 1. Load and Analyze Report Data

```python
import os

# Find report files
files = [f for f in os.listdir(CSV_PATH) if f.endswith('.csv')]
equity_files = [f for f in files if 'portfolio_comparison_v2' in f]
events_files = [f for f in files if 'cash_flow_events' in f]

if equity_files:
    # Load equity curves
    equity_df = pd.read_csv(f'{CSV_PATH}/{equity_files[-1]}')
    equity_df['date'] = pd.to_datetime(equity_df['date'])

    print("=== PORTFOLIO COMPARISON SUMMARY ===")
    print(f"Analysis period: {equity_df['date'].min().date()} to {equity_df['date'].max().date()}")
    print(f"Data points: {len(equity_df)}")

    # Calculate returns
    actual_start = equity_df['actual'].iloc[0]
    actual_end = equity_df['actual'].iloc[-1]
    hypo_start = equity_df['hypothetical'].iloc[0]
    hypo_end = equity_df['hypothetical'].iloc[-1]

    actual_return = ((actual_end - actual_start) / actual_start) * 100
    hypo_return = ((hypo_end - hypo_start) / hypo_start) * 100

    print(f"\nActual Trading:")
    print(f"  Start: ${actual_start:,.2f}")
    print(f"  End: ${actual_end:,.2f}")
    print(f"  Return: {actual_return:+.2f}%")

    print(f"\nBuy-and-Hold:")
    print(f"  Start: ${hypo_start:,.2f}")
    print(f"  End: ${hypo_end:,.2f}")
    print(f"  Return: {hypo_return:+.2f}%")

    print(f"\nOutperformance: {actual_return - hypo_return:+.2f}%")
```

### 2. Generate Concise Table for Client (Row-Limited)

⚠️ **Important**: Client responses have length limits. Return only essential rows.

```python
import os

files = [f for f in os.listdir(CSV_PATH) if f.endswith('.csv')]
equity_files = [f for f in files if 'portfolio_comparison_v2' in f]

if equity_files:
    equity_df = pd.read_csv(f'{CSV_PATH}/{equity_files[-1]}')
    equity_df['date'] = pd.to_datetime(equity_df['date'])

    # Strategy 1: Sample evenly across time period (recommended)
    # Take every Nth row to get ~20-30 data points
    total_rows = len(equity_df)
    target_points = 25  # Adjust based on client limit
    step = max(1, total_rows // target_points)

    sampled_df = equity_df.iloc[::step].copy()

    # Always include the last row (most recent)
    if sampled_df.iloc[-1]['date'] != equity_df.iloc[-1]['date']:
        sampled_df = pd.concat([sampled_df, equity_df.iloc[[-1]]])

    # Format for client-side plotting
    plot_table = sampled_df[['date', 'actual', 'hypothetical']].copy()
    plot_table['date'] = plot_table['date'].dt.strftime('%Y-%m-%d')
    plot_table = plot_table.rename(columns={
        'date': 'Date',
        'actual': 'Actual Trading ($)',
        'hypothetical': 'Buy-and-Hold ($)'
    })

    print("=== EQUITY CURVES (SAMPLED) ===")
    print(f"Showing {len(plot_table)} of {total_rows} data points")
    print(plot_table.to_string(index=False))

    # Alternative Strategy 2: Weekly/Daily aggregation
    # For longer periods, aggregate to weekly
    if total_rows > 50:
        equity_df['week'] = equity_df['date'].dt.to_period('W')
        weekly_df = equity_df.groupby('week').agg({
            'date': 'last',
            'actual': 'last',
            'hypothetical': 'last'
        }).reset_index(drop=True)

        print("\n=== WEEKLY EQUITY CURVES ===")
        print(f"Showing {len(weekly_df)} weekly points")
```

### 3. Generate Concise Text Review

```python
import os
import json

# Load metrics JSON
files = [f for f in os.listdir(CSV_PATH) if f.endswith('.json')]
metrics_files = [f for f in files if 'portfolio_comparison_v2_metrics' in f]

if metrics_files:
    with open(f'{CSV_PATH}/{metrics_files[-1]}', 'r') as f:
        metrics = json.load(f)

    # Generate concise review
    total_invested = metrics['total_invested']
    actual = metrics['actual']
    hypothetical = metrics['hypothetical']

    print("="*70)
    print("PORTFOLIO PERFORMANCE REVIEW")
    print("="*70)

    # Investment summary
    print(f"\n💰 CAPITAL INVESTED: ${total_invested:,.2f}")

    # Performance comparison
    print(f"\n📊 ACTUAL TRADING PERFORMANCE:")
    print(f"   Final Value:    ${actual['final']:,.2f}")
    print(f"   Return:         {actual['return_pct']:+.2f}%")
    print(f"   Profit/Loss:    ${actual['profit_loss']:+,.2f}")
    print(f"   Max Drawdown:   {actual['max_drawdown_pct']:.2f}%")

    print(f"\n📈 BUY-AND-HOLD BENCHMARK (33% BTC, 33% ETH, 33% USDT):")
    print(f"   Final Value:    ${hypothetical['final']:,.2f}")
    print(f"   Return:         {hypothetical['return_pct']:+.2f}%")
    print(f"   Profit/Loss:    ${hypothetical['profit_loss']:+,.2f}")
    print(f"   Max Drawdown:   {hypothetical['max_drawdown_pct']:.2f}%")

    # Outperformance analysis
    outperformance = actual['return_pct'] - hypothetical['return_pct']
    print(f"\n{'='*70}")
    print(f"📌 OUTPERFORMANCE: {outperformance:+.2f}%")
    print(f"{'='*70}")

    if outperformance > 0:
        print("✅ Your trading strategy OUTPERFORMED the buy-and-hold benchmark!")
        edge = abs(outperformance)
        print(f"   You gained an additional {edge:.2f}% through active trading.")
    else:
        print("❌ Your trading strategy UNDERPERFORMED the buy-and-hold benchmark.")
        loss = abs(outperformance)
        print(f"   You would have gained {loss:.2f}% more by simply holding.")

    # Risk-adjusted analysis
    actual_risk_adj = actual['return_pct'] / abs(actual['max_drawdown_pct']) if actual['max_drawdown_pct'] != 0 else 0
    hypo_risk_adj = hypothetical['return_pct'] / abs(hypothetical['max_drawdown_pct']) if hypothetical['max_drawdown_pct'] != 0 else 0

    print(f"\n⚖️  RISK-ADJUSTED RETURNS:")
    print(f"   Actual:        {actual_risk_adj:.3f}")
    print(f"   Buy-and-Hold:  {hypo_risk_adj:.3f}")

    if actual_risk_adj > hypo_risk_adj:
        print("   ✅ Better risk-adjusted performance")
    else:
        print("   ❌ Worse risk-adjusted performance")

    # Strategic insights
    print(f"\n💡 INSIGHTS:")

    # Volatility comparison
    if abs(actual['max_drawdown_pct']) < abs(hypothetical['max_drawdown_pct']):
        print("   • Lower volatility: Your strategy is more stable")
    else:
        print("   • Higher volatility: Your strategy experienced larger swings")

    # Absolute gains
    if actual['profit_loss'] > 0:
        print(f"   • Profitable: Made ${actual['profit_loss']:,.2f} in absolute gains")
    else:
        print(f"   • Loss: Lost ${abs(actual['profit_loss']):,.2f} in absolute terms")

    # Recommendation
    print(f"\n🎯 RECOMMENDATION:")
    if outperformance > 2:
        print("   Strong outperformance - continue current strategy with monitoring")
    elif outperformance > 0:
        print("   Modest outperformance - strategy is working, consider optimization")
    elif outperformance > -2:
        print("   Slight underperformance - review trading decisions")
    else:
        print("   Significant underperformance - consider reverting to buy-and-hold")
```

### 4. Analyze Cash Flow Events

```python
import os

files = [f for f in os.listdir(CSV_PATH) if f.endswith('.csv')]
events_files = [f for f in files if 'cash_flow_events' in f]

if events_files:
    events_df = pd.read_csv(f'{CSV_PATH}/{events_files[-1]}')
    events_df['timestamp'] = pd.to_datetime(events_df['timestamp'])

    print("=== CASH FLOW ANALYSIS ===")

    # Event breakdown
    event_counts = events_df['type'].value_counts()
    print(f"\nEvent breakdown:")
    for event_type, count in event_counts.items():
        print(f"  {event_type}: {count} events")

    # Capital flow summary
    deposits = events_df[events_df['usd_value'] > 0]['usd_value'].sum()
    withdrawals = abs(events_df[events_df['usd_value'] < 0]['usd_value'].sum())
    net_flow = deposits - withdrawals

    print(f"\nCapital flows:")
    print(f"  Total deposits:    ${deposits:,.2f}")
    print(f"  Total withdrawals: ${withdrawals:,.2f}")
    print(f"  Net capital flow:  ${net_flow:,.2f}")

    # Recent events (limited for client)
    print(f"\nRecent events (last 5):")
    recent = events_df.nlargest(5, 'timestamp')
    for _, event in recent.iterrows():
        sign = "+" if event['usd_value'] > 0 else ""
        print(f"  {event['timestamp'].strftime('%Y-%m-%d')}: {event['type']:12s} {sign}${event['usd_value']:>10,.2f}")
```

### 5. Complete Analysis Workflow

```python
import os
import json

def analyze_portfolio_report():
    \"\"\"Complete portfolio analysis with concise output\"\"\"

    # 1. Load all report files
    files = os.listdir(CSV_PATH)
    equity_file = next((f for f in files if 'portfolio_comparison_v2.csv' in f), None)
    metrics_file = next((f for f in files if 'portfolio_comparison_v2_metrics.json' in f), None)
    events_file = next((f for f in files if 'cash_flow_events.csv' in f), None)

    if not all([equity_file, metrics_file, events_file]):
        print("❌ Error: Report files not found. Run portfolio_comparison_v2.py first.")
        return

    # 2. Load data
    equity_df = pd.read_csv(f'{CSV_PATH}/{equity_file}')
    equity_df['date'] = pd.to_datetime(equity_df['date'])

    with open(f'{CSV_PATH}/{metrics_file}', 'r') as f:
        metrics = json.load(f)

    events_df = pd.read_csv(f'{CSV_PATH}/{events_file}')

    # 3. Generate concise review
    print("="*70)
    print("PORTFOLIO PERFORMANCE REPORT")
    print("="*70)
    print(f"\nPeriod: {equity_df['date'].min().date()} to {equity_df['date'].max().date()}")
    print(f"Capital Invested: ${metrics['total_invested']:,.2f}")
    print(f"Events Tracked: {len(events_df)}")

    # Performance summary
    actual = metrics['actual']
    hypo = metrics['hypothetical']
    outperf = actual['return_pct'] - hypo['return_pct']

    print(f"\n{'Strategy':<20} {'Return':>10} {'Final Value':>15} {'Max DD':>10}")
    print("-" * 70)
    print(f"{'Actual Trading':<20} {actual['return_pct']:>9.2f}% ${actual['final']:>13,.2f} {actual['max_drawdown_pct']:>9.2f}%")
    print(f"{'Buy-and-Hold':<20} {hypo['return_pct']:>9.2f}% ${hypo['final']:>13,.2f} {hypo['max_drawdown_pct']:>9.2f}%")
    print(f"{'Difference':<20} {outperf:>9.2f}% ${actual['final']-hypo['final']:>13,.2f}")

    # Verdict
    print(f"\n{'='*70}")
    if outperf > 0:
        print(f"✅ OUTPERFORMED by {outperf:.2f}%")
    else:
        print(f"❌ UNDERPERFORMED by {abs(outperf):.2f}%")
    print(f"{'='*70}")

    # 4. Return sampled data for plotting (CLIENT LIMIT AWARE)
    total_rows = len(equity_df)
    step = max(1, total_rows // 25)  # Max 25 points
    sampled = equity_df.iloc[::step][['date', 'actual', 'hypothetical']].copy()

    # Ensure last point included
    if sampled.iloc[-1]['date'] != equity_df.iloc[-1]['date']:
        sampled = pd.concat([sampled, equity_df.iloc[[-1]][['date', 'actual', 'hypothetical']]])

    sampled['date'] = sampled['date'].dt.strftime('%Y-%m-%d')

    print(f"\n=== EQUITY CURVE DATA ({len(sampled)} points) ===")
    print(sampled.to_string(index=False, max_rows=30))

# Run the analysis
analyze_portfolio_report()
```

## CLI Usage (Alternative)

For interactive exploration:

```bash
# Default 30-day analysis
python3 examples/report/portfolio_comparison_v2.py

# Interactive mode with plot
python3 examples/report/portfolio_comparison_v2.py --interactive

# Custom period
python3 examples/report/portfolio_comparison_v2.py --days 60 -i

# Different time granularity
python3 examples/report/portfolio_comparison_v2.py --interval 5m --days 7 -i
```

## Best Practices

1. **Row Limits**: Always sample or aggregate data to stay within client response limits (20-30 rows recommended)
2. **Date Formatting**: Convert dates to strings for clean table display
3. **Concise Output**: Focus on key metrics rather than verbose explanations
4. **Error Handling**: Check if report files exist before loading
5. **Metrics JSON**: Use the metrics JSON file for precise calculations rather than recalculating
6. **Event Analysis**: Cash flow events CSV provides the complete capitalization history

## Key Files Generated

| File | Purpose |
|------|---------|
| `portfolio_comparison_v2.csv` | Daily equity curves (actual & buy-and-hold) |
| `portfolio_comparison_v2_metrics.json` | Performance metrics and returns |
| `cash_flow_events.csv` | All P2P and deposit events with USD values |
| `portfolio_comparison_v2.png` | Visualization chart |

## Response Length Management

To avoid hitting client response limits:

- **Sampling**: `df.iloc[::step]` - take every Nth row
- **Aggregation**: Group by week/month for long periods
- **Limiting**: Use `.head(N)` or `.tail(N)` for specific rows
- **Target**: Aim for 20-30 rows maximum in returned tables
"""

    @local_mcp_instance.resource(
        f"{safe_name}://documentation",
        name="Polygon API Documentation",
        description="Documentation for Polygon.io financial market data API",
        mime_type="text/markdown"
    )
    def get_documentation_resource() -> str:
        """Expose Polygon API documentation as an MCP resource."""
        return """# Polygon API Documentation

## Available Tools

### polygon_news
Fetches market news from Polygon.io financial news aggregator.

**Parameters:**
- `start_date` (optional): Start date in 'YYYY-MM-DD' format
- `end_date` (optional): End date in 'YYYY-MM-DD' format
- `save_csv` (optional): If True, saves data to CSV file

**Returns:** Formatted table with datetime and topic columns, or CSV filename if save_csv=True.

### polygon_ticker_details
Fetch comprehensive ticker details including company info, market cap, sector, etc.

**Parameters:**
- `tickers` (required): List of ticker symbols (e.g., ['AAPL', 'MSFT'])
- `save_csv` (optional): If True, saves data to CSV file and returns filename

**Returns:** Success message with CSV filename or summary information.

### polygon_price_data
Fetch historical price data for tickers.

**Parameters:**
- `tickers` (required): List of ticker symbols
- `from_date` (optional): Start date in YYYY-MM-DD format (defaults to 30 days ago)
- `to_date` (optional): End date in YYYY-MM-DD format (defaults to today)
- `timespan` (optional): Time span (day, week, month, quarter, year)
- `multiplier` (optional): Size of the time window
- `save_csv` (optional): If True, saves data to CSV file and returns filename

**Returns:** Success message with CSV filename or summary information.

### polygon_price_metrics
Calculate price-based metrics for tickers (volatility, returns, etc.).

**Parameters:**
- `tickers` (required): List of ticker symbols
- `from_date` (optional): Start date in YYYY-MM-DD format (defaults to 30 days ago)
- `to_date` (optional): End date in YYYY-MM-DD format (defaults to today)
- `save_csv` (optional): If True, saves data to CSV file and returns filename

**Returns:** Success message with CSV filename or summary information.

---

## py_eval Tool - Python Data Analysis

Execute Python code with pandas/numpy pre-loaded and access to CSV folder.

**Parameters:**
- `code` (required): Python code to execute
- `timeout_sec` (optional): Execution timeout in seconds (default: 5.0)

**⚠️ IMPORTANT NOTES:**
- **Variable Persistence**: Each py_eval call starts fresh - variables do NOT persist between calls
- **Reload Data**: You must reload CSV files in each py_eval call that needs them
- **Temporary Files**: You can save temporary files within a py_eval session and read them back in the same call
- **Libraries**: pandas (pd), numpy (np) are pre-loaded. Standard library modules available.

### Available Variables
- `pd`: pandas library (version 2.2.3+)
- `np`: numpy library (version 1.26.4+)
- `CSV_PATH`: string path to `/work/csv/` folder containing all CSV files

### CSV File Types and Structures

#### 1. Price Data Files (`price_data_*.csv`)
**Columns:**
- `ticker` (str): Stock ticker symbol
- `timestamp` (str): Trading session timestamp 'YYYY-MM-DD HH:MM:SS'
- `open`, `high`, `low`, `close` (float): OHLC prices
- `volume` (float): Trading volume
- `vwap` (float): Volume Weighted Average Price
- `transactions` (int): Number of transactions
- `date` (str): Trading date 'YYYY-MM-DD'

#### 2. Ticker Details Files (`ticker_details_*.csv`)
**Key Columns:**
- `ticker` (str): Stock ticker symbol
- `name` (str): Company full name
- `market_cap` (float): Market capitalization in USD
- `total_employees` (int): Number of employees
- `description` (str): Company description
- `homepage_url` (str): Company website
- `sic_code` (int): Standard Industrial Classification
- `sic_description` (str): Industry sector description
- Plus 20+ additional company details columns

#### 3. Price Metrics Files (`price_metrics_*.csv`)
**Columns:**
- `ticker` (str): Stock ticker symbol
- `start_date`, `end_date` (str): Analysis period 'YYYY-MM-DD'
- `trading_days` (int): Number of trading days
- `start_price`, `end_price` (float): Period start/end prices
- `high_price`, `low_price` (float): Period high/low prices
- `total_return` (float): Total return as decimal (0.12 = 12%)
- `volatility` (float): Price volatility (std dev of daily returns)
- `avg_daily_volume`, `total_volume` (float): Volume metrics
- `price_range_ratio` (float): (High-Low)/Low ratio

#### 4. News Files (`news_*.csv`)
**Columns:**
- `datetime` (str): Publication date/time 'YYYY-MM-DD HH:MM:SS'
- `topic` (str): News headline/title

---

## Python Analysis Examples

### 1. CSV File Discovery and Loading

```python
import os

# List all CSV files
print("=== CSV FILES AVAILABLE ===")
files = [f for f in os.listdir(CSV_PATH) if f.endswith('.csv')]
print(f"Found {len(files)} CSV files:")

# Group by type
price_data_files = [f for f in files if 'price_data_' in f]
ticker_files = [f for f in files if 'ticker_details_' in f]
metrics_files = [f for f in files if 'price_metrics_' in f]
news_files = [f for f in files if 'news_' in f]

print(f"Price data: {len(price_data_files)} files")
print(f"Ticker details: {len(ticker_files)} files")
print(f"Price metrics: {len(metrics_files)} files")
print(f"News: {len(news_files)} files")

# Load the most recent files (if they exist)
if price_data_files:
    price_df = pd.read_csv(f'{CSV_PATH}/{price_data_files[-1]}')
    print(f"\nLoaded price data: {price_df.shape} - {list(price_df.columns)}")

if ticker_files:
    ticker_df = pd.read_csv(f'{CSV_PATH}/{ticker_files[-1]}')
    print(f"Loaded ticker details: {ticker_df.shape}")
```

### 2. Portfolio Performance Analysis

```python
# Reload data (required in each py_eval call)
import os
files = [f for f in os.listdir(CSV_PATH) if f.endswith('.csv')]
metrics_files = [f for f in files if 'price_metrics_' in f]
ticker_files = [f for f in files if 'ticker_details_' in f]

if metrics_files and ticker_files:
    # Load latest files
    metrics_df = pd.read_csv(f'{CSV_PATH}/{metrics_files[-1]}')
    ticker_df = pd.read_csv(f'{CSV_PATH}/{ticker_files[-1]}')

    # Risk-adjusted performance analysis
    metrics_df['risk_adj_return'] = metrics_df['total_return'] / metrics_df['volatility']

    print("=== TOP PERFORMERS (Risk-Adjusted) ===")
    top_performers = metrics_df.nlargest(5, 'risk_adj_return')
    for _, stock in top_performers.iterrows():
        print(f"{stock['ticker']}: Return={stock['total_return']*100:.1f}%, "
              f"Vol={stock['volatility']*100:.1f}%, Risk-Adj={stock['risk_adj_return']:.2f}")

    # Market cap analysis
    combined = pd.merge(metrics_df, ticker_df[['ticker', 'market_cap']], on='ticker', how='left')
    combined['market_cap_billions'] = combined['market_cap'] / 1e9

    print("\n=== LARGE CAP PERFORMANCE ===")
    large_caps = combined[combined['market_cap_billions'] > 1000]
    large_caps_sorted = large_caps.sort_values('total_return', ascending=False)
    print(large_caps_sorted[['ticker', 'market_cap_billions', 'total_return']].round(3))
```

### 3. Technical Analysis with Price Data

```python
# Load price data for technical analysis
files = [f for f in os.listdir(CSV_PATH) if f.endswith('.csv')]
price_files = [f for f in files if 'price_data_' in f]

if price_files:
    df = pd.read_csv(f'{CSV_PATH}/{price_files[-1]}')

    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'])

    print("=== TECHNICAL ANALYSIS ===")
    for ticker in df['ticker'].unique():
        ticker_data = df[df['ticker'] == ticker].sort_values('date')

        # Calculate moving averages
        ticker_data['sma_5'] = ticker_data['close'].rolling(window=5).mean()
        ticker_data['sma_20'] = ticker_data['close'].rolling(window=20).mean()

        # Calculate daily returns
        ticker_data['daily_return'] = ticker_data['close'].pct_change()

        # Recent metrics
        recent_return = ticker_data['daily_return'].tail(5).mean() * 100
        current_price = ticker_data['close'].iloc[-1]
        sma5 = ticker_data['sma_5'].iloc[-1]
        sma20 = ticker_data['sma_20'].iloc[-1]

        print(f"{ticker}: Price=${current_price:.2f}, 5-day avg return={recent_return:.2f}%")
        if sma5 > sma20:
            print(f"  ↗️ Bullish: SMA5 (${sma5:.2f}) > SMA20 (${sma20:.2f})")
        else:
            print(f"  ↘️ Bearish: SMA5 (${sma5:.2f}) < SMA20 (${sma20:.2f})")
```

### 4. News Sentiment and Timeline Analysis

```python
# Load and analyze news data
files = [f for f in os.listdir(CSV_PATH) if f.endswith('.csv')]
news_files = [f for f in files if 'news_' in f]

if news_files:
    news_df = pd.read_csv(f'{CSV_PATH}/{news_files[-1]}')
    news_df['datetime'] = pd.to_datetime(news_df['datetime'])

    print("=== NEWS ANALYSIS ===")
    print(f"Total news articles: {len(news_df)}")

    # Daily news volume
    news_df['date'] = news_df['datetime'].dt.date
    daily_counts = news_df.groupby('date').size().sort_values(ascending=False)
    print(f"\nBusiest news days:")
    print(daily_counts.head())

    # Keyword analysis
    keywords = ['Fed', 'interest', 'earnings', 'AI', 'tech', 'market']
    print(f"\nKeyword frequency:")
    for keyword in keywords:
        count = news_df['topic'].str.contains(keyword, case=False, na=False).sum()
        print(f"  {keyword}: {count} mentions")

    # Recent headlines
    print(f"\nMost recent headlines:")
    recent_news = news_df.nlargest(5, 'datetime')
    for _, article in recent_news.iterrows():
        print(f"  {article['datetime'].strftime('%m/%d %H:%M')}: {article['topic'][:80]}...")
```

### 5. Saving and Loading Temporary Analysis Files

```python
# Create combined analysis and save as temp file for later use
files = [f for f in os.listdir(CSV_PATH) if f.endswith('.csv')]
metrics_files = [f for f in files if 'price_metrics_' in f]
ticker_files = [f for f in files if 'ticker_details_' in f]

if metrics_files and ticker_files:
    metrics_df = pd.read_csv(f'{CSV_PATH}/{metrics_files[-1]}')
    ticker_df = pd.read_csv(f'{CSV_PATH}/{ticker_files[-1]}')

    # Create comprehensive analysis
    analysis = pd.merge(metrics_df,
                       ticker_df[['ticker', 'name', 'market_cap', 'total_employees', 'sic_description']],
                       on='ticker', how='left')

    # Add calculated fields
    analysis['market_cap_billions'] = analysis['market_cap'] / 1e9
    analysis['risk_adj_return'] = analysis['total_return'] / analysis['volatility']
    analysis['return_pct'] = analysis['total_return'] * 100
    analysis['employees_per_billion_cap'] = analysis['total_employees'] / analysis['market_cap_billions']

    # Save temporary analysis file
    temp_file = f'{CSV_PATH}/temp_comprehensive_analysis.csv'
    analysis.to_csv(temp_file, index=False)
    print(f"✅ Saved comprehensive analysis to: temp_comprehensive_analysis.csv")
    print(f"   Shape: {analysis.shape}")
    print(f"   Columns: {list(analysis.columns)}")

    # Demonstrate reading it back in same session
    reloaded = pd.read_csv(temp_file)
    print(f"✅ Successfully reloaded: {reloaded.shape}")

    # Show summary
    print("\n=== COMPREHENSIVE ANALYSIS SUMMARY ===")
    summary = reloaded.nlargest(3, 'risk_adj_return')[['ticker', 'name', 'return_pct', 'market_cap_billions']]
    print(summary.round(2))
```

### 6. Error Handling and Robust File Loading

```python
# Robust file loading with error handling
def load_latest_csv(file_pattern):
    \"\"\"Load the most recent CSV file matching the pattern\"\"\"
    try:
        files = [f for f in os.listdir(CSV_PATH) if file_pattern in f and f.endswith('.csv')]
        if not files:
            print(f"❌ No files found matching pattern: {file_pattern}")
            return None

        latest_file = sorted(files)[-1]  # Get the last one alphabetically
        df = pd.read_csv(f'{CSV_PATH}/{latest_file}')
        print(f"✅ Loaded {latest_file}: {df.shape}")
        return df

    except Exception as e:
        print(f"❌ Error loading {file_pattern}: {e}")
        return None

# Use robust loading
price_data = load_latest_csv('price_data_')
ticker_details = load_latest_csv('ticker_details_')
price_metrics = load_latest_csv('price_metrics_')

# Only proceed if we have the required data
if price_metrics is not None:
    print("\n=== PORTFOLIO RISK ANALYSIS ===")

    # Volatility analysis
    high_vol = price_metrics[price_metrics['volatility'] > price_metrics['volatility'].mean()]
    low_vol = price_metrics[price_metrics['volatility'] <= price_metrics['volatility'].mean()]

    print(f"High volatility stocks ({len(high_vol)}):")
    for _, stock in high_vol.iterrows():
        print(f"  {stock['ticker']}: {stock['volatility']*100:.1f}% volatility, {stock['total_return']*100:.1f}% return")

    print(f"\\nLow volatility stocks ({len(low_vol)}):")
    for _, stock in low_vol.iterrows():
        print(f"  {stock['ticker']}: {stock['volatility']*100:.1f}% volatility, {stock['total_return']*100:.1f}% return")
```

---

## Best Practices

1. **Always reload data** - Start each py_eval with file discovery and loading
2. **Use error handling** - Check if files exist before loading
3. **Save intermediate results** - Use temporary CSV files for complex multi-step analysis
4. **Keep analysis focused** - Each py_eval call should accomplish one analytical goal
5. **Print progress** - Use print statements to show what your code is doing
6. **Combine datasets** - Merge price_metrics with ticker_details for comprehensive analysis
"""
