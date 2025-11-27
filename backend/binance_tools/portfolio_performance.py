"""
Portfolio Performance MCP Tool

Provides a comprehensive portfolio performance report comparing actual trading
results against a hypothetical buy-and-hold strategy (33% BTC, 33% ETH, 34% USDT).

Key features:
- Fixed $3000 initial portfolio baseline
- Trade-focused: Uses actual spot trades only (no P2P/deposit complexity)
- Smart initialization: Adjusts initial allocation to prevent negative holdings
- Benchmark rebalances on each actual trade timestamp
"""

import logging
import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for server use
import matplotlib.pyplot as plt
from binance.client import Client
from typing import Optional, Dict, Tuple, Any, List
from mcp_service import format_csv_response
from sentry_utils import with_sentry_tracing
from mcp.server.fastmcp import Image as MCPImage
from PIL import Image as PILImage
from mcp_image_utils import to_mcp_image

logger = logging.getLogger(__name__)

# Portfolio constants
INITIAL_CAPITAL = 3000.0  # Fixed initial portfolio value in USD
PORTFOLIO_WEIGHTS = {
    'BTC': 0.333,
    'ETH': 0.333,
    'USDT': 0.334  # Adjusted to sum to 1.0
}

# Visualization settings
FIGURE_SIZE = (14, 8)
DPI = 100


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_price_at_timestamp(klines_df: pd.DataFrame, timestamp: pd.Timestamp) -> Optional[float]:
    """Get price at or nearest to a specific timestamp"""
    if klines_df.empty:
        return None

    # Find closest timestamp (prefer earlier)
    klines_df_copy = klines_df.copy()
    klines_df_copy['time_diff'] = abs(klines_df_copy['timestamp'] - timestamp)
    closest = klines_df_copy.loc[klines_df_copy['time_diff'].idxmin()]

    return closest['close']


# ============================================================================
# DATA FETCHING FUNCTIONS
# ============================================================================

def fetch_historical_klines(client: Client, symbol: str, interval: str = '1h', days: int = 30) -> pd.DataFrame:
    """Fetch historical klines/candlestick data"""
    logger.info(f"Fetching historical klines for {symbol} ({interval}, last {days} days)...")

    try:
        start_time = datetime.now() - timedelta(days=days)
        start_str = start_time.strftime('%Y-%m-%d')

        klines = client.get_historical_klines(
            symbol=symbol,
            interval=interval,
            start_str=start_str
        )

        records = []
        for kline in klines:
            records.append({
                'timestamp': datetime.fromtimestamp(int(kline[0]) / 1000),
                'timestamp_ms': int(kline[0]),
                'open': float(kline[1]),
                'high': float(kline[2]),
                'low': float(kline[3]),
                'close': float(kline[4]),
                'volume': float(kline[5])
            })

        df = pd.DataFrame(records)
        logger.info(f"Loaded {len(df)} klines for {symbol}")

        return df

    except Exception as e:
        logger.warning(f"Error fetching klines for {symbol}: {e}")
        return pd.DataFrame()


def fetch_spot_trade_history(client: Client, symbol: str, days: Optional[int] = None) -> pd.DataFrame:
    """Fetch spot trade history for a symbol"""
    logger.info(f"Fetching spot trade history for {symbol}...")

    try:
        trades = client.get_my_trades(symbol=symbol, limit=1000)

        if not trades:
            return pd.DataFrame()

        # Calculate cutoff time if days specified
        cutoff_time = None
        if days is not None:
            cutoff_time = (datetime.now() - timedelta(days=days)).timestamp() * 1000

        records = []
        for trade in trades:
            trade_time = int(trade['time'])

            # Skip if outside time window
            if cutoff_time and trade_time < cutoff_time:
                continue

            records.append({
                'timestamp': datetime.fromtimestamp(trade_time / 1000),
                'timestamp_ms': trade_time,
                'symbol': trade['symbol'],
                'side': 'BUY' if trade['isBuyer'] else 'SELL',
                'price': float(trade['price']),
                'qty': float(trade['qty']),
                'quote_qty': float(trade['quoteQty']),
                'commission': float(trade['commission']),
                'commission_asset': trade['commissionAsset']
            })

        df = pd.DataFrame(records)
        if not df.empty:
            df = df.sort_values('timestamp').reset_index(drop=True)

        logger.info(f"Found {len(df)} trades for {symbol}")

        return df

    except Exception as e:
        logger.warning(f"Error fetching trades for {symbol}: {e}")
        return pd.DataFrame()


# ============================================================================
# PORTFOLIO BUILDING FUNCTIONS
# ============================================================================

def build_trades_table(client: Client, days: int, klines_btc: pd.DataFrame,
                       klines_eth: pd.DataFrame) -> pd.DataFrame:
    """
    Build a comprehensive trades table with historical prices attached.

    Returns DataFrame with columns:
    - timestamp, symbol, side, qty, quote_qty, price, commission, commission_asset
    - btc_price, eth_price (market prices at trade time for portfolio valuation)
    """
    logger.info(f"Building trades table for last {days} days...")

    # Fetch trades for both pairs
    trades_btc = fetch_spot_trade_history(client, 'BTCUSDT', days=days)
    trades_eth = fetch_spot_trade_history(client, 'ETHUSDT', days=days)

    # Combine all trades
    all_trades = pd.concat([trades_btc, trades_eth], ignore_index=True)

    if all_trades.empty:
        logger.warning("No trades found in the specified period")
        return pd.DataFrame()

    # Sort by timestamp
    all_trades = all_trades.sort_values('timestamp').reset_index(drop=True)

    # Attach historical prices for BTC and ETH at each trade timestamp
    btc_prices = []
    eth_prices = []

    for _, trade in all_trades.iterrows():
        btc_price = get_price_at_timestamp(klines_btc, trade['timestamp'])
        eth_price = get_price_at_timestamp(klines_eth, trade['timestamp'])
        btc_prices.append(btc_price)
        eth_prices.append(eth_price)

    all_trades['btc_price'] = btc_prices
    all_trades['eth_price'] = eth_prices

    logger.info(f"Built trades table with {len(all_trades)} trades")

    return all_trades


def calculate_initial_allocation(trades_df: pd.DataFrame, klines_btc: pd.DataFrame,
                                  klines_eth: pd.DataFrame, start_date: datetime) -> Dict[str, float]:
    """
    Calculate initial allocation using smart initialization.

    Strategy:
    1. Start with 33/33/34 baseline allocation
    2. Replay all trades to find minimum holdings for each asset
    3. If any asset goes negative, increase its initial allocation
    4. Adjust USDT to keep total at $3000

    Returns dict with initial holdings: {'BTC': x, 'ETH': y, 'USDT': z}
    """
    logger.info("Calculating smart initial allocation...")

    # Get prices at start date for initial allocation
    start_timestamp = pd.Timestamp(start_date)
    btc_price_start = get_price_at_timestamp(klines_btc, start_timestamp)
    eth_price_start = get_price_at_timestamp(klines_eth, start_timestamp)

    if btc_price_start is None or eth_price_start is None:
        # Fallback to first available price
        btc_price_start = klines_btc['close'].iloc[0] if not klines_btc.empty else 60000
        eth_price_start = klines_eth['close'].iloc[0] if not klines_eth.empty else 3000

    # Base allocation: 33/33/34
    base_btc = (INITIAL_CAPITAL * PORTFOLIO_WEIGHTS['BTC']) / btc_price_start
    base_eth = (INITIAL_CAPITAL * PORTFOLIO_WEIGHTS['ETH']) / eth_price_start
    base_usdt = INITIAL_CAPITAL * PORTFOLIO_WEIGHTS['USDT']

    if trades_df.empty:
        return {
            'BTC': base_btc,
            'ETH': base_eth,
            'USDT': base_usdt,
            'btc_price_start': btc_price_start,
            'eth_price_start': eth_price_start
        }

    # Replay trades to find minimum holdings
    holdings = {'BTC': base_btc, 'ETH': base_eth, 'USDT': base_usdt}
    min_holdings = {'BTC': base_btc, 'ETH': base_eth, 'USDT': base_usdt}

    for _, trade in trades_df.iterrows():
        if trade['symbol'] == 'BTCUSDT':
            if trade['side'] == 'BUY':
                holdings['BTC'] += trade['qty']
                holdings['USDT'] -= trade['quote_qty']
            else:  # SELL
                holdings['BTC'] -= trade['qty']
                holdings['USDT'] += trade['quote_qty']
        elif trade['symbol'] == 'ETHUSDT':
            if trade['side'] == 'BUY':
                holdings['ETH'] += trade['qty']
                holdings['USDT'] -= trade['quote_qty']
            else:  # SELL
                holdings['ETH'] -= trade['qty']
                holdings['USDT'] += trade['quote_qty']

        # Handle commission
        comm_asset = trade['commission_asset']
        if comm_asset in holdings:
            holdings[comm_asset] -= trade['commission']

        # Track minimum
        for asset in ['BTC', 'ETH', 'USDT']:
            min_holdings[asset] = min(min_holdings[asset], holdings[asset])

    # Adjust initial allocation if any minimum is negative
    adjustment_needed = False
    adjustments = {'BTC': 0.0, 'ETH': 0.0, 'USDT': 0.0}

    for asset in ['BTC', 'ETH']:
        if min_holdings[asset] < 0:
            adjustment_needed = True
            # Need to add enough to make minimum = 0 (with small buffer)
            deficit = abs(min_holdings[asset]) * 1.01  # 1% buffer
            adjustments[asset] = deficit
            logger.info(f"Adjusting initial {asset} allocation by +{deficit:.8f} to prevent negative holdings")

    if adjustment_needed:
        # Calculate USD value of adjustments
        btc_adjustment_usd = adjustments['BTC'] * btc_price_start
        eth_adjustment_usd = adjustments['ETH'] * eth_price_start
        total_adjustment_usd = btc_adjustment_usd + eth_adjustment_usd

        # Reduce USDT allocation to compensate
        final_allocation = {
            'BTC': base_btc + adjustments['BTC'],
            'ETH': base_eth + adjustments['ETH'],
            'USDT': base_usdt - total_adjustment_usd
        }

        # Check if USDT went negative - if so, we need to scale down
        if final_allocation['USDT'] < 0:
            logger.warning("USDT allocation went negative, scaling all allocations proportionally")
            # Scale everything to fit within $3000
            total_needed = (final_allocation['BTC'] * btc_price_start +
                          final_allocation['ETH'] * eth_price_start)
            if total_needed > INITIAL_CAPITAL:
                scale = INITIAL_CAPITAL / total_needed * 0.95  # Leave 5% for USDT
                final_allocation['BTC'] *= scale
                final_allocation['ETH'] *= scale
                final_allocation['USDT'] = INITIAL_CAPITAL - (final_allocation['BTC'] * btc_price_start +
                                                              final_allocation['ETH'] * eth_price_start)
    else:
        final_allocation = {
            'BTC': base_btc,
            'ETH': base_eth,
            'USDT': base_usdt
        }

    final_allocation['btc_price_start'] = btc_price_start
    final_allocation['eth_price_start'] = eth_price_start

    # Log final allocation
    total_value = (final_allocation['BTC'] * btc_price_start +
                   final_allocation['ETH'] * eth_price_start +
                   final_allocation['USDT'])
    logger.info(f"Final initial allocation: BTC={final_allocation['BTC']:.8f} (${final_allocation['BTC'] * btc_price_start:.2f}), "
                f"ETH={final_allocation['ETH']:.8f} (${final_allocation['ETH'] * eth_price_start:.2f}), "
                f"USDT=${final_allocation['USDT']:.2f}, Total=${total_value:.2f}")

    return final_allocation


def build_actual_portfolio(trades_df: pd.DataFrame, initial_allocation: Dict[str, float],
                           klines_btc: pd.DataFrame, klines_eth: pd.DataFrame,
                           start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Build actual portfolio equity curve by replaying trades.

    Starting from initial_allocation, replay each trade and calculate daily portfolio value.
    """
    logger.info("Building actual portfolio equity curve...")

    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    equity_data = []

    for date in date_range:
        date_dt = pd.Timestamp(date)

        # Start with initial allocation
        holdings = {
            'BTC': initial_allocation['BTC'],
            'ETH': initial_allocation['ETH'],
            'USDT': initial_allocation['USDT']
        }

        # Apply trades up to this date
        if not trades_df.empty:
            trades_to_date = trades_df[trades_df['timestamp'].dt.date <= date.date()]

            for _, trade in trades_to_date.iterrows():
                if trade['symbol'] == 'BTCUSDT':
                    if trade['side'] == 'BUY':
                        holdings['BTC'] += trade['qty']
                        holdings['USDT'] -= trade['quote_qty']
                    else:  # SELL
                        holdings['BTC'] -= trade['qty']
                        holdings['USDT'] += trade['quote_qty']
                elif trade['symbol'] == 'ETHUSDT':
                    if trade['side'] == 'BUY':
                        holdings['ETH'] += trade['qty']
                        holdings['USDT'] -= trade['quote_qty']
                    else:  # SELL
                        holdings['ETH'] -= trade['qty']
                        holdings['USDT'] += trade['quote_qty']

                # Handle commission
                comm_asset = trade['commission_asset']
                if comm_asset in holdings:
                    holdings[comm_asset] -= trade['commission']

        # Get prices for this date
        btc_price = get_price_at_timestamp(klines_btc, date_dt)
        eth_price = get_price_at_timestamp(klines_eth, date_dt)

        if btc_price is None or eth_price is None:
            continue

        # Calculate portfolio value
        portfolio_value = (
            holdings['BTC'] * btc_price +
            holdings['ETH'] * eth_price +
            holdings['USDT']
        )

        equity_data.append({
            'date': date,
            'equity_usdt': portfolio_value,
            'btc_holdings': holdings['BTC'],
            'eth_holdings': holdings['ETH'],
            'usdt_holdings': holdings['USDT'],
            'btc_price': btc_price,
            'eth_price': eth_price
        })

    equity_df = pd.DataFrame(equity_data)

    if not equity_df.empty:
        logger.info(f"Actual portfolio: Start=${equity_df.iloc[0]['equity_usdt']:,.2f}, "
                   f"End=${equity_df.iloc[-1]['equity_usdt']:,.2f}")

    return equity_df


def build_benchmark_portfolio(trades_df: pd.DataFrame, initial_allocation: Dict[str, float],
                              klines_btc: pd.DataFrame, klines_eth: pd.DataFrame,
                              start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Build benchmark portfolio equity curve (33/33/34 allocation).

    Benchmark rebalances to target weights on each actual trade timestamp.
    """
    logger.info("Building benchmark portfolio equity curve (33% BTC, 33% ETH, 34% USDT)...")

    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    # Get trade timestamps for rebalancing
    rebalance_dates = set()
    if not trades_df.empty:
        rebalance_dates = set(trades_df['timestamp'].dt.date.tolist())

    # Initial benchmark allocation (same starting point as actual for fair comparison)
    benchmark_holdings = {
        'BTC': initial_allocation['BTC'],
        'ETH': initial_allocation['ETH'],
        'USDT': initial_allocation['USDT']
    }

    equity_data = []

    for date in date_range:
        date_dt = pd.Timestamp(date)

        # Get prices for this date
        btc_price = get_price_at_timestamp(klines_btc, date_dt)
        eth_price = get_price_at_timestamp(klines_eth, date_dt)

        if btc_price is None or eth_price is None:
            continue

        # Rebalance if this is a trade date
        if date.date() in rebalance_dates:
            # Calculate current portfolio value
            current_value = (
                benchmark_holdings['BTC'] * btc_price +
                benchmark_holdings['ETH'] * eth_price +
                benchmark_holdings['USDT']
            )

            # Rebalance to target weights
            benchmark_holdings['BTC'] = (current_value * PORTFOLIO_WEIGHTS['BTC']) / btc_price
            benchmark_holdings['ETH'] = (current_value * PORTFOLIO_WEIGHTS['ETH']) / eth_price
            benchmark_holdings['USDT'] = current_value * PORTFOLIO_WEIGHTS['USDT']

        # Calculate portfolio value
        portfolio_value = (
            benchmark_holdings['BTC'] * btc_price +
            benchmark_holdings['ETH'] * eth_price +
            benchmark_holdings['USDT']
        )

        equity_data.append({
            'date': date,
            'equity_usdt': portfolio_value,
            'btc_holdings': benchmark_holdings['BTC'],
            'eth_holdings': benchmark_holdings['ETH'],
            'usdt_holdings': benchmark_holdings['USDT'],
            'btc_price': btc_price,
            'eth_price': eth_price
        })

    equity_df = pd.DataFrame(equity_data)

    if not equity_df.empty:
        logger.info(f"Benchmark portfolio: Start=${equity_df.iloc[0]['equity_usdt']:,.2f}, "
                   f"End=${equity_df.iloc[-1]['equity_usdt']:,.2f}")

    return equity_df


# ============================================================================
# METRICS AND VISUALIZATION
# ============================================================================

def calculate_metrics(actual_df: pd.DataFrame, benchmark_df: pd.DataFrame,
                      trades_df: pd.DataFrame) -> Tuple[Dict, pd.DataFrame]:
    """Calculate comparison metrics"""
    logger.info("Calculating performance metrics...")

    # Merge dataframes
    merged = pd.merge(
        actual_df[['date', 'equity_usdt']].rename(columns={'equity_usdt': 'actual'}),
        benchmark_df[['date', 'equity_usdt']].rename(columns={'equity_usdt': 'benchmark'}),
        on='date',
        how='outer'
    ).sort_values('date')

    merged = merged.ffill().bfill()

    # Calculate metrics
    actual_start = merged['actual'].iloc[0]
    actual_final = merged['actual'].iloc[-1]
    benchmark_start = merged['benchmark'].iloc[0]
    benchmark_final = merged['benchmark'].iloc[-1]

    actual_return = ((actual_final - actual_start) / actual_start) * 100
    benchmark_return = ((benchmark_final - benchmark_start) / benchmark_start) * 100

    # Maximum drawdown
    actual_peak = merged['actual'].expanding().max()
    actual_dd = ((merged['actual'] - actual_peak) / actual_peak * 100).min()

    benchmark_peak = merged['benchmark'].expanding().max()
    benchmark_dd = ((merged['benchmark'] - benchmark_peak) / benchmark_peak * 100).min()

    # Trade statistics
    total_trades = len(trades_df) if not trades_df.empty else 0
    buy_trades = len(trades_df[trades_df['side'] == 'BUY']) if not trades_df.empty else 0
    sell_trades = len(trades_df[trades_df['side'] == 'SELL']) if not trades_df.empty else 0

    metrics = {
        'initial_capital': INITIAL_CAPITAL,
        'actual': {
            'start': actual_start,
            'final': actual_final,
            'return_pct': actual_return,
            'profit_loss': actual_final - actual_start,
            'max_drawdown_pct': actual_dd
        },
        'benchmark': {
            'start': benchmark_start,
            'final': benchmark_final,
            'return_pct': benchmark_return,
            'profit_loss': benchmark_final - benchmark_start,
            'max_drawdown_pct': benchmark_dd
        },
        'trade_stats': {
            'total_trades': total_trades,
            'buy_trades': buy_trades,
            'sell_trades': sell_trades
        }
    }

    logger.info(f"Actual return: {actual_return:+.2f}%, Benchmark return: {benchmark_return:+.2f}%")

    return metrics, merged


def create_visualization(merged_df: pd.DataFrame, metrics: Dict, output_path: Path) -> None:
    """Create comparison chart"""
    logger.info("Generating visualization...")

    fig, ax = plt.subplots(figsize=FIGURE_SIZE, dpi=DPI)

    ax.plot(merged_df['date'], merged_df['actual'],
            label='Actual Trading Portfolio', linewidth=2.5, color='#2E86AB')
    ax.plot(merged_df['date'], merged_df['benchmark'],
            label='Benchmark (33% BTC, 33% ETH, 34% USDT)', linewidth=2.5,
            color='#A23B72', linestyle='--')

    ax.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax.set_ylabel('Portfolio Value (USDT)', fontsize=12, fontweight='bold')
    ax.set_title('Portfolio Performance: Trading vs Benchmark\n$3,000 Initial Capital',
                 fontsize=15, fontweight='bold', pad=20)
    ax.legend(fontsize=11, loc='best')
    ax.grid(True, alpha=0.3, linestyle=':')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))

    # Add metrics box
    outperformance = metrics['actual']['return_pct'] - metrics['benchmark']['return_pct']
    metrics_text = (
        f"INITIAL CAPITAL: ${metrics['initial_capital']:,.2f}\n"
        f"\n"
        f"ACTUAL TRADING\n"
        f"  Final: ${metrics['actual']['final']:,.2f}\n"
        f"  Return: {metrics['actual']['return_pct']:+.2f}%\n"
        f"  P/L: ${metrics['actual']['profit_loss']:+,.2f}\n"
        f"  Max DD: {metrics['actual']['max_drawdown_pct']:.2f}%\n"
        f"\n"
        f"BENCHMARK (33/33/34)\n"
        f"  Final: ${metrics['benchmark']['final']:,.2f}\n"
        f"  Return: {metrics['benchmark']['return_pct']:+.2f}%\n"
        f"  P/L: ${metrics['benchmark']['profit_loss']:+,.2f}\n"
        f"  Max DD: {metrics['benchmark']['max_drawdown_pct']:.2f}%\n"
        f"\n"
        f"Outperformance: {outperformance:+.2f}%\n"
        f"Total Trades: {metrics['trade_stats']['total_trades']}"
    )

    props = dict(boxstyle='round', facecolor='wheat', alpha=0.85)
    ax.text(0.02, 0.98, metrics_text, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', bbox=props, family='monospace')

    plt.tight_layout()
    plt.savefig(output_path, dpi=DPI, bbox_inches='tight')
    plt.close(fig)

    logger.info(f"Saved visualization: {output_path.name}")


def build_markdown_report(trades_df: pd.DataFrame, metrics: Dict, initial_allocation: Dict) -> str:
    """Build markdown-formatted performance report"""

    outperformance = metrics['actual']['return_pct'] - metrics['benchmark']['return_pct']
    outperf_verdict = "Trading strategy outperformed benchmark" if outperformance > 0 else "Trading strategy underperformed benchmark"

    report = f"""# Portfolio Performance Report

## Initial Setup

**Initial Capital:** ${metrics['initial_capital']:,.2f}

**Initial Allocation:**
- BTC: {initial_allocation['BTC']:.8f} (${initial_allocation['BTC'] * initial_allocation['btc_price_start']:,.2f})
- ETH: {initial_allocation['ETH']:.8f} (${initial_allocation['ETH'] * initial_allocation['eth_price_start']:,.2f})
- USDT: ${initial_allocation['USDT']:,.2f}

---

## Performance Metrics

### ACTUAL TRADING PORTFOLIO
- **Starting Value:** ${metrics['actual']['start']:,.2f}
- **Final Value:** ${metrics['actual']['final']:,.2f}
- **Return:** {metrics['actual']['return_pct']:+.2f}%
- **Profit/Loss:** ${metrics['actual']['profit_loss']:+,.2f}
- **Max Drawdown:** {metrics['actual']['max_drawdown_pct']:.2f}%

### BENCHMARK (33% BTC, 33% ETH, 34% USDT)
- **Starting Value:** ${metrics['benchmark']['start']:,.2f}
- **Final Value:** ${metrics['benchmark']['final']:,.2f}
- **Return:** {metrics['benchmark']['return_pct']:+.2f}%
- **Profit/Loss:** ${metrics['benchmark']['profit_loss']:+,.2f}
- **Max Drawdown:** {metrics['benchmark']['max_drawdown_pct']:.2f}%

---

## Comparison Analysis

**Outperformance:** {outperformance:+.2f}%

**Verdict:** {outperf_verdict}

---

## Trade Statistics

- **Total Trades:** {metrics['trade_stats']['total_trades']}
- **Buy Trades:** {metrics['trade_stats']['buy_trades']}
- **Sell Trades:** {metrics['trade_stats']['sell_trades']}

"""

    # Add recent trades
    if not trades_df.empty and len(trades_df) > 0:
        report += "\n### Recent Trades\n\n"
        report += "| Time | Symbol | Side | Qty | Price | Value |\n"
        report += "|------|--------|------|-----|-------|-------|\n"
        for _, trade in trades_df.tail(min(10, len(trades_df))).iterrows():
            report += f"| {trade['timestamp'].strftime('%Y-%m-%d %H:%M')} | {trade['symbol']} | {trade['side']} | {trade['qty']:.6f} | ${trade['price']:,.2f} | ${trade['quote_qty']:,.2f} |\n"

    return report


# ============================================================================
# MAIN FETCH FUNCTION
# ============================================================================

@with_sentry_tracing("binance_portfolio_performance")
def fetch_portfolio_performance(binance_client: Client, csv_dir: Path, days: int = 30) -> Dict[str, any]:
    """
    Generate comprehensive portfolio performance report.

    Args:
        binance_client: Initialized Binance Client
        csv_dir: Directory to save CSV files
        days: Number of days to analyze (default: 30)

    Returns:
        Dictionary with:
        - trades_csv_path: Path to trades table CSV
        - equity_csv_path: Path to equity curves CSV
        - metrics_csv_path: Path to metrics CSV
        - png_path: Path to visualization PNG
        - markdown_report: Formatted markdown report string
        - metrics: Dictionary with performance metrics
    """
    logger.info(f"Starting portfolio performance analysis for last {days} days")

    try:
        # Fetch historical prices
        logger.info("Fetching historical prices...")
        klines_btc = fetch_historical_klines(binance_client, 'BTCUSDT', interval='1h', days=days)
        klines_eth = fetch_historical_klines(binance_client, 'ETHUSDT', interval='1h', days=days)

        if klines_btc.empty or klines_eth.empty:
            raise ValueError("Failed to fetch historical price data")

        # Build trades table
        trades_df = build_trades_table(binance_client, days, klines_btc, klines_eth)

        # Determine date range
        end_date = datetime.now().date()
        if not trades_df.empty:
            start_date = trades_df['timestamp'].min().date()
        else:
            start_date = (datetime.now() - timedelta(days=days)).date()

        # Calculate smart initial allocation
        initial_allocation = calculate_initial_allocation(trades_df, klines_btc, klines_eth, start_date)

        # Build portfolios
        actual_equity = build_actual_portfolio(trades_df, initial_allocation, klines_btc, klines_eth, start_date, end_date)
        benchmark_equity = build_benchmark_portfolio(trades_df, initial_allocation, klines_btc, klines_eth, start_date, end_date)

        if actual_equity.empty or benchmark_equity.empty:
            raise ValueError("Failed to build equity curves")

        # Calculate metrics
        metrics, merged = calculate_metrics(actual_equity, benchmark_equity, trades_df)

        # Generate unique ID for this report
        report_id = str(uuid.uuid4())[:8]

        # Save trades table CSV
        trades_csv_path = csv_dir / f"portfolio_trades_{report_id}.csv"
        if not trades_df.empty:
            trades_df.to_csv(trades_csv_path, index=False)
            logger.info(f"Saved trades CSV: {trades_csv_path.name}")
        else:
            # Create empty CSV with headers
            pd.DataFrame(columns=['timestamp', 'symbol', 'side', 'qty', 'quote_qty', 'price',
                                  'commission', 'commission_asset', 'btc_price', 'eth_price']).to_csv(trades_csv_path, index=False)

        # Save equity curves CSV
        equity_csv_path = csv_dir / f"portfolio_equity_{report_id}.csv"
        merged.to_csv(equity_csv_path, index=False)
        logger.info(f"Saved equity curves CSV: {equity_csv_path.name}")

        # Convert metrics to CSV format
        metrics_records = [
            {'metric': 'initial_capital', 'value': metrics['initial_capital']},
            {'metric': 'actual_start', 'value': metrics['actual']['start']},
            {'metric': 'actual_final', 'value': metrics['actual']['final']},
            {'metric': 'actual_return_pct', 'value': metrics['actual']['return_pct']},
            {'metric': 'actual_profit_loss', 'value': metrics['actual']['profit_loss']},
            {'metric': 'actual_max_drawdown_pct', 'value': metrics['actual']['max_drawdown_pct']},
            {'metric': 'benchmark_start', 'value': metrics['benchmark']['start']},
            {'metric': 'benchmark_final', 'value': metrics['benchmark']['final']},
            {'metric': 'benchmark_return_pct', 'value': metrics['benchmark']['return_pct']},
            {'metric': 'benchmark_profit_loss', 'value': metrics['benchmark']['profit_loss']},
            {'metric': 'benchmark_max_drawdown_pct', 'value': metrics['benchmark']['max_drawdown_pct']},
            {'metric': 'outperformance_pct', 'value': metrics['actual']['return_pct'] - metrics['benchmark']['return_pct']},
            {'metric': 'total_trades', 'value': metrics['trade_stats']['total_trades']},
            {'metric': 'buy_trades', 'value': metrics['trade_stats']['buy_trades']},
            {'metric': 'sell_trades', 'value': metrics['trade_stats']['sell_trades']}
        ]
        metrics_df = pd.DataFrame(metrics_records)
        metrics_csv_path = csv_dir / f"portfolio_metrics_{report_id}.csv"
        metrics_df.to_csv(metrics_csv_path, index=False)
        logger.info(f"Saved metrics CSV: {metrics_csv_path.name}")

        # Generate visualization
        png_path = csv_dir / f"portfolio_chart_{report_id}.png"
        create_visualization(merged, metrics, png_path)

        # Build markdown report
        markdown_report = build_markdown_report(trades_df, metrics, initial_allocation)

        logger.info("Portfolio performance analysis completed successfully")

        return {
            'trades_csv_path': trades_csv_path,
            'equity_csv_path': equity_csv_path,
            'metrics_csv_path': metrics_csv_path,
            'png_path': png_path,
            'markdown_report': markdown_report,
            'metrics': metrics,
            'merged_df': merged,
            'trades_df': trades_df,
            'initial_allocation': initial_allocation
        }

    except Exception as e:
        logger.error(f"Error in portfolio performance analysis: {e}")
        raise


# ============================================================================
# MCP TOOL REGISTRATION
# ============================================================================

def register_binance_portfolio_performance(local_mcp_instance, local_binance_client, csv_dir):
    """Register the binance_portfolio_performance tool"""

    @local_mcp_instance.tool()
    def binance_portfolio_performance(days: int = 30) -> list[Any]:
        """
        Generate a comprehensive portfolio performance report comparing your actual trading
        results against a benchmark buy-and-hold strategy (33% BTC, 33% ETH, 34% USDT).

        **How it works:**
        - Uses a fixed $3,000 initial capital baseline
        - Both portfolios start with the same initial allocation
        - Actual portfolio tracks your real spot trades (BTCUSDT, ETHUSDT)
        - Benchmark rebalances to 33/33/34 weights on each trade date
        - Smart initialization prevents negative holdings

        **Generated outputs:**
        1. **Trades Table CSV** - All your trades with historical prices attached
        2. **Equity Curves CSV** - Daily portfolio values (actual vs benchmark)
        3. **Metrics CSV** - Returns, P/L, drawdowns, trade statistics
        4. **Chart PNG** - Visual comparison of equity curves
        5. **Markdown Report** - Comprehensive performance summary

        Parameters:
            days (int): Number of days to analyze (default: 30)

        Returns:
            list: [ImageContent, str] - Portfolio chart followed by detailed report

        CSV Output Files:

        1. **Trades Table** (`portfolio_trades_*.csv`):
            - timestamp (datetime): Trade execution time
            - symbol (string): Trading pair (BTCUSDT, ETHUSDT)
            - side (string): BUY or SELL
            - qty (float): Quantity traded
            - quote_qty (float): USDT value of trade
            - price (float): Execution price
            - commission (float): Trading fee
            - commission_asset (string): Fee asset
            - btc_price (float): BTC market price at trade time
            - eth_price (float): ETH market price at trade time

        2. **Equity Curves** (`portfolio_equity_*.csv`):
            - date (datetime): Date
            - actual (float): Actual portfolio value in USDT
            - benchmark (float): Benchmark portfolio value in USDT

        3. **Metrics** (`portfolio_metrics_*.csv`):
            - metric (string): Metric name
            - value (float): Metric value

        Example usage:
            binance_portfolio_performance(days=30)
            binance_portfolio_performance(days=7)

        Note:
            - Analysis limited to BTC, ETH, USDT spot trading
            - Initial capital fixed at $3,000 for consistent comparison
            - Benchmark rebalances on each trade date to maintain target weights
        """
        logger.info(f"binance_portfolio_performance tool invoked with days={days}")

        try:
            # Call the main fetch function
            result = fetch_portfolio_performance(
                binance_client=local_binance_client,
                csv_dir=csv_dir,
                days=days
            )

            # Build comprehensive response
            response_parts = []

            # Add trades table CSV info
            response_parts.append("## Trades Table\n")
            response_parts.append(format_csv_response(result['trades_csv_path'], result['trades_df']))

            # Add equity curves CSV info
            response_parts.append("\n\n## Portfolio Equity Curves\n")
            response_parts.append(format_csv_response(result['equity_csv_path'], result['merged_df']))

            # Add metrics CSV info
            metrics_df = pd.read_csv(result['metrics_csv_path'])
            response_parts.append("\n\n## Performance Metrics\n")
            response_parts.append(format_csv_response(result['metrics_csv_path'], metrics_df))

            # Add PNG file info
            png_size_bytes = result['png_path'].stat().st_size
            if png_size_bytes < 1024:
                png_size_str = f"{png_size_bytes} bytes"
            elif png_size_bytes < 1024 * 1024:
                png_size_str = f"{png_size_bytes / 1024:.1f} KB"
            else:
                png_size_str = f"{png_size_bytes / (1024 * 1024):.1f} MB"

            response_parts.append(f"\n\n## Visualization Chart\n")
            response_parts.append(f"Chart saved to PNG\n\n")
            response_parts.append(f"File: {result['png_path'].name}\n")
            response_parts.append(f"Size: {png_size_str}\n")

            # Generate download URL
            public_base = os.getenv("MCP_PUBLIC_ASSET_BASE_URL", "").rstrip("/")
            if not public_base:
                public_base_url = os.getenv("MCP_PUBLIC_BASE_URL", "").rstrip("/")
                if public_base_url:
                    public_base = f"{public_base_url}/binance/assets"

            if public_base:
                download_url = f"{public_base}/{result['png_path'].name}"
                response_parts.append(f"\n**Download URL:** {download_url}\n")
            else:
                response_parts.append(f"\nConfigure MCP_PUBLIC_BASE_URL to enable download links.\n")

            # Add markdown performance report
            response_parts.append("\n\n" + "="*80 + "\n")
            response_parts.append(result['markdown_report'])
            response_parts.append("\n" + "="*80)

            final_response = "".join(response_parts)

            # Load the PNG image and convert for inline display
            logger.info("Loading PNG for inline display...")
            with PILImage.open(result['png_path']) as full_image:
                # Create a copy for preview
                preview_image = full_image.copy()

                # Resize for optimal response size (max 1200x900 for chart readability)
                max_width = 1200
                max_height = 900
                preview_image.thumbnail((max_width, max_height), resample=PILImage.Resampling.LANCZOS)

                # Convert to RGB for JPEG compatibility
                if preview_image.mode not in ("RGB", "L"):
                    preview_image = preview_image.convert("RGB")

                # Convert to MCPImage with JPEG compression
                mcp_image = to_mcp_image(
                    preview_image,
                    format="jpeg",
                    quality=85,
                    optimize=True
                )

                # Convert to ImageContent
                image_content = mcp_image.to_image_content()

            logger.info(f"binance_portfolio_performance completed successfully")

            # Return list with image first, then text report
            return [image_content, final_response]

        except Exception as e:
            logger.error(f"Error in binance_portfolio_performance tool: {e}")
            error_msg = f"Error generating portfolio performance report: {str(e)}"
            # Return as list for consistency
            return [error_msg]
