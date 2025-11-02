"""
Portfolio Comparison MCP Tool

Provides a comprehensive portfolio performance report comparing actual trading
results against a hypothetical buy-and-hold strategy (33% BTC, 33% ETH, 33% USDT).

This tool wraps the portfolio_comparison_v2.py script logic and makes it available
as an MCP tool for easy access by AI agents.
"""

import logging
import uuid
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for server use
import matplotlib.pyplot as plt
from binance.client import Client
from typing import Optional, Dict, Tuple
from mcp_service import format_csv_response
from sentry_utils import with_sentry_tracing

logger = logging.getLogger(__name__)

# Portfolio allocation for hypothetical portfolio
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

    # Use close price
    return closest['close']


# ============================================================================
# DATA FETCHING FUNCTIONS
# ============================================================================

def fetch_p2p_history(client: Client, trade_type: str, days: int = 30) -> pd.DataFrame:
    """Fetch P2P trading history (BUY or SELL)"""
    logger.info(f"Fetching P2P {trade_type} history (last {days} days)...")

    try:
        response = client.get_c2c_trade_history(tradeType=trade_type, rows=100)

        if response.get('code') != '000000' or not response.get('success'):
            logger.warning(f"P2P API error: {response.get('message', 'Unknown error')}")
            return pd.DataFrame()

        trades = response.get('data', [])

        if not trades:
            logger.info(f"No {trade_type} trades found")
            return pd.DataFrame()

        # Filter by date and COMPLETED status only
        cutoff_time = (datetime.now() - timedelta(days=days)).timestamp() * 1000

        records = []
        for trade in trades:
            create_time = trade.get('createTime')

            # Skip if older than cutoff or not completed
            if create_time < cutoff_time:
                continue
            if trade.get('orderStatus') != 'COMPLETED':
                continue

            records.append({
                'timestamp': datetime.fromtimestamp(create_time / 1000),
                'timestamp_ms': create_time,
                'type': trade_type,
                'asset': trade.get('asset'),
                'fiat': trade.get('fiat'),
                'crypto_amount': float(trade.get('amount', 0)),
                'fiat_amount': float(trade.get('totalPrice', 0)),
                'unit_price': float(trade.get('unitPrice', 0)),
                'commission': float(trade.get('commission', 0))
            })

        df = pd.DataFrame(records)
        if not df.empty:
            df = df.sort_values('timestamp').reset_index(drop=True)
            logger.info(f"Found {len(df)} completed {trade_type} trades")

        return df

    except Exception as e:
        logger.warning(f"Error fetching P2P {trade_type} history: {e}")
        return pd.DataFrame()


def fetch_deposit_history(client: Client, days: int = 30) -> pd.DataFrame:
    """Fetch crypto deposit history"""
    logger.info(f"Fetching deposit history (last {days} days)...")

    try:
        deposits = client.get_deposit_history()

        if not deposits:
            logger.info("No deposits found")
            return pd.DataFrame()

        # Filter by date and SUCCESS status only
        cutoff_time = (datetime.now() - timedelta(days=days)).timestamp() * 1000

        records = []
        for deposit in deposits:
            complete_time = deposit.get('completeTime') or deposit.get('insertTime')

            if not complete_time:
                continue

            # Skip if older than cutoff or not successful
            if complete_time < cutoff_time:
                continue
            if deposit.get('status') != 1:  # 1 = Success
                continue

            records.append({
                'timestamp': datetime.fromtimestamp(complete_time / 1000),
                'timestamp_ms': complete_time,
                'type': 'DEPOSIT',
                'coin': deposit.get('coin'),
                'amount': float(deposit.get('amount', 0)),
                'network': deposit.get('network'),
                'txId': deposit.get('txId')
            })

        df = pd.DataFrame(records)
        if not df.empty:
            df = df.sort_values('timestamp').reset_index(drop=True)
            logger.info(f"Found {len(df)} successful deposits")

        return df

    except Exception as e:
        logger.warning(f"Error fetching deposit history: {e}")
        return pd.DataFrame()


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


def get_current_balances(client: Client) -> Dict[str, float]:
    """Get current spot account balances"""
    logger.info("Fetching current account balances...")

    try:
        account = client.get_account()
        balances = {}

        for balance in account['balances']:
            asset = balance['asset']
            free = float(balance['free'])
            locked = float(balance['locked'])
            total = free + locked

            # Only include non-zero balances
            if total > 0:
                balances[asset] = total

        # Ensure we have BTC, ETH, USDT even if zero
        for asset in ['BTC', 'ETH', 'USDT']:
            if asset not in balances:
                balances[asset] = 0.0

        logger.info(f"Current balances: BTC={balances.get('BTC', 0.0):.8f}, "
                   f"ETH={balances.get('ETH', 0.0):.8f}, USDT=${balances.get('USDT', 0.0):,.2f}")

        return balances

    except Exception as e:
        logger.warning(f"Error fetching account balances: {e}")
        return {'BTC': 0.0, 'ETH': 0.0, 'USDT': 0.0}


# ============================================================================
# PORTFOLIO BUILDING FUNCTIONS
# ============================================================================

def build_cash_flow_timeline(p2p_buy_df: pd.DataFrame, p2p_sell_df: pd.DataFrame,
                             deposit_df: pd.DataFrame, klines_btc: pd.DataFrame,
                             klines_eth: pd.DataFrame) -> pd.DataFrame:
    """Build timeline of all capitalization events with USD values"""
    logger.info("Building cash flow timeline...")

    events = []

    # P2P BUY events (cash in via fiat)
    for _, row in p2p_buy_df.iterrows():
        events.append({
            'timestamp': row['timestamp'],
            'timestamp_ms': row['timestamp_ms'],
            'type': 'P2P_BUY',
            'asset': row['asset'],
            'usd_value': row['fiat_amount'],
            'crypto_amount': row['crypto_amount'],
            'description': f"P2P BUY {row['crypto_amount']:.4f} {row['asset']} @ ${row['unit_price']:.2f}"
        })

    # P2P SELL events (cash out to fiat)
    for _, row in p2p_sell_df.iterrows():
        events.append({
            'timestamp': row['timestamp'],
            'timestamp_ms': row['timestamp_ms'],
            'type': 'P2P_SELL',
            'asset': row['asset'],
            'usd_value': -row['fiat_amount'],
            'crypto_amount': -row['crypto_amount'],
            'description': f"P2P SELL {row['crypto_amount']:.4f} {row['asset']} @ ${row['unit_price']:.2f}"
        })

    # Deposit events (crypto in)
    for _, row in deposit_df.iterrows():
        coin = row['coin']
        amount = row['amount']

        # Get price at deposit time
        price = None
        if coin == 'BTC':
            price = get_price_at_timestamp(klines_btc, row['timestamp'])
        elif coin == 'ETH':
            price = get_price_at_timestamp(klines_eth, row['timestamp'])
        elif coin == 'USDT':
            price = 1.0

        if price is None:
            logger.warning(f"Could not find price for {coin} deposit at {row['timestamp']}")
            continue

        usd_value = amount * price

        events.append({
            'timestamp': row['timestamp'],
            'timestamp_ms': row['timestamp_ms'],
            'type': 'DEPOSIT',
            'asset': coin,
            'usd_value': usd_value,
            'crypto_amount': amount,
            'description': f"DEPOSIT {amount:.8f} {coin} @ ${price:,.2f} = ${usd_value:,.2f}"
        })

    # Convert to DataFrame and sort
    events_df = pd.DataFrame(events)

    if events_df.empty:
        logger.warning("No cash flow events found!")
        return events_df

    events_df = events_df.sort_values('timestamp').reset_index(drop=True)

    # Calculate cumulative capital invested
    events_df['cumulative_invested'] = events_df['usd_value'].cumsum()

    logger.info(f"Total cash flow events: {len(events_df)}")
    logger.info(f"Net capital invested: ${events_df['cumulative_invested'].iloc[-1]:,.2f}")

    return events_df


def build_hypothetical_portfolio(events_df: pd.DataFrame, klines_btc: pd.DataFrame,
                                 klines_eth: pd.DataFrame, end_date) -> pd.DataFrame:
    """Build hypothetical buy-and-hold portfolio (33/33/33) using cash flow events"""
    logger.info("Building hypothetical portfolio (33% BTC, 33% ETH, 33% USDT)...")

    # Build daily equity curve from first event to end date
    start_date = events_df['timestamp'].min().date()
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    equity_data = []

    for date in date_range:
        date_dt = pd.Timestamp(date)

        # Apply any events that occurred on or before this date
        events_to_date = events_df[events_df['timestamp'].dt.date <= date.date()]

        if events_to_date.empty:
            continue

        # Recalculate holdings up to this date
        temp_btc = 0.0
        temp_eth = 0.0
        temp_usdt = 0.0

        for _, event in events_to_date.iterrows():
            capital_change = event['usd_value']
            event_timestamp = event['timestamp']

            btc_price_event = get_price_at_timestamp(klines_btc, event_timestamp)
            eth_price_event = get_price_at_timestamp(klines_eth, event_timestamp)

            if btc_price_event and eth_price_event:
                temp_btc += (capital_change * PORTFOLIO_WEIGHTS['BTC']) / btc_price_event
                temp_eth += (capital_change * PORTFOLIO_WEIGHTS['ETH']) / eth_price_event
                temp_usdt += capital_change * PORTFOLIO_WEIGHTS['USDT']

        # Get prices for THIS date to calculate value
        btc_price = get_price_at_timestamp(klines_btc, date_dt)
        eth_price = get_price_at_timestamp(klines_eth, date_dt)

        if btc_price is None or eth_price is None:
            continue

        portfolio_value = (
            temp_btc * btc_price +
            temp_eth * eth_price +
            temp_usdt
        )

        equity_data.append({
            'date': date,
            'equity_usdt': portfolio_value,
            'btc_value': temp_btc * btc_price,
            'eth_value': temp_eth * eth_price,
            'usdt_value': temp_usdt
        })

    equity_df = pd.DataFrame(equity_data)

    if not equity_df.empty:
        logger.info(f"Final hypothetical equity: ${equity_df.iloc[-1]['equity_usdt']:,.2f}")

    return equity_df


def build_actual_portfolio(client: Client, events_df: pd.DataFrame, klines_btc: pd.DataFrame,
                          klines_eth: pd.DataFrame, end_date, lookback_days: int) -> pd.DataFrame:
    """Build actual portfolio equity curve from trades and cash flows"""
    logger.info("Building actual portfolio equity curve...")

    # Get current actual balances for validation
    current_balances = get_current_balances(client)

    # Fetch spot trades filtered to the SAME time window as cash flows
    trades_btc = fetch_spot_trade_history(client, 'BTCUSDT', days=lookback_days)
    trades_eth = fetch_spot_trade_history(client, 'ETHUSDT', days=lookback_days)

    # Combine all trades
    all_trades = pd.concat([trades_btc, trades_eth], ignore_index=True)
    if not all_trades.empty:
        all_trades = all_trades.sort_values('timestamp').reset_index(drop=True)
        logger.info(f"Total spot trades in {lookback_days}-day window: {len(all_trades)}")

    # Process events and trades chronologically
    start_date = events_df['timestamp'].min().date() if not events_df.empty else (datetime.now() - timedelta(days=lookback_days)).date()
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    equity_data = []

    for date in date_range:
        date_dt = pd.Timestamp(date)

        # Apply cash flow events up to this date
        events_to_date = events_df[events_df['timestamp'].dt.date <= date.date()]

        # Reset holdings and replay all events/trades
        temp_holdings = {'BTC': 0.0, 'ETH': 0.0, 'USDT': 0.0}

        # Apply cash flow events
        for _, event in events_to_date.iterrows():
            if event['type'] == 'P2P_BUY':
                temp_holdings[event['asset']] += event['crypto_amount']
            elif event['type'] == 'P2P_SELL':
                temp_holdings[event['asset']] += event['crypto_amount']  # Already negative
            elif event['type'] == 'DEPOSIT':
                temp_holdings[event['asset']] += event['crypto_amount']

        # Apply trades up to this date
        if not all_trades.empty:
            trades_to_date = all_trades[all_trades['timestamp'].dt.date <= date.date()]

            for _, trade in trades_to_date.iterrows():
                if trade['symbol'] == 'BTCUSDT':
                    if trade['side'] == 'BUY':
                        temp_holdings['BTC'] += trade['qty']
                        temp_holdings['USDT'] -= trade['quote_qty']
                    else:  # SELL
                        temp_holdings['BTC'] -= trade['qty']
                        temp_holdings['USDT'] += trade['quote_qty']
                elif trade['symbol'] == 'ETHUSDT':
                    if trade['side'] == 'BUY':
                        temp_holdings['ETH'] += trade['qty']
                        temp_holdings['USDT'] -= trade['quote_qty']
                    else:  # SELL
                        temp_holdings['ETH'] -= trade['qty']
                        temp_holdings['USDT'] += trade['quote_qty']

                # Handle commission
                comm_asset = trade['commission_asset']
                if comm_asset in temp_holdings:
                    temp_holdings[comm_asset] -= trade['commission']

        # Get prices for THIS date
        btc_price = get_price_at_timestamp(klines_btc, date_dt)
        eth_price = get_price_at_timestamp(klines_eth, date_dt)

        if btc_price is None or eth_price is None:
            continue

        # Calculate portfolio value
        portfolio_value = (
            temp_holdings['BTC'] * btc_price +
            temp_holdings['ETH'] * eth_price +
            temp_holdings['USDT']
        )

        equity_data.append({
            'date': date,
            'equity_usdt': portfolio_value,
            'btc_holdings': temp_holdings['BTC'],
            'eth_holdings': temp_holdings['ETH'],
            'usdt_holdings': temp_holdings['USDT']
        })

    equity_df = pd.DataFrame(equity_data)

    if not equity_df.empty:
        logger.info(f"Calculated final equity: ${equity_df.iloc[-1]['equity_usdt']:,.2f}")

    return equity_df


# ============================================================================
# METRICS AND VISUALIZATION
# ============================================================================

def calculate_metrics(actual_df: pd.DataFrame, hypothetical_df: pd.DataFrame,
                     total_invested: float) -> Tuple[Dict, pd.DataFrame]:
    """Calculate comparison metrics"""
    logger.info("Calculating performance metrics...")

    # Merge dataframes
    merged = pd.merge(
        actual_df[['date', 'equity_usdt']].rename(columns={'equity_usdt': 'actual'}),
        hypothetical_df[['date', 'equity_usdt']].rename(columns={'equity_usdt': 'hypothetical'}),
        on='date',
        how='outer'
    ).sort_values('date')

    merged = merged.ffill().bfill()

    # Calculate metrics
    actual_final = merged['actual'].iloc[-1]
    hypo_final = merged['hypothetical'].iloc[-1]

    actual_return = ((actual_final - total_invested) / total_invested) * 100 if total_invested > 0 else 0
    hypo_return = ((hypo_final - total_invested) / total_invested) * 100 if total_invested > 0 else 0

    # Maximum drawdown
    actual_peak = merged['actual'].expanding().max()
    actual_dd = ((merged['actual'] - actual_peak) / actual_peak * 100).min()

    hypo_peak = merged['hypothetical'].expanding().max()
    hypo_dd = ((merged['hypothetical'] - hypo_peak) / hypo_peak * 100).min()

    metrics = {
        'total_invested': total_invested,
        'actual': {
            'final': actual_final,
            'return_pct': actual_return,
            'profit_loss': actual_final - total_invested,
            'max_drawdown_pct': actual_dd
        },
        'hypothetical': {
            'final': hypo_final,
            'return_pct': hypo_return,
            'profit_loss': hypo_final - total_invested,
            'max_drawdown_pct': hypo_dd
        }
    }

    logger.info(f"Actual return: {actual_return:+.2f}%, Hypothetical return: {hypo_return:+.2f}%")

    return metrics, merged


def create_visualization(merged_df: pd.DataFrame, metrics: Dict, output_path: Path) -> None:
    """Create comparison chart"""
    logger.info("Generating visualization...")

    fig, ax = plt.subplots(figsize=FIGURE_SIZE, dpi=DPI)

    ax.plot(merged_df['date'], merged_df['actual'],
            label='Actual Trading (Spot Only)', linewidth=2.5, color='#2E86AB')
    ax.plot(merged_df['date'], merged_df['hypothetical'],
            label='Buy-and-Hold (33% BTC, 33% ETH, 33% USDT)', linewidth=2.5,
            color='#A23B72', linestyle='--')

    ax.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax.set_ylabel('Portfolio Value (USDT)', fontsize=12, fontweight='bold')
    ax.set_title('Portfolio Comparison: Event-Based Analysis\nActual Trading vs Buy-and-Hold Strategy',
                 fontsize=15, fontweight='bold', pad=20)
    ax.legend(fontsize=11, loc='best')
    ax.grid(True, alpha=0.3, linestyle=':')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))

    # Add metrics box
    outperformance = metrics['actual']['return_pct'] - metrics['hypothetical']['return_pct']
    metrics_text = (
        f"INVESTED: ${metrics['total_invested']:,.2f}\n"
        f"\n"
        f"ACTUAL TRADING\n"
        f"  Final: ${metrics['actual']['final']:,.2f}\n"
        f"  Return: {metrics['actual']['return_pct']:+.2f}%\n"
        f"  P/L: ${metrics['actual']['profit_loss']:+,.2f}\n"
        f"  Max DD: {metrics['actual']['max_drawdown_pct']:.2f}%\n"
        f"\n"
        f"BUY-AND-HOLD\n"
        f"  Final: ${metrics['hypothetical']['final']:,.2f}\n"
        f"  Return: {metrics['hypothetical']['return_pct']:+.2f}%\n"
        f"  P/L: ${metrics['hypothetical']['profit_loss']:+,.2f}\n"
        f"  Max DD: {metrics['hypothetical']['max_drawdown_pct']:.2f}%\n"
        f"\n"
        f"Outperformance: {outperformance:+.2f}%"
    )

    props = dict(boxstyle='round', facecolor='wheat', alpha=0.85)
    ax.text(0.02, 0.98, metrics_text, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', bbox=props, family='monospace')

    plt.tight_layout()
    plt.savefig(output_path, dpi=DPI, bbox_inches='tight')
    plt.close(fig)

    logger.info(f"Saved visualization: {output_path.name}")


def build_markdown_report(events_df: pd.DataFrame, metrics: Dict) -> str:
    """Build markdown-formatted performance report"""

    # Cash flow summary
    p2p_buy_total = events_df[events_df['type'] == 'P2P_BUY']['usd_value'].sum() if not events_df.empty else 0
    p2p_sell_total = abs(events_df[events_df['type'] == 'P2P_SELL']['usd_value'].sum()) if not events_df.empty else 0
    deposit_total = events_df[events_df['type'] == 'DEPOSIT']['usd_value'].sum() if not events_df.empty else 0

    outperformance = metrics['actual']['return_pct'] - metrics['hypothetical']['return_pct']
    outperf_verdict = "✓ Trading strategy outperformed buy-and-hold" if outperformance > 0 else "✗ Trading strategy underperformed buy-and-hold"

    report = f"""# Portfolio Comparison Report

## Capital Invested Summary

**Total Capital Invested:** ${metrics['total_invested']:,.2f}

- **P2P BUY (deposits):** ${p2p_buy_total:,.2f}
- **P2P SELL (withdrawals):** ${p2p_sell_total:,.2f}
- **CRYPTO DEPOSITS:** ${deposit_total:,.2f}

---

## Performance Metrics

### ACTUAL TRADING
- **Final Equity:** ${metrics['actual']['final']:,.2f}
- **Return:** {metrics['actual']['return_pct']:+.2f}%
- **Profit/Loss:** ${metrics['actual']['profit_loss']:+,.2f}
- **Max Drawdown:** {metrics['actual']['max_drawdown_pct']:.2f}%

### BUY-AND-HOLD (33% BTC, 33% ETH, 33% USDT)
- **Final Equity:** ${metrics['hypothetical']['final']:,.2f}
- **Return:** {metrics['hypothetical']['return_pct']:+.2f}%
- **Profit/Loss:** ${metrics['hypothetical']['profit_loss']:+,.2f}
- **Max Drawdown:** {metrics['hypothetical']['max_drawdown_pct']:.2f}%

---

## Outperformance Analysis

**Outperformance:** {outperformance:+.2f}%

{outperf_verdict}

---

## Cash Flow Events

Total events: {len(events_df)}

"""

    # Add top events
    if not events_df.empty and len(events_df) > 0:
        report += "\n### Recent Events\n\n"
        for _, event in events_df.tail(min(10, len(events_df))).iterrows():
            sign = "+" if event['usd_value'] > 0 else ""
            report += f"- **{event['timestamp'].strftime('%Y-%m-%d %H:%M')}** | {event['type']} | {sign}${event['usd_value']:,.2f} | {event['description']}\n"

    return report


# ============================================================================
# MAIN FETCH FUNCTION
# ============================================================================

@with_sentry_tracing("binance_portfolio_comparison")
def fetch_portfolio_comparison(binance_client: Client, csv_dir: Path, days: int = 30) -> Dict[str, any]:
    """
    Generate comprehensive portfolio comparison report.

    Args:
        binance_client: Initialized Binance Client
        csv_dir: Directory to save CSV files
        days: Number of days to analyze (default: 30)

    Returns:
        Dictionary with:
        - equity_csv_path: Path to equity curves CSV
        - events_csv_path: Path to cash flow events CSV
        - metrics_csv_path: Path to metrics CSV
        - png_path: Path to visualization PNG
        - markdown_report: Formatted markdown report string
        - metrics: Dictionary with performance metrics
    """
    logger.info(f"Starting portfolio comparison analysis for last {days} days")

    try:
        # Fetch data
        logger.info("Fetching P2P history...")
        p2p_buy = fetch_p2p_history(binance_client, 'BUY', days=days)
        p2p_sell = fetch_p2p_history(binance_client, 'SELL', days=days)

        logger.info("Fetching deposit history...")
        deposits = fetch_deposit_history(binance_client, days=days)

        logger.info("Fetching historical prices...")
        klines_btc = fetch_historical_klines(binance_client, 'BTCUSDT', interval='1h', days=days)
        klines_eth = fetch_historical_klines(binance_client, 'ETHUSDT', interval='1h', days=days)

        if klines_btc.empty or klines_eth.empty:
            raise ValueError("Failed to fetch historical price data")

        # Build cash flow timeline
        events_df = build_cash_flow_timeline(p2p_buy, p2p_sell, deposits, klines_btc, klines_eth)

        if events_df.empty:
            raise ValueError("No cash flow events found in the specified period")

        total_invested = events_df['cumulative_invested'].iloc[-1]
        end_date = datetime.now().date()

        # Build portfolios
        hypothetical_equity = build_hypothetical_portfolio(events_df, klines_btc, klines_eth, end_date)
        actual_equity = build_actual_portfolio(binance_client, events_df, klines_btc, klines_eth, end_date, days)

        if hypothetical_equity.empty or actual_equity.empty:
            raise ValueError("Failed to build equity curves")

        # Calculate metrics
        metrics, merged = calculate_metrics(actual_equity, hypothetical_equity, total_invested)

        # Generate unique ID for this report
        report_id = str(uuid.uuid4())[:8]

        # Save CSV files
        equity_csv_path = csv_dir / f"portfolio_equity_{report_id}.csv"
        merged.to_csv(equity_csv_path, index=False)
        logger.info(f"Saved equity curves CSV: {equity_csv_path.name}")

        events_csv_path = csv_dir / f"cash_flow_events_{report_id}.csv"
        events_df.to_csv(events_csv_path, index=False)
        logger.info(f"Saved cash flow events CSV: {events_csv_path.name}")

        # Convert metrics to CSV format
        metrics_records = [
            {'metric': 'total_invested', 'value': metrics['total_invested']},
            {'metric': 'actual_final', 'value': metrics['actual']['final']},
            {'metric': 'actual_return_pct', 'value': metrics['actual']['return_pct']},
            {'metric': 'actual_profit_loss', 'value': metrics['actual']['profit_loss']},
            {'metric': 'actual_max_drawdown_pct', 'value': metrics['actual']['max_drawdown_pct']},
            {'metric': 'hypothetical_final', 'value': metrics['hypothetical']['final']},
            {'metric': 'hypothetical_return_pct', 'value': metrics['hypothetical']['return_pct']},
            {'metric': 'hypothetical_profit_loss', 'value': metrics['hypothetical']['profit_loss']},
            {'metric': 'hypothetical_max_drawdown_pct', 'value': metrics['hypothetical']['max_drawdown_pct']},
            {'metric': 'outperformance_pct', 'value': metrics['actual']['return_pct'] - metrics['hypothetical']['return_pct']}
        ]
        metrics_df = pd.DataFrame(metrics_records)
        metrics_csv_path = csv_dir / f"portfolio_metrics_{report_id}.csv"
        metrics_df.to_csv(metrics_csv_path, index=False)
        logger.info(f"Saved metrics CSV: {metrics_csv_path.name}")

        # Generate visualization
        png_path = csv_dir / f"portfolio_chart_{report_id}.png"
        create_visualization(merged, metrics, png_path)

        # Build markdown report
        markdown_report = build_markdown_report(events_df, metrics)

        logger.info("Portfolio comparison analysis completed successfully")

        return {
            'equity_csv_path': equity_csv_path,
            'events_csv_path': events_csv_path,
            'metrics_csv_path': metrics_csv_path,
            'png_path': png_path,
            'markdown_report': markdown_report,
            'metrics': metrics,
            'merged_df': merged,
            'events_df': events_df
        }

    except Exception as e:
        logger.error(f"Error in portfolio comparison analysis: {e}")
        raise


# ============================================================================
# MCP TOOL REGISTRATION
# ============================================================================

def register_binance_portfolio_comparison(local_mcp_instance, local_binance_client, csv_dir):
    """Register the binance_portfolio_comparison tool"""

    @local_mcp_instance.tool()
    def binance_portfolio_comparison(days: int = 30) -> str:
        """
        Generate a comprehensive portfolio performance report comparing actual trading results
        against a hypothetical buy-and-hold strategy (33% BTC, 33% ETH, 33% USDT).

        This tool analyzes your trading performance using actual capitalization events from:
        - P2P trading history (BUY/SELL transactions)
        - Deposit history (crypto inflows)
        - Spot trading history

        It generates multiple outputs:
        1. Equity curves CSV (daily portfolio values)
        2. Cash flow events CSV (all P2P and deposit events)
        3. Performance metrics CSV (returns, P/L, drawdowns)
        4. Visualization PNG chart (equity curves comparison)
        5. Markdown-formatted performance report

        Parameters:
            days (int): Number of days to analyze (default: 30)

        Returns:
            str: Comprehensive report with all generated file information and markdown performance summary

        CSV Output Files:

        1. **Equity Curves CSV** (`portfolio_equity_*.csv`):
            - date (datetime): Date
            - actual (float): Actual portfolio value in USDT
            - hypothetical (float): Hypothetical buy-and-hold portfolio value in USDT

        2. **Cash Flow Events CSV** (`cash_flow_events_*.csv`):
            - timestamp (datetime): Event timestamp
            - timestamp_ms (int): Event timestamp in milliseconds
            - type (string): Event type (P2P_BUY, P2P_SELL, DEPOSIT)
            - asset (string): Asset symbol (BTC, ETH, USDT)
            - usd_value (float): USD value of the event (positive for inflows, negative for outflows)
            - crypto_amount (float): Cryptocurrency amount
            - description (string): Human-readable description
            - cumulative_invested (float): Cumulative capital invested up to this event

        3. **Performance Metrics CSV** (`portfolio_metrics_*.csv`):
            - metric (string): Metric name
            - value (float): Metric value

        4. **Visualization PNG** (`portfolio_chart_*.png`):
            - Chart comparing actual vs hypothetical portfolio equity curves
            - Includes performance metrics overlay
            - Accessible via web URL (temp PNG folder will be published on web)

        Performance Report:
            The tool returns a markdown-formatted report including:
            - Capital invested summary (P2P deposits, withdrawals, crypto deposits)
            - Performance metrics for both actual and hypothetical portfolios
            - Return percentages and profit/loss
            - Maximum drawdown analysis
            - Outperformance analysis (actual vs hypothetical)
            - Recent cash flow events timeline

        Use Cases:
            - Evaluate trading strategy performance vs passive holding
            - Analyze portfolio returns over time
            - Compare active trading results against buy-and-hold
            - Identify outperformance or underperformance
            - Review cash flow events and their impact on portfolio
            - Track maximum drawdowns and risk metrics

        Example usage:
            binance_portfolio_comparison(days=30)
            binance_portfolio_comparison(days=60)
            binance_portfolio_comparison(days=90)

        Note:
            - Requires historical data access (P2P, deposits, trades)
            - Analysis is limited to BTC, ETH, and USDT
            - Hypothetical portfolio uses 33/33/33 equal-weight allocation
            - All values are calculated in USDT
            - PNG chart files are saved to the same directory as CSV files for web publishing
        """
        logger.info(f"binance_portfolio_comparison tool invoked with days={days}")

        try:
            # Call the main fetch function
            result = fetch_portfolio_comparison(
                binance_client=local_binance_client,
                csv_dir=csv_dir,
                days=days
            )

            # Build comprehensive response
            response_parts = []

            # Add equity curves CSV info
            response_parts.append("## Portfolio Equity Curves\n")
            response_parts.append(format_csv_response(result['equity_csv_path'], result['merged_df']))

            # Add cash flow events CSV info
            response_parts.append("\n\n## Cash Flow Events\n")
            response_parts.append(format_csv_response(result['events_csv_path'], result['events_df']))

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
            response_parts.append(f"✓ Chart saved to PNG\n\n")
            response_parts.append(f"File: {result['png_path'].name}\n")
            response_parts.append(f"Size: {png_size_str}\n")
            response_parts.append(f"Location: data/mcp-binance/{result['png_path'].name}\n")
            response_parts.append(f"\nThe chart will be accessible via web once the temp PNG folder is published.\n")

            # Add markdown performance report
            response_parts.append("\n\n" + "="*80 + "\n")
            response_parts.append(result['markdown_report'])
            response_parts.append("\n" + "="*80)

            final_response = "".join(response_parts)

            logger.info(f"binance_portfolio_comparison completed successfully")

            return final_response

        except Exception as e:
            logger.error(f"Error in binance_portfolio_comparison tool: {e}")
            return f"Error generating portfolio comparison report: {str(e)}"
