"""
Futures Trades Visualization MCP Tool

Generates individual trade visualizations for futures positions showing
price action, entry/exit points, and position metrics.
"""

import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for server use
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
from binance.client import Client
from mcp.server.fastmcp import Image as MCPImage
from PIL import Image as PILImage

from mcp_image_utils import to_mcp_image
from mcp_service import format_csv_response
from request_logger import log_request
from sentry_utils import with_sentry_tracing

logger = logging.getLogger(__name__)

# ============================================================================
# COLOR CONSTANTS
# ============================================================================

COLORS = {
    "profit": "#00C853",
    "loss": "#FF5252",
    "neutral": "#FFC107",
    "primary": "#2196F3",
    "secondary": "#9C27B0",
    "background": "#1a1a2e",
    "text": "#ffffff",
    "grid": "#333355",
}

ASSET_COLORS = {
    "BTC": "#F7931A", "ETH": "#627EEA", "SOL": "#00FFA3",
    "XRP": "#23292F", "DOGE": "#C2A633", "SUI": "#6FBCF0",
    "BNB": "#F3BA2F", "TRX": "#FF0013", "AR": "#222326",
    "VET": "#15BDFF", "SAND": "#00ADEF", "GRT": "#6747ED",
    "MANA": "#FF2D55", "EGLD": "#23F7DD", "XMR": "#FF6600",
    "RPL": "#FF7043", "AVAX": "#E84142", "LINK": "#2A5ADA",
    "ADA": "#0033AD", "DOT": "#E6007A", "MATIC": "#8247E5",
    "ATOM": "#2E3148", "UNI": "#FF007A", "LTC": "#BFBBBB",
}

DEFAULT_FUTURES_SYMBOLS = [
    "BTC", "ETH", "SOL", "XRP", "DOGE", "SUI", "BNB", "TRX",
    "AR", "VET", "SAND", "GRT", "MANA", "EGLD", "XMR", "RPL",
    "AVAX", "LINK", "ADA", "DOT", "MATIC", "ATOM", "UNI", "LTC"
]

# Visualization settings
FIGURE_SIZE = (14, 8)
DPI = 100


# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class TradeVisualization:
    """Container for trade visualization data"""
    trade_index: int
    asset: str
    symbol: str
    position_side: str
    entry_time: datetime
    entry_price: float
    exit_time: Optional[datetime]
    exit_price: Optional[float]
    realized_pnl: Optional[float]
    unrealized_pnl: Optional[float]
    lifetime_minutes: Optional[float]
    is_open: bool
    quantity: float
    tp_price: Optional[float] = None
    sl_price: Optional[float] = None


# ============================================================================
# DATA FETCHING FUNCTIONS
# ============================================================================

@with_sentry_tracing("fetch_all_futures_trades")
def fetch_all_futures_trades(binance_client: Client, symbols: List[str], days: int = 30) -> pd.DataFrame:
    """
    Fetch all futures trades for given symbols within the lookback period.

    Args:
        binance_client: Initialized Binance Client
        symbols: List of base asset symbols (e.g., ['BTC', 'ETH'])
        days: Number of days to look back

    Returns:
        DataFrame with all trade records
    """
    logger.info(f"Fetching futures trades for {len(symbols)} symbols, last {days} days")

    cutoff_time = datetime.now() - timedelta(days=days)
    cutoff_ms = int(cutoff_time.timestamp() * 1000)

    all_records = []

    for symbol in symbols:
        futures_symbol = f"{symbol}USDT"
        try:
            trades = binance_client.futures_account_trades(symbol=futures_symbol, limit=1000)

            for trade in trades:
                trade_time_ms = int(trade['time'])
                if trade_time_ms < cutoff_ms:
                    continue

                all_records.append({
                    'id': trade['id'],
                    'orderId': trade['orderId'],
                    'symbol': trade['symbol'],
                    'asset': symbol,
                    'side': trade['side'],
                    'positionSide': trade['positionSide'],
                    'price': float(trade['price']),
                    'qty': float(trade['qty']),
                    'quoteQty': float(trade['quoteQty']),
                    'realizedPnl': float(trade['realizedPnl']),
                    'commission': float(trade['commission']),
                    'commissionAsset': trade['commissionAsset'],
                    'time': datetime.fromtimestamp(trade_time_ms / 1000),
                    'time_ms': trade_time_ms,
                    'buyer': trade['buyer'],
                    'maker': trade['maker'],
                })

        except Exception as e:
            logger.warning(f"Error fetching trades for {futures_symbol}: {e}")
            continue

    if not all_records:
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    df = df.sort_values('time').reset_index(drop=True)

    logger.info(f"Retrieved {len(df)} total futures trades")
    return df


@with_sentry_tracing("fetch_conditional_orders_for_symbol")
def fetch_conditional_orders_for_symbol(binance_client: Client, symbol: str) -> Dict[str, Optional[float]]:
    """
    Fetch TP/SL conditional orders for a specific symbol.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'MANAUSDT')

    Returns:
        Dictionary with 'tp_price' and 'sl_price' (None if not set)
    """
    logger.info(f"Fetching conditional orders for {symbol}")

    result = {'tp_price': None, 'sl_price': None}

    try:
        # Use the Algo Service endpoint (same as get_futures_conditional_orders)
        orders = binance_client._request_futures_api('get', 'openAlgoOrders', signed=True, data={'symbol': symbol})

        for order in orders:
            order_type = order.get('orderType', '')
            trigger_price = float(order.get('triggerPrice', 0))

            if trigger_price > 0:
                if 'TAKE_PROFIT' in order_type:
                    result['tp_price'] = trigger_price
                elif 'STOP' in order_type and 'TRAILING' not in order_type:
                    result['sl_price'] = trigger_price

        logger.info(f"Conditional orders for {symbol}: TP={result['tp_price']}, SL={result['sl_price']}")
        return result

    except Exception as e:
        logger.warning(f"Error fetching conditional orders for {symbol}: {e}")
        return result


@with_sentry_tracing("load_open_positions")
def load_open_positions(binance_client: Client) -> pd.DataFrame:
    """
    Load all open futures positions.

    Returns:
        DataFrame with open position details
    """
    logger.info("Loading open futures positions")

    try:
        positions = binance_client.futures_position_information()

        records = []
        for pos in positions:
            pos_amt = float(pos.get('positionAmt', 0))
            if pos_amt == 0:
                continue

            symbol = pos.get('symbol', '')
            asset = symbol.replace('USDT', '') if symbol.endswith('USDT') else symbol

            records.append({
                'symbol': symbol,
                'asset': asset,
                'positionSide': pos.get('positionSide', 'BOTH'),
                'positionAmt': pos_amt,
                'entryPrice': float(pos.get('entryPrice', 0)),
                'markPrice': float(pos.get('markPrice', 0)),
                'unRealizedProfit': float(pos.get('unRealizedProfit', 0)),
                'leverage': int(pos.get('leverage', 1)),
                'liquidationPrice': float(pos.get('liquidationPrice', 0)) if pos.get('liquidationPrice') else None,
                'updateTime': datetime.fromtimestamp(int(pos.get('updateTime', 0)) / 1000) if pos.get('updateTime') else None,
            })

        df = pd.DataFrame(records) if records else pd.DataFrame()
        logger.info(f"Found {len(df)} open positions")
        return df

    except Exception as e:
        logger.error(f"Error loading open positions: {e}")
        return pd.DataFrame()


def compute_closed_trades(trades_df: pd.DataFrame) -> List[TradeVisualization]:
    """
    Compute closed trades by replaying trade history with position tracking.

    For each (symbol, positionSide), tracks:
    - entry_time: timestamp of first opening trade
    - qty: cumulative quantity

    Opening trades: (LONG + BUY) or (SHORT + SELL)
    Closing trades: (LONG + SELL) or (SHORT + BUY) with realizedPnl != 0

    Returns:
        List of TradeVisualization objects for closed trades
    """
    if trades_df.empty:
        return []

    closed_trades = []

    # Group by symbol and positionSide
    for (symbol, pos_side), group in trades_df.groupby(['symbol', 'positionSide']):
        group = group.sort_values('time').reset_index(drop=True)
        asset = symbol.replace('USDT', '') if symbol.endswith('USDT') else symbol

        position_qty = 0.0
        entry_time = None
        entry_prices = []  # Track entry prices for weighted average
        entry_qtys = []
        total_realized_pnl = 0.0

        for _, trade in group.iterrows():
            side = trade['side']
            qty = trade['qty']
            price = trade['price']
            realized_pnl = trade['realizedPnl']
            trade_time = trade['time']

            # Determine if opening or closing
            is_opening = (
                (pos_side in ['LONG', 'BOTH'] and side == 'BUY') or
                (pos_side == 'SHORT' and side == 'SELL')
            )

            is_closing = (
                (pos_side in ['LONG', 'BOTH'] and side == 'SELL') or
                (pos_side == 'SHORT' and side == 'BUY')
            )

            if is_opening:
                if position_qty == 0:
                    entry_time = trade_time
                    entry_prices = []
                    entry_qtys = []
                    total_realized_pnl = 0.0

                position_qty += qty
                entry_prices.append(price)
                entry_qtys.append(qty)

            elif is_closing:
                position_qty -= qty
                total_realized_pnl += realized_pnl

                # Position fully closed
                if position_qty <= 0.001:  # Allow for floating point errors
                    if entry_time and entry_prices:
                        # Calculate weighted average entry price
                        total_qty = sum(entry_qtys)
                        avg_entry = sum(p * q for p, q in zip(entry_prices, entry_qtys)) / total_qty if total_qty > 0 else price

                        # Determine direction for display
                        direction = 'LONG' if pos_side in ['LONG', 'BOTH'] else 'SHORT'
                        if pos_side == 'BOTH':
                            direction = 'LONG' if entry_prices[0] < price else 'SHORT'

                        lifetime_minutes = (trade_time - entry_time).total_seconds() / 60.0

                        closed_trades.append(TradeVisualization(
                            trade_index=len(closed_trades),
                            asset=asset,
                            symbol=symbol,
                            position_side=direction,
                            entry_time=entry_time,
                            entry_price=avg_entry,
                            exit_time=trade_time,
                            exit_price=price,
                            realized_pnl=total_realized_pnl,
                            unrealized_pnl=None,
                            lifetime_minutes=lifetime_minutes,
                            is_open=False,
                            quantity=total_qty,
                        ))

                    # Reset tracking
                    position_qty = 0.0
                    entry_time = None
                    entry_prices = []
                    entry_qtys = []
                    total_realized_pnl = 0.0

    # Sort by exit time (most recent first)
    closed_trades.sort(key=lambda x: x.exit_time or datetime.min, reverse=True)

    # Re-index
    for i, trade in enumerate(closed_trades):
        trade.trade_index = i

    return closed_trades


def build_open_trade_visualizations(open_positions_df: pd.DataFrame, trades_df: pd.DataFrame) -> List[TradeVisualization]:
    """
    Build TradeVisualization objects for open positions.

    Args:
        open_positions_df: DataFrame of open positions
        trades_df: DataFrame of all trades (to find entry time)

    Returns:
        List of TradeVisualization objects for open positions
    """
    if open_positions_df.empty:
        return []

    open_trades = []

    for _, pos in open_positions_df.iterrows():
        symbol = pos['symbol']
        asset = pos['asset']
        pos_side = pos['positionSide']
        pos_amt = pos['positionAmt']
        entry_price = pos['entryPrice']
        unrealized_pnl = pos['unRealizedProfit']

        # Determine direction
        direction = 'LONG' if pos_amt > 0 else 'SHORT'

        # Try to find entry time from trades
        entry_time = None
        if not trades_df.empty:
            symbol_trades = trades_df[
                (trades_df['symbol'] == symbol) &
                (trades_df['positionSide'] == pos_side)
            ].sort_values('time')

            if not symbol_trades.empty:
                # Find first opening trade for this position
                # This is approximate - actual tracking would need full position replay
                entry_time = symbol_trades.iloc[0]['time']

        if entry_time is None:
            entry_time = pos.get('updateTime') or datetime.now()

        open_trades.append(TradeVisualization(
            trade_index=len(open_trades),
            asset=asset,
            symbol=symbol,
            position_side=direction,
            entry_time=entry_time,
            entry_price=entry_price,
            exit_time=None,
            exit_price=None,
            realized_pnl=None,
            unrealized_pnl=unrealized_pnl,
            lifetime_minutes=None,
            is_open=True,
            quantity=abs(pos_amt),
        ))

    return open_trades


@with_sentry_tracing("fetch_klines_for_trade")
def fetch_klines_for_trade(binance_client: Client, trade: TradeVisualization, padding_hours: float = 1.0) -> pd.DataFrame:
    """
    Fetch klines data for a specific trade with padding.

    Args:
        binance_client: Initialized Binance Client
        trade: TradeVisualization object
        padding_hours: Hours to add before entry and after exit

    Returns:
        DataFrame with OHLCV data
    """
    symbol = trade.symbol

    # Calculate time range
    start_time = trade.entry_time - timedelta(hours=padding_hours)

    if trade.is_open:
        end_time = datetime.now() + timedelta(minutes=30)
    else:
        end_time = trade.exit_time + timedelta(minutes=30)

    # Determine appropriate interval based on trade duration
    duration_hours = (end_time - start_time).total_seconds() / 3600

    if duration_hours <= 4:
        interval = '1m'
    elif duration_hours <= 24:
        interval = '5m'
    elif duration_hours <= 72:
        interval = '15m'
    elif duration_hours <= 168:  # 1 week
        interval = '1h'
    else:
        interval = '4h'

    logger.info(f"Fetching {interval} klines for {symbol} from {start_time} to {end_time}")

    try:
        klines = binance_client.futures_klines(
            symbol=symbol,
            interval=interval,
            startTime=int(start_time.timestamp() * 1000),
            endTime=int(end_time.timestamp() * 1000),
            limit=1000
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
                'volume': float(kline[5]),
                'close_time_ms': int(kline[6]),
                'quote_volume': float(kline[7]),
                'trades': int(kline[8]),
                'taker_buy_volume': float(kline[9]),
                'taker_buy_quote_volume': float(kline[10]),
            })

        df = pd.DataFrame(records)
        logger.info(f"Retrieved {len(df)} klines for {symbol}")
        return df

    except Exception as e:
        logger.error(f"Error fetching klines for {symbol}: {e}")
        return pd.DataFrame()


# ============================================================================
# PLOTTING FUNCTIONS
# ============================================================================

def plot_single_trade(
    trade: TradeVisualization,
    klines_df: pd.DataFrame,
    output_path: Path,
    tp_price: Optional[float] = None,
    sl_price: Optional[float] = None
) -> None:
    """
    Generate a visualization plot for a single trade.

    Args:
        trade: TradeVisualization object
        klines_df: DataFrame with OHLCV data
        output_path: Path to save the PNG file
        tp_price: Take profit price level (optional)
        sl_price: Stop loss price level (optional)
    """
    logger.info(f"Generating plot for {trade.asset} {trade.position_side} trade")

    if klines_df.empty:
        logger.warning("No klines data available for plotting")
        return

    # Get asset color
    asset_color = ASSET_COLORS.get(trade.asset, COLORS['loss'])  # Default to red/loss color for price line

    # Create figure with dark theme
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=FIGURE_SIZE, dpi=DPI,
                                     gridspec_kw={'height_ratios': [3, 1]},
                                     sharex=True)
    fig.patch.set_facecolor(COLORS['background'])

    for ax in [ax1, ax2]:
        ax.set_facecolor(COLORS['background'])
        ax.tick_params(colors=COLORS['text'], labelsize=9)
        ax.xaxis.label.set_color(COLORS['text'])
        ax.yaxis.label.set_color(COLORS['text'])
        ax.title.set_color(COLORS['text'])
        for spine in ax.spines.values():
            spine.set_color(COLORS['grid'])

    # Determine P&L color for the trade
    pnl_value = trade.realized_pnl if not trade.is_open else trade.unrealized_pnl
    pnl_color = COLORS['profit'] if (pnl_value or 0) >= 0 else COLORS['loss']

    # Plot price line with asset color
    ax1.plot(klines_df['timestamp'], klines_df['close'],
             color=asset_color, linewidth=1.5, label=f'{trade.asset} Price')

    # Fill area between entry price and price line (shows profit/loss region)
    # For LONG: profit when price > entry, loss when price < entry
    # For SHORT: profit when price < entry, loss when price > entry
    ax1.fill_between(
        klines_df['timestamp'],
        trade.entry_price,
        klines_df['close'],
        alpha=0.3,
        color=pnl_color,
        label='P&L Region'
    )

    # Plot entry horizontal line
    entry_label = f"Entry: ${trade.entry_price:,.4f}"
    ax1.axhline(y=trade.entry_price, color=COLORS['primary'], linestyle='--',
                alpha=0.8, linewidth=2, label=entry_label)

    # Plot entry VERTICAL line at entry time
    ax1.axvline(x=trade.entry_time, color=COLORS['primary'], linestyle=':',
                alpha=0.6, linewidth=1.5)

    # Plot entry marker
    ax1.scatter([trade.entry_time], [trade.entry_price], color=COLORS['primary'],
                s=150, marker='^' if trade.position_side == 'LONG' else 'v',
                zorder=5, edgecolors='white', linewidths=2)

    # Plot Take Profit level if available
    if tp_price and tp_price > 0:
        ax1.axhline(y=tp_price, color=COLORS['profit'], linestyle='-.',
                    alpha=0.8, linewidth=1.5, label=f"TP: ${tp_price:,.4f}")

    # Plot Stop Loss level if available
    if sl_price and sl_price > 0:
        ax1.axhline(y=sl_price, color=COLORS['loss'], linestyle='-.',
                    alpha=0.8, linewidth=1.5, label=f"SL: ${sl_price:,.4f}")

    # Plot exit marker (if closed)
    if not trade.is_open and trade.exit_time and trade.exit_price:
        exit_color = COLORS['profit'] if (trade.realized_pnl or 0) >= 0 else COLORS['loss']
        exit_label = f"Exit: ${trade.exit_price:,.4f}"
        ax1.axhline(y=trade.exit_price, color=exit_color, linestyle='--',
                    alpha=0.7, linewidth=1.5, label=exit_label)
        ax1.scatter([trade.exit_time], [trade.exit_price], color=exit_color,
                    s=150, marker='x', zorder=5, linewidths=3)

        # Add exit vertical line
        ax1.axvline(x=trade.exit_time, color=exit_color, linestyle=':',
                    alpha=0.6, linewidth=1.5)
    else:
        # For open trades, show current price line
        current_price = klines_df['close'].iloc[-1]
        ax1.axhline(y=current_price, color=pnl_color, linestyle=':',
                    alpha=0.7, linewidth=1.5, label=f"Current: ${current_price:,.4f}")

    # Configure price axis
    ax1.set_ylabel('Price (USDT)', fontsize=12, fontweight='bold', color=COLORS['text'])
    ax1.legend(loc='upper left', fontsize=9, facecolor=COLORS['background'],
               edgecolor=COLORS['grid'], labelcolor=COLORS['text'])
    ax1.grid(True, alpha=0.3, color=COLORS['grid'], linestyle=':')
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.4f}'))

    # Add position badge in upper right
    badge_color = COLORS['loss'] if trade.position_side == 'SHORT' else COLORS['profit']
    badge_text = f"[{trade.position_side}]"
    ax1.text(0.98, 0.92, badge_text, transform=ax1.transAxes, fontsize=14,
             fontweight='bold', verticalalignment='top', horizontalalignment='right',
             color=badge_color,
             bbox=dict(boxstyle='round,pad=0.3', facecolor=COLORS['background'],
                       edgecolor=badge_color, alpha=0.9))

    # Plot volume with taker buy/sell separation (cleaner style like original)
    if 'taker_buy_volume' in klines_df.columns:
        # Calculate taker sell volume
        taker_sell_volume = klines_df['volume'] - klines_df['taker_buy_volume']

        # Calculate bar width based on time interval
        if len(klines_df) > 1:
            time_diff = (klines_df['timestamp'].iloc[1] - klines_df['timestamp'].iloc[0]).total_seconds()
            bar_width = timedelta(seconds=time_diff * 0.8)
        else:
            bar_width = timedelta(minutes=5)

        # Plot taker buy volume (green)
        ax2.bar(klines_df['timestamp'], klines_df['taker_buy_volume'],
                color=COLORS['profit'], alpha=0.7, width=bar_width, label='Taker Buy')

        # Plot taker sell volume (red) stacked on top
        ax2.bar(klines_df['timestamp'], taker_sell_volume,
                bottom=klines_df['taker_buy_volume'],
                color=COLORS['loss'], alpha=0.7, width=bar_width, label='Taker Sell')

        ax2.legend(loc='upper left', fontsize=8, facecolor=COLORS['background'],
                   edgecolor=COLORS['grid'], labelcolor=COLORS['text'])
    else:
        # Fallback: simple volume bars
        colors = [COLORS['profit'] if klines_df['close'].iloc[i] >= klines_df['open'].iloc[i]
                  else COLORS['loss'] for i in range(len(klines_df))]
        if len(klines_df) > 1:
            time_diff = (klines_df['timestamp'].iloc[1] - klines_df['timestamp'].iloc[0]).total_seconds()
            bar_width = timedelta(seconds=time_diff * 0.8)
        else:
            bar_width = timedelta(minutes=5)
        ax2.bar(klines_df['timestamp'], klines_df['volume'], color=colors, alpha=0.6, width=bar_width)

    ax2.set_ylabel('Volume (USDT)', fontsize=10, color=COLORS['text'])
    ax2.grid(True, alpha=0.3, color=COLORS['grid'], linestyle=':')

    # Format x-axis with rotated datetime labels
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    ax2.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right', fontsize=8)

    # Build title with trade info
    if trade.is_open:
        pnl_str = f"Unrealized: ${trade.unrealized_pnl or 0:+,.2f}"
        status_str = "OPEN"
        pnl_pct = ""
        if trade.entry_price > 0 and trade.unrealized_pnl is not None:
            notional = trade.quantity * trade.entry_price
            if notional > 0:
                pnl_pct = f" ({(trade.unrealized_pnl / notional * 100):+.2f}%)"
    else:
        pnl_str = f"Realized: ${trade.realized_pnl or 0:+,.2f}"
        lifetime_str = ""
        if trade.lifetime_minutes:
            if trade.lifetime_minutes < 60:
                lifetime_str = f" ({trade.lifetime_minutes:.0f}m)"
            elif trade.lifetime_minutes < 1440:
                lifetime_str = f" ({trade.lifetime_minutes/60:.1f}h)"
            else:
                lifetime_str = f" ({trade.lifetime_minutes/1440:.1f}d)"
        status_str = f"CLOSED{lifetime_str}"
        pnl_pct = ""
        if trade.entry_price > 0 and trade.realized_pnl is not None:
            notional = trade.quantity * trade.entry_price
            if notional > 0:
                pnl_pct = f" ({(trade.realized_pnl / notional * 100):+.2f}%)"

    # Format entry time for title
    entry_time_str = trade.entry_time.strftime('%Y-%m-%d %H:%M') + " UTC"
    current_price = klines_df['close'].iloc[-1]

    title = f"{trade.asset} {trade.position_side} Trade: {entry_time_str} - {status_str}\n"
    title += f"Entry: ${trade.entry_price:,.4f}  |  {pnl_str}{pnl_pct}  |  Current: ${current_price:,.4f}"
    ax1.set_title(title, fontsize=12, fontweight='bold', color=COLORS['text'], pad=15)

    # Add trade info box in upper right (below badge)
    info_text = (
        f"Entry: {trade.entry_time.strftime('%Y-%m-%d %H:%M')}\n"
        f"Entry Price: ${trade.entry_price:,.4f}\n"
        f"Quantity: {trade.quantity:.4f}\n"
    )
    if tp_price:
        info_text += f"TP: ${tp_price:,.4f}\n"
    if sl_price:
        info_text += f"SL: ${sl_price:,.4f}\n"
    if not trade.is_open:
        info_text += f"Exit: {trade.exit_time.strftime('%Y-%m-%d %H:%M')}\n"
        info_text += f"Exit Price: ${trade.exit_price:,.4f}\n"

    props = dict(boxstyle='round', facecolor=COLORS['background'], alpha=0.9,
                 edgecolor=COLORS['grid'])
    ax1.text(0.98, 0.78, info_text, transform=ax1.transAxes, fontsize=8,
             verticalalignment='top', horizontalalignment='right',
             bbox=props, family='monospace', color=COLORS['text'])

    plt.tight_layout()
    plt.savefig(output_path, dpi=DPI, bbox_inches='tight',
                facecolor=COLORS['background'], edgecolor='none')
    plt.close(fig)

    logger.info(f"Saved trade visualization: {output_path.name}")


# ============================================================================
# MAIN FETCH FUNCTION
# ============================================================================

@with_sentry_tracing("binance_visualize_futures_trades")
def fetch_visualize_futures_trades(
    binance_client: Client,
    csv_dir: Path,
    days: int = 30,
    asset: Optional[str] = None,
    open_only: bool = False,
    trade_index: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Generate futures trade visualizations.

    Args:
        binance_client: Initialized Binance Client
        csv_dir: Directory to save output files
        days: Lookback period in days
        asset: Filter to specific asset (e.g., 'XMR')
        open_only: Only visualize open positions
        trade_index: Specific trade index to visualize

    Returns:
        Dictionary with visualization results
    """
    logger.info(f"Starting futures trades visualization (days={days}, asset={asset}, open_only={open_only})")

    # Determine symbols to fetch
    if asset:
        symbols = [asset.upper()]
    else:
        symbols = DEFAULT_FUTURES_SYMBOLS

    # Fetch all trades
    trades_df = fetch_all_futures_trades(binance_client, symbols, days=days)

    # Load open positions
    open_positions_df = load_open_positions(binance_client)

    # Filter open positions by asset if specified
    if asset and not open_positions_df.empty:
        open_positions_df = open_positions_df[open_positions_df['asset'] == asset.upper()]

    # Build trade visualizations
    all_trades: List[TradeVisualization] = []

    # Add open positions
    open_trades = build_open_trade_visualizations(open_positions_df, trades_df)

    if not open_only:
        # Add closed trades
        closed_trades = compute_closed_trades(trades_df)

        # Filter by asset if specified
        if asset:
            closed_trades = [t for t in closed_trades if t.asset == asset.upper()]

        # Combine: open trades first, then closed trades
        all_trades = open_trades + closed_trades
    else:
        all_trades = open_trades

    # Re-index all trades
    for i, trade in enumerate(all_trades):
        trade.trade_index = i

    # Generate unique ID for this report
    report_id = str(uuid.uuid4())[:8]

    # Build trades summary DataFrame
    trades_records = []
    for trade in all_trades:
        trades_records.append({
            'trade_index': trade.trade_index,
            'asset': trade.asset,
            'symbol': trade.symbol,
            'position_side': trade.position_side,
            'entry_time': trade.entry_time.strftime('%Y-%m-%d %H:%M:%S'),
            'entry_price': trade.entry_price,
            'exit_time': trade.exit_time.strftime('%Y-%m-%d %H:%M:%S') if trade.exit_time else None,
            'exit_price': trade.exit_price,
            'quantity': trade.quantity,
            'realized_pnl': trade.realized_pnl,
            'unrealized_pnl': trade.unrealized_pnl,
            'lifetime_minutes': trade.lifetime_minutes,
            'is_open': trade.is_open,
        })

    trades_summary_df = pd.DataFrame(trades_records) if trades_records else pd.DataFrame(columns=[
        'trade_index', 'asset', 'symbol', 'position_side', 'entry_time', 'entry_price',
        'exit_time', 'exit_price', 'quantity', 'realized_pnl', 'unrealized_pnl',
        'lifetime_minutes', 'is_open'
    ])

    # Save trades summary CSV
    trades_csv_path = csv_dir / f"futures_trades_{report_id}.csv"
    trades_summary_df.to_csv(trades_csv_path, index=False)
    logger.info(f"Saved trades summary to {trades_csv_path.name}")

    # Determine which trade to visualize
    if not all_trades:
        return {
            'trades_csv_path': trades_csv_path,
            'trades_df': trades_summary_df,
            'klines_csv_path': None,
            'klines_df': None,
            'png_path': None,
            'visualized_trade': None,
            'all_trades': all_trades,
        }

    # Select trade to visualize
    if trade_index is not None:
        if trade_index < 0 or trade_index >= len(all_trades):
            raise ValueError(f"trade_index {trade_index} out of range (0-{len(all_trades)-1})")
        selected_trade = all_trades[trade_index]
    else:
        # Default: most recent trade (first in list after sorting)
        selected_trade = all_trades[0]

    # Fetch klines for the selected trade
    klines_df = fetch_klines_for_trade(binance_client, selected_trade)

    # Save klines CSV
    klines_csv_path = None
    if not klines_df.empty:
        klines_csv_path = csv_dir / f"futures_klines_{selected_trade.asset}_{report_id}.csv"
        klines_df.to_csv(klines_csv_path, index=False)
        logger.info(f"Saved klines to {klines_csv_path.name}")

    # Fetch conditional orders (TP/SL) for open positions
    tp_price = None
    sl_price = None
    if selected_trade.is_open:
        conditional_orders = fetch_conditional_orders_for_symbol(binance_client, selected_trade.symbol)
        tp_price = conditional_orders.get('tp_price')
        sl_price = conditional_orders.get('sl_price')

    # Generate visualization
    png_path = None
    if not klines_df.empty:
        entry_date_str = selected_trade.entry_time.strftime('%Y%m%d')
        png_path = csv_dir / f"trade_viz_{selected_trade.asset}_{entry_date_str}_{report_id}.png"
        plot_single_trade(selected_trade, klines_df, png_path, tp_price=tp_price, sl_price=sl_price)

    return {
        'trades_csv_path': trades_csv_path,
        'trades_df': trades_summary_df,
        'klines_csv_path': klines_csv_path,
        'klines_df': klines_df,
        'png_path': png_path,
        'visualized_trade': selected_trade,
        'all_trades': all_trades,
    }


# ============================================================================
# MCP TOOL REGISTRATION
# ============================================================================

def register_binance_visualize_futures_trades(local_mcp_instance, local_binance_client, csv_dir, requests_dir):
    """Register the binance_visualize_futures_trades tool"""

    @local_mcp_instance.tool()
    def binance_visualize_futures_trades(
        requester: str,
        days: int = 30,
        asset: Optional[str] = None,
        open_only: bool = False,
        trade_index: Optional[int] = None,
    ) -> list[Any]:
        """
        Generate visualizations for futures trades showing price action, entry/exit points, and P&L.

        This tool creates individual trade visualizations with price charts, entry/exit markers,
        volume data, and performance metrics. It also saves trade data to CSV files for further
        analysis.

        ✓ READ-ONLY OPERATION - Safe to run anytime

        Parameters:
            requester (str, required): Name/ID of the requester for logging purposes
            days (int, optional): Lookback period in days (default: 30)
            asset (str, optional): Filter to specific asset (e.g., 'XMR', 'BTC')
                - If provided: Only shows trades for this asset
                - If omitted: Shows trades for all tracked assets
            open_only (bool, optional): Only visualize open positions (default: False)
            trade_index (int, optional): Visualize specific trade by index from the CSV
                - If provided: Visualizes the trade at this index
                - If omitted: Visualizes the most recent trade

        Returns:
            list: [ImageContent, str] - Trade visualization chart followed by detailed report

        CSV Output Files:

        1. **Trades Summary** (`futures_trades_*.csv`):
            - trade_index (integer): Index for use with trade_index parameter
            - asset (string): Base symbol (e.g., 'XMR', 'BTC')
            - symbol (string): Full trading pair (e.g., 'XMRUSDT')
            - position_side (string): 'LONG' or 'SHORT'
            - entry_time (datetime): Position entry timestamp
            - entry_price (float): Entry price
            - exit_time (datetime): Exit timestamp (null for open positions)
            - exit_price (float): Exit price (null for open positions)
            - quantity (float): Position size
            - realized_pnl (float): Realized P&L (null for open positions)
            - unrealized_pnl (float): Unrealized P&L (for open positions)
            - lifetime_minutes (float): Trade duration in minutes (null for open)
            - is_open (boolean): True if currently open

        2. **Klines Data** (`futures_klines_*_*.csv`):
            - timestamp (datetime): Candle timestamp
            - open (float): Open price
            - high (float): High price
            - low (float): Low price
            - close (float): Close price
            - volume (float): Base asset volume
            - quote_volume (float): Quote asset (USDT) volume

        3. **Visualization Chart** (`trade_viz_*_*.png`):
            - Price chart with entry/exit markers
            - Volume subplot
            - Trade metrics and P&L display

        Use Cases:
            - Review recent trade performance with visual context
            - Analyze entry/exit timing against price action
            - Share trade visualizations
            - Document trading history
            - Identify patterns in winning/losing trades

        Example usage:
            # Visualize most recent trade (all assets)
            binance_visualize_futures_trades()

            # Visualize most recent XMR trade
            binance_visualize_futures_trades(asset="XMR")

            # Only show open positions
            binance_visualize_futures_trades(open_only=True)

            # Visualize specific trade from CSV
            binance_visualize_futures_trades(trade_index=3)

            # Last 7 days of trading
            binance_visualize_futures_trades(days=7)

        Multi-Trade Workflow:
            1. First call without trade_index to get CSV with all trades
            2. Review trades in the CSV file
            3. Call again with specific trade_index to visualize other trades

        Analysis with py_eval:
            ```python
            df = pd.read_csv('data/mcp-binance/futures_trades_*.csv')

            # Total P&L from closed trades
            total_pnl = df[df['is_open'] == False]['realized_pnl'].sum()

            # Win rate
            wins = df[(df['is_open'] == False) & (df['realized_pnl'] > 0)]
            win_rate = len(wins) / len(df[df['is_open'] == False]) * 100

            # Average trade duration
            avg_duration = df['lifetime_minutes'].mean()
            ```

        Note:
            - Completely safe READ-ONLY operation
            - Chart is displayed inline and saved as PNG
            - Download URL provided for sharing
            - All trade data saved to CSV for analysis
        """
        logger.info(f"binance_visualize_futures_trades tool invoked by {requester}")

        try:
            result = fetch_visualize_futures_trades(
                binance_client=local_binance_client,
                csv_dir=csv_dir,
                days=days,
                asset=asset,
                open_only=open_only,
                trade_index=trade_index,
            )

            # Build response
            response_parts = []
            response_parts.append("═" * 79)
            response_parts.append("\nFUTURES TRADES VISUALIZATION\n")
            response_parts.append("═" * 79)

            # Add trades CSV info
            response_parts.append("\n\n## CSV 1: Trades Summary\n\n")
            response_parts.append(format_csv_response(result['trades_csv_path'], result['trades_df']))

            # Add klines CSV info (if available)
            if result['klines_csv_path'] and result['klines_df'] is not None and not result['klines_df'].empty:
                response_parts.append("\n\n## CSV 2: Plot Data (Klines)\n\n")
                response_parts.append(format_csv_response(result['klines_csv_path'], result['klines_df']))

            # Add PNG info (if available)
            image_content = None
            if result['png_path'] and result['png_path'].exists():
                png_size_bytes = result['png_path'].stat().st_size
                if png_size_bytes < 1024:
                    png_size_str = f"{png_size_bytes} bytes"
                elif png_size_bytes < 1024 * 1024:
                    png_size_str = f"{png_size_bytes / 1024:.1f} KB"
                else:
                    png_size_str = f"{png_size_bytes / (1024 * 1024):.1f} MB"

                response_parts.append(f"\n\n## Visualization Chart\n\n")
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

                # Load PNG for inline display
                with PILImage.open(result['png_path']) as full_image:
                    preview_image = full_image.copy()

                    # Resize for optimal response size
                    max_width = 1200
                    max_height = 900
                    preview_image.thumbnail((max_width, max_height), resample=PILImage.Resampling.LANCZOS)

                    # Convert to RGB for JPEG
                    if preview_image.mode not in ("RGB", "L"):
                        preview_image = preview_image.convert("RGB")

                    mcp_image = to_mcp_image(
                        preview_image,
                        format="jpeg",
                        quality=85,
                        optimize=True
                    )
                    image_content = mcp_image.to_image_content()

            # Add summary
            response_parts.append("\n\n" + "═" * 79)
            response_parts.append("\nSUMMARY\n")
            response_parts.append("═" * 79 + "\n\n")

            all_trades = result['all_trades']
            visualized_trade = result['visualized_trade']

            if visualized_trade:
                pnl = visualized_trade.realized_pnl if not visualized_trade.is_open else visualized_trade.unrealized_pnl
                pnl_type = "Realized" if not visualized_trade.is_open else "Unrealized"
                status = "OPEN" if visualized_trade.is_open else "CLOSED"

                response_parts.append(f"Visualized Trade: {visualized_trade.asset} {visualized_trade.position_side} ")
                response_parts.append(f"(trade_index={visualized_trade.trade_index}, {status})\n")
                response_parts.append(f"{pnl_type} P&L: ${pnl or 0:+,.2f}\n\n")

            open_count = sum(1 for t in all_trades if t.is_open)
            closed_count = len(all_trades) - open_count

            response_parts.append(f"Open Positions: {open_count}\n")
            response_parts.append(f"Closed Trades: {closed_count} (last {days} days)\n")

            # Calculate total P&L
            total_realized = sum(t.realized_pnl or 0 for t in all_trades if not t.is_open)
            total_unrealized = sum(t.unrealized_pnl or 0 for t in all_trades if t.is_open)
            response_parts.append(f"Total Realized P&L: ${total_realized:+,.2f}\n")
            if total_unrealized != 0:
                response_parts.append(f"Total Unrealized P&L: ${total_unrealized:+,.2f}\n")

            if len(all_trades) > 1:
                response_parts.append(f"\nUse trade_index parameter (0-{len(all_trades)-1}) to visualize other trades from the CSV.\n")

            response_parts.append("\n" + "═" * 79)

            final_response = "".join(response_parts)

            # Log request
            log_request(
                requests_dir=requests_dir,
                requester=requester,
                tool_name="binance_visualize_futures_trades",
                input_params={
                    "days": days,
                    "asset": asset,
                    "open_only": open_only,
                    "trade_index": trade_index,
                },
                output_result=[image_content, final_response] if image_content else [final_response]
            )

            logger.info("binance_visualize_futures_trades completed successfully")

            # Return with image first if available
            if image_content:
                return [image_content, final_response]
            else:
                return [final_response]

        except Exception as e:
            logger.error(f"Error in binance_visualize_futures_trades: {e}")
            error_msg = f"Error generating futures trade visualization: {str(e)}"
            return [error_msg]
