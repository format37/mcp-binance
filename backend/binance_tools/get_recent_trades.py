import logging
from datetime import datetime
from decimal import Decimal
import uuid
from mcp_service import format_csv_response
import pandas as pd
from binance.client import Client
from sentry_utils import with_sentry_tracing

logger = logging.getLogger(__name__)


@with_sentry_tracing("binance_get_recent_trades")
def fetch_recent_trades(binance_client: Client, symbol: str = 'BTCUSDT', limit: int = 100) -> pd.DataFrame:
    """
    Fetch recent trades for a symbol and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
        limit: Number of recent trades to fetch (default: 100, max: 1000)

    Returns:
        DataFrame with recent trades containing columns:
        - trade_id: Unique trade identifier
        - time: Trade execution timestamp
        - price: Execution price
        - quantity: Trade quantity
        - total_value: Total value of trade (price * quantity)
        - side: 'BUY' if buyer initiated, 'SELL' if seller initiated
        - is_buyer_maker: True if buyer was the maker (placed limit order)

    Note:
        Trades are returned in reverse chronological order (most recent first).
        'BUY' side means the trade was initiated by a buyer (aggressive buy, market buy).
        'SELL' side means the trade was initiated by a seller (aggressive sell, market sell).
    """
    logger.info(f"Fetching recent trades for {symbol} with limit {limit}")

    try:
        # Fetch recent trades from Binance API
        trades = binance_client.get_recent_trades(symbol=symbol, limit=limit)

        records = []
        for trade in trades:
            price = Decimal(trade['price'])
            qty = Decimal(trade['qty'])
            total_value = price * qty

            # isBuyerMaker: True means seller initiated (taker was seller)
            # We want to show from taker perspective
            side = 'SELL' if trade['isBuyerMaker'] else 'BUY'

            # Convert timestamp to readable format
            trade_time = datetime.fromtimestamp(trade['time'] / 1000)

            records.append({
                'trade_id': int(trade['id']),
                'time': trade_time.strftime('%Y-%m-%d %H:%M:%S'),
                'price': float(price),
                'quantity': float(qty),
                'total_value': float(total_value),
                'side': side,
                'is_buyer_maker': bool(trade['isBuyerMaker'])
            })

        logger.info(f"Successfully fetched {len(records)} recent trades for {symbol}")

    except Exception as e:
        logger.error(f"Error fetching recent trades from Binance API: {e}")
        raise

    # Create DataFrame
    df = pd.DataFrame(records)

    # Sort by trade_id descending (most recent first)
    if not df.empty:
        df = df.sort_values('trade_id', ascending=False).reset_index(drop=True)

    return df


def register_binance_get_recent_trades(local_mcp_instance, local_binance_client, csv_dir):
    """Register the binance_get_recent_trades tool"""
    @local_mcp_instance.tool()
    def binance_get_recent_trades(symbol: str = 'BTCUSDT', limit: int = 100) -> str:
        """
        Fetch recent executed trades for a trading symbol and save to CSV file for analysis.

        This tool retrieves the most recent trades executed on Binance, showing actual execution
        prices, quantities, timing, and trade direction. Essential for understanding real-time
        market activity, buy/sell pressure, and trade flow patterns.

        Parameters:
            symbol (str): Trading pair symbol (default: 'BTCUSDT')
                Examples: 'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT'
            limit (int): Number of recent trades to fetch (default: 100, max: 1000)
                Higher limits provide more historical context

        Returns:
            str: Formatted response with CSV file info, schema, sample data, and Python snippet to load the file.

        CSV Output Columns:
            - trade_id (integer): Unique trade identifier (monotonically increasing)
            - time (string): Trade execution timestamp (YYYY-MM-DD HH:MM:SS)
            - price (float): Execution price
            - quantity (float): Quantity traded
            - total_value (float): Total value of trade (price × quantity)
            - side (string): 'BUY' if buyer initiated (market buy), 'SELL' if seller initiated (market sell)
            - is_buyer_maker (boolean): True if buyer was maker (placed limit order that got filled)

        Understanding Trade Direction:
            - BUY side: Trade initiated by buyer (taker buy / aggressive buy / market buy order)
              This indicates buying pressure as someone bought at the ask price
            - SELL side: Trade initiated by seller (taker sell / aggressive sell / market sell order)
              This indicates selling pressure as someone sold at the bid price
            - is_buyer_maker: If True, buyer was passive (limit order), seller was aggressive
            - Trades are shown from the taker's (aggressive party's) perspective

        Market Analysis Use Cases:
            - Buy/sell pressure: Count BUY vs SELL trades to gauge market sentiment
            - Trade velocity: Analyze time gaps between trades for activity level
            - Price momentum: Track sequential price changes for trend detection
            - Volume analysis: Analyze quantity distribution across trades
            - Large trade detection: Identify unusually large trades (potential whales)
            - Execution quality: Analyze actual execution prices vs order book
            - Market microstructure: Study trade patterns and clustering

        Trading Strategy Applications:
            - Momentum trading: High buy-initiated volume suggests upward momentum
            - Reversal detection: Sudden shift in buy/sell ratio may signal reversal
            - Volume spikes: Large trades or clusters indicate significant events
            - Price action: Sequential same-side trades often precede larger moves
            - Slippage analysis: Compare execution prices to understand market impact
            - Order flow trading: Use trade direction patterns for entry/exit signals
            - Tape reading: Analyze trade sequences for institutional activity

        Always use the py_eval tool to analyze the saved CSV file for insights such as:
            - Buy/sell ratio: count(side='BUY') / count(side='SELL')
            - Total volume by side: sum(quantity) grouped by side
            - Average trade size: mean(quantity) overall and by side
            - Price momentum: compare recent prices to earlier prices
            - Large trades: trades where quantity > 90th percentile
            - Trade frequency: time gaps between consecutive trades
            - Aggressor analysis: percentage of trades that are buyer vs seller initiated
            - Volume-weighted average price (VWAP): sum(price * quantity) / sum(quantity)

        Example usage:
            binance_get_recent_trades(symbol='BTCUSDT', limit=100)
            binance_get_recent_trades(symbol='ETHUSDT', limit=500)
            binance_get_recent_trades(symbol='BNBUSDT', limit=1000)

        Note:
            Trades are returned in reverse chronological order (most recent first).
            Data is READ-ONLY and does not execute any trades.
            Trade data represents actual executed transactions on the exchange.
        """
        logger.info(f"binance_get_recent_trades tool invoked for symbol: {symbol}, limit: {limit}")

        # Validate and cap limit
        if limit > 1000:
            limit = 1000
            logger.warning(f"Limit capped at maximum: 1000")
        elif limit < 1:
            limit = 1
            logger.warning(f"Limit must be at least 1, using 1")

        # Call fetch_recent_trades function
        df = fetch_recent_trades(binance_client=local_binance_client, symbol=symbol, limit=limit)

        if df.empty:
            return f"No recent trades found for {symbol}."

        # Generate filename with unique identifier
        filename = f"trades_{symbol}_{limit}_{str(uuid.uuid4())[:8]}.csv"
        filepath = csv_dir / filename

        # Save to CSV file
        df.to_csv(filepath, index=False)
        logger.info(f"Saved trade data to {filename} for {symbol} ({len(df)} trades)")

        # Return formatted response
        return format_csv_response(filepath, df)
