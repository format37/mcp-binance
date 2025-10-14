import logging
from decimal import Decimal
import uuid
from mcp_service import format_csv_response
import pandas as pd
from binance.client import Client
from sentry_utils import with_sentry_tracing

logger = logging.getLogger(__name__)


@with_sentry_tracing("binance_get_price")
def fetch_price(binance_client: Client, symbol: str = 'BTCUSDT') -> pd.DataFrame:
    """
    Fetch current price for a symbol and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')

    Returns:
        DataFrame with current price containing columns:
        - symbol: Trading pair symbol
        - price: Current price

    Note:
        This is the simplest and fastest way to get current price.
        For more detailed price information, use binance_get_ticker.
    """
    logger.info(f"Fetching current price for {symbol}")

    try:
        # Fetch current price from Binance API
        price_data = binance_client.get_symbol_ticker(symbol=symbol)

        # Build record
        record = {
            'symbol': price_data['symbol'],
            'price': float(Decimal(price_data['price']))
        }

        logger.info(f"Successfully fetched price for {symbol}: {record['price']}")

    except Exception as e:
        logger.error(f"Error fetching price from Binance API: {e}")
        raise

    # Create DataFrame with single row
    df = pd.DataFrame([record])

    return df


def register_binance_get_price(local_mcp_instance, local_binance_client, csv_dir):
    """Register the binance_get_price tool"""
    @local_mcp_instance.tool()
    def binance_get_price(symbol: str = 'BTCUSDT') -> str:
        """
        Fetch current price for a trading symbol and save to CSV file for analysis.

        This tool retrieves the current spot price for a symbol in the simplest and fastest way.
        Perfect for quick price checks when you don't need comprehensive market statistics.

        Parameters:
            symbol (str): Trading pair symbol (default: 'BTCUSDT')
                Examples: 'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT'

        Returns:
            str: Formatted response with CSV file info, schema, sample data, and Python snippet to load the file.

        CSV Output Columns:
            - symbol (string): Trading pair symbol
            - price (float): Current/last traded price

        Key Features:
            - Lightweight: Minimal data transfer for fast response
            - Simple: Only returns the essential current price
            - Fast: Optimized endpoint for speed
            - Real-time: Reflects the most recent trade price

        When to Use This Tool:
            - Quick price checks without needing detailed statistics
            - High-frequency price monitoring
            - Simple price alerts or thresholds
            - Fast portfolio valuation updates
            - Lightweight API usage when rate limits are a concern

        When to Use Other Tools Instead:
            - For 24h statistics (change, volume, high/low): use binance_get_ticker
            - For bid/ask spread: use binance_get_book_ticker
            - For smoothed/average price: use binance_get_avg_price
            - For order book depth: use binance_get_orderbook
            - For trade history: use binance_get_recent_trades

        Market Analysis Use Cases:
            - Price monitoring: Track current price for alerts or triggers
            - Multi-symbol comparison: Quickly fetch prices for multiple symbols
            - Portfolio valuation: Calculate total value using current prices
            - Price recording: Log prices over time for historical analysis
            - Threshold checking: Compare price against buy/sell targets

        Trading Strategy Applications:
            - Price alerts: Check if price crosses threshold levels
            - Simple moving average: Calculate SMA from historical price snapshots
            - Correlation analysis: Track price movements across multiple symbols
            - Arbitrage detection: Compare prices across different pairs
            - Portfolio tracking: Calculate total portfolio value at current prices

        Always use the py_eval tool to analyze the saved CSV file for insights such as:
            - Price comparison across multiple symbols
            - Percentage change from a reference price
            - Price thresholds and trigger levels
            - Simple price statistics over time

        Example usage:
            binance_get_price(symbol='BTCUSDT')
            binance_get_price(symbol='ETHUSDT')
            binance_get_price(symbol='BNBUSDT')

        Note:
            This tool provides only the current price. For comprehensive market data
            including volume, 24h change, and other statistics, use binance_get_ticker.
            Data is READ-ONLY and does not execute any trades.
        """
        logger.info(f"binance_get_price tool invoked for symbol: {symbol}")

        # Call fetch_price function
        df = fetch_price(binance_client=local_binance_client, symbol=symbol)

        # Generate filename with unique identifier
        filename = f"price_{symbol}_{str(uuid.uuid4())[:8]}.csv"
        filepath = csv_dir / filename

        # Save to CSV file
        df.to_csv(filepath, index=False)
        logger.info(f"Saved price data to {filename} for {symbol}")

        # Return formatted response
        return format_csv_response(filepath, df)
