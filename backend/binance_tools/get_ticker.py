import logging
from datetime import datetime
from decimal import Decimal
import uuid
from mcp_service import format_csv_response
import pandas as pd
from binance.client import Client
from sentry_utils import with_sentry_tracing

logger = logging.getLogger(__name__)


@with_sentry_tracing("binance_get_ticker")
def fetch_ticker(binance_client: Client, symbol: str = 'BTCUSDT') -> pd.DataFrame:
    """
    Fetch 24-hour ticker statistics for a symbol and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')

    Returns:
        DataFrame with 24-hour ticker statistics containing columns:
        - symbol: Trading pair symbol
        - current_price: Current/last traded price
        - price_change: Absolute price change in 24h
        - price_change_percent: Percentage price change in 24h
        - open_price: Price at the start of 24h period
        - high_price: Highest price in 24h
        - low_price: Lowest price in 24h
        - prev_close_price: Previous day's closing price
        - weighted_avg_price: Volume-weighted average price over 24h
        - bid_price: Current best bid price
        - bid_qty: Quantity at best bid
        - ask_price: Current best ask price
        - ask_qty: Quantity at best ask
        - spread: Difference between ask and bid (ask - bid)
        - spread_percent: Spread as percentage of bid price
        - volume: Total volume traded in base currency (24h)
        - quote_volume: Total volume traded in quote currency (24h)
        - num_trades: Number of trades executed in 24h
        - open_time: Period start timestamp
        - close_time: Period end timestamp

    Note:
        All statistics cover a rolling 24-hour period ending at the current time.
    """
    logger.info(f"Fetching 24-hour ticker statistics for {symbol}")

    try:
        # Fetch 24-hour ticker statistics from Binance API
        ticker = binance_client.get_ticker(symbol=symbol)

        # Convert timestamps to readable format
        open_time = datetime.fromtimestamp(ticker['openTime'] / 1000)
        close_time = datetime.fromtimestamp(ticker['closeTime'] / 1000)

        # Calculate spread
        bid_price = Decimal(ticker['bidPrice'])
        ask_price = Decimal(ticker['askPrice'])
        spread = ask_price - bid_price
        spread_percent = float((spread / bid_price) * 100) if bid_price > 0 else 0.0

        # Build record
        record = {
            'symbol': ticker['symbol'],
            'current_price': float(Decimal(ticker['lastPrice'])),
            'price_change': float(Decimal(ticker['priceChange'])),
            'price_change_percent': float(Decimal(ticker['priceChangePercent'])),
            'open_price': float(Decimal(ticker['openPrice'])),
            'high_price': float(Decimal(ticker['highPrice'])),
            'low_price': float(Decimal(ticker['lowPrice'])),
            'prev_close_price': float(Decimal(ticker['prevClosePrice'])),
            'weighted_avg_price': float(Decimal(ticker['weightedAvgPrice'])),
            'bid_price': float(bid_price),
            'bid_qty': float(Decimal(ticker['bidQty'])),
            'ask_price': float(ask_price),
            'ask_qty': float(Decimal(ticker['askQty'])),
            'spread': float(spread),
            'spread_percent': spread_percent,
            'volume': float(Decimal(ticker['volume'])),
            'quote_volume': float(Decimal(ticker['quoteVolume'])),
            'num_trades': int(ticker['count']),
            'open_time': open_time.strftime('%Y-%m-%d %H:%M:%S'),
            'close_time': close_time.strftime('%Y-%m-%d %H:%M:%S')
        }

        logger.info(f"Successfully fetched ticker data for {symbol}")

    except Exception as e:
        logger.error(f"Error fetching ticker data from Binance API: {e}")
        raise

    # Create DataFrame with single row
    df = pd.DataFrame([record])

    return df


def register_binance_get_ticker(local_mcp_instance, local_binance_client, csv_dir):
    """Register the binance_get_ticker tool"""
    @local_mcp_instance.tool()
    def binance_get_ticker(symbol: str = 'BTCUSDT') -> str:
        """
        Fetch 24-hour ticker statistics for a trading symbol and save to CSV file for analysis.

        This tool retrieves comprehensive 24-hour rolling statistics including price changes,
        volume, high/low prices, bid/ask spread, and trading activity. Perfect for market
        overview and volatility analysis.

        Parameters:
            symbol (str): Trading pair symbol (default: 'BTCUSDT')
                Examples: 'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT'

        Returns:
            str: Formatted response with CSV file info, schema, sample data, and Python snippet to load the file.

        CSV Output Columns:
            - symbol (string): Trading pair symbol
            - current_price (float): Current/last traded price
            - price_change (float): Absolute price change in 24h
            - price_change_percent (float): Percentage price change in 24h (e.g., -1.84 means -1.84%)
            - open_price (float): Price at the start of 24h period
            - high_price (float): Highest price in 24h period
            - low_price (float): Lowest price in 24h period
            - prev_close_price (float): Previous day's closing price
            - weighted_avg_price (float): Volume-weighted average price over 24h
            - bid_price (float): Current best bid (buy) price
            - bid_qty (float): Quantity available at best bid
            - ask_price (float): Current best ask (sell) price
            - ask_qty (float): Quantity available at best ask
            - spread (float): Price difference between ask and bid
            - spread_percent (float): Spread as percentage of bid price
            - volume (float): Total volume traded in base currency (24h)
            - quote_volume (float): Total volume traded in quote currency (24h)
            - num_trades (integer): Number of trades executed in 24h
            - open_time (string): Period start timestamp (YYYY-MM-DD HH:MM:SS)
            - close_time (string): Period end timestamp (YYYY-MM-DD HH:MM:SS)

        Market Analysis Use Cases:
            - Market performance: Track price_change_percent to understand 24h performance
            - Volatility analysis: Compare high_price and low_price to measure price range
            - Volume analysis: Monitor volume and num_trades for liquidity assessment
            - Market sentiment: Analyze bid/ask quantities and spread for market pressure
            - Price targets: Use high/low as resistance/support levels
            - Average pricing: Use weighted_avg_price for smoothed price reference
            - Spread monitoring: Check spread_percent to assess market efficiency

        Trading Strategy Applications:
            - Trend identification: positive price_change indicates uptrend, negative indicates downtrend
            - Volatility trading: High price range (high-low) indicates opportunity for range trading
            - Liquidity checks: High volume and num_trades indicate good market liquidity
            - Entry/exit timing: Use spread analysis to optimize order placement
            - Performance comparison: Compare multiple symbols to find best performers

        Always use the py_eval tool to analyze the saved CSV file for insights such as:
            - Calculating price range: high_price - low_price
            - Volatility ratio: (high_price - low_price) / open_price
            - Volume momentum: quote_volume for market interest
            - Market pressure: comparing bid_qty vs ask_qty
            - Relative performance: price_change_percent comparison across symbols

        Example usage:
            binance_get_ticker(symbol='BTCUSDT')
            binance_get_ticker(symbol='ETHUSDT')

        Note:
            Statistics cover a rolling 24-hour period ending at the current time.
            Data is READ-ONLY and does not execute any trades.
        """
        logger.info(f"binance_get_ticker tool invoked for symbol: {symbol}")

        # Call fetch_ticker function
        df = fetch_ticker(binance_client=local_binance_client, symbol=symbol)

        # Generate filename with unique identifier
        filename = f"ticker_{symbol}_{str(uuid.uuid4())[:8]}.csv"
        filepath = csv_dir / filename

        # Save to CSV file
        df.to_csv(filepath, index=False)
        logger.info(f"Saved ticker data to {filename} for {symbol}")

        # Return formatted response
        return format_csv_response(filepath, df)
