import logging
from datetime import datetime, timedelta
import uuid
from mcp_service import format_csv_response
from request_logger import log_request
import pandas as pd
from binance.client import Client
from sentry_utils import with_sentry_tracing

logger = logging.getLogger(__name__)


@with_sentry_tracing("binance_get_historical_klines")
def fetch_historical_klines(
    binance_client: Client,
    symbol: str = 'BTCUSDT',
    interval: str = '1h',
    days: int = 30
) -> pd.DataFrame:
    """
    Fetch historical klines/candlestick data for a symbol and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
        interval: Kline interval (e.g., '1m', '5m', '15m', '1h', '4h', '1d')
        days: Number of days to look back (default: 30)

    Returns:
        DataFrame with historical klines containing columns:
        - open_time: Kline open time (timestamp ms)
        - open_time_readable: Human-readable open time (YYYY-MM-DD HH:MM:SS)
        - open: Opening price
        - high: Highest price
        - low: Lowest price
        - close: Closing price
        - volume: Trading volume in base asset
        - close_time: Kline close time (timestamp ms)
        - close_time_readable: Human-readable close time
        - quote_volume: Trading volume in quote asset
        - num_trades: Number of trades
        - taker_buy_base_volume: Taker buy volume in base asset
        - taker_buy_quote_volume: Taker buy volume in quote asset

    Note:
        This provides historical OHLCV data for technical analysis and backtesting.
        Use this to get accurate historical prices at specific timestamps.
    """
    logger.info(f"Fetching historical klines for {symbol}, interval={interval}, days={days}")

    try:
        # Calculate start time (days ago from now)
        start_time = datetime.now() - timedelta(days=days)
        start_str = start_time.strftime('%Y-%m-%d')

        # Fetch historical klines from Binance API
        klines = binance_client.get_historical_klines(
            symbol=symbol,
            interval=interval,
            start_str=start_str
        )

        # Process klines into records
        records = []
        for kline in klines:
            open_time = int(kline[0])
            close_time = int(kline[6])

            record = {
                'open_time': open_time,
                'open_time_readable': datetime.fromtimestamp(open_time / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                'open': float(kline[1]),
                'high': float(kline[2]),
                'low': float(kline[3]),
                'close': float(kline[4]),
                'volume': float(kline[5]),
                'close_time': close_time,
                'close_time_readable': datetime.fromtimestamp(close_time / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                'quote_volume': float(kline[7]),
                'num_trades': int(kline[8]),
                'taker_buy_base_volume': float(kline[9]),
                'taker_buy_quote_volume': float(kline[10])
            }
            records.append(record)

        logger.info(f"Successfully fetched {len(records)} klines for {symbol}")

    except Exception as e:
        logger.error(f"Error fetching historical klines from Binance API: {e}")
        raise

    # Create DataFrame
    df = pd.DataFrame(records)

    return df


def register_binance_get_historical_klines(local_mcp_instance, local_binance_client, csv_dir, requests_dir):
    """Register the binance_get_historical_klines tool"""
    @local_mcp_instance.tool()
    def binance_get_historical_klines(
        requester: str,
        symbol: str = 'BTCUSDT',
        interval: str = '1h',
        days: int = 30
    ) -> str:
        """
        Fetch historical klines/candlestick (OHLCV) data and save to CSV file for analysis.

        This tool retrieves historical price data in candlestick format, essential for
        technical analysis, backtesting strategies, and getting accurate historical prices
        at specific timestamps.

        Parameters:
            requester (str): Identifier of who is calling this tool (e.g., 'trading-agent', 'user-alex').
                Used for request logging and audit purposes.

            symbol (str): Trading pair symbol (default: 'BTCUSDT')
                Examples: 'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT'

            interval (str): Candlestick interval (default: '1h')
                Valid intervals: '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M'
                Examples:
                - '1m': 1 minute (high granularity, more data points)
                - '5m': 5 minutes (good for intraday analysis)
                - '1h': 1 hour (balanced granularity)
                - '1d': 1 day (daily candles for long-term analysis)

            days (int): Number of days to look back (default: 30)
                Examples: 7 (last week), 30 (last month), 90 (last quarter), 365 (last year)
                Note: Binance may have limits on historical data availability

        Returns:
            str: Formatted response with CSV file info, schema, sample data, and Python snippet to load the file.

        CSV Output Columns:
            - open_time (integer): Kline open time in milliseconds (timestamp)
            - open_time_readable (string): Human-readable open time (YYYY-MM-DD HH:MM:SS)
            - open (float): Opening price at the start of the interval
            - high (float): Highest price during the interval
            - low (float): Lowest price during the interval
            - close (float): Closing price at the end of the interval
            - volume (float): Total trading volume in base asset during interval
            - close_time (integer): Kline close time in milliseconds (timestamp)
            - close_time_readable (string): Human-readable close time
            - quote_volume (float): Total trading volume in quote asset (e.g., USDT)
            - num_trades (integer): Number of trades executed during interval
            - taker_buy_base_volume (float): Volume of taker buy orders in base asset
            - taker_buy_quote_volume (float): Volume of taker buy orders in quote asset

        Key Features:
            - Historical prices: Get accurate OHLC data for any timestamp
            - Flexible intervals: From 1-minute to monthly candles
            - Complete OHLCV: Open, High, Low, Close, Volume data
            - Trade statistics: Number of trades and buy/sell volume breakdown
            - Timestamps: Both millisecond precision and human-readable formats

        When to Use This Tool:
            - Get historical prices at specific dates/times for portfolio valuation
            - Backtest trading strategies using past price data
            - Calculate historical returns and performance metrics
            - Analyze price patterns and trends over time
            - Value deposits/withdrawals at historical prices
            - Build equity curves from cash flow events

        Market Analysis Use Cases:
            - Price discovery: Find exact price at a specific timestamp
            - Trend analysis: Identify uptrends/downtrends using OHLC patterns
            - Volatility measurement: Calculate high-low ranges over time
            - Volume analysis: Track trading activity patterns
            - Support/resistance: Identify key price levels from historical data
            - Moving averages: Calculate SMAs, EMAs from close prices

        Trading Strategy Applications:
            - Backtesting: Test strategy performance on historical data
            - Pattern recognition: Detect candlestick patterns (doji, hammer, etc.)
            - Breakout detection: Identify when price breaks historical highs/lows
            - Mean reversion: Calculate deviation from historical averages
            - Momentum indicators: Build RSI, MACD, Bollinger Bands from OHLCV
            - Performance metrics: Calculate Sharpe ratio, max drawdown, etc.

        Portfolio Analysis Use Cases:
            - Historical valuation: Value assets at specific past dates
            - Deposit/withdrawal pricing: Get price at the time of cash flow events
            - Buy-and-hold comparison: Calculate what-if scenarios with historical prices
            - Cost basis calculation: Determine acquisition costs at historical prices
            - Return calculation: Measure portfolio returns over time

        Always use the py_eval tool to analyze the saved CSV file for insights such as:
            - Finding price at specific timestamp: df[df['open_time_readable'] == '2024-01-15 10:00:00']['close']
            - Calculating daily returns: df['close'].pct_change()
            - Finding high/low over period: df['high'].max(), df['low'].min()
            - Volume analysis: df['volume'].sum(), df['num_trades'].sum()
            - Price interpolation: Use pandas interpolate() for missing timestamps

        Example usage:
            # Get 1-hour candles for last 30 days
            binance_get_historical_klines(symbol='BTCUSDT', interval='1h', days=30)

            # Get 1-day candles for last year
            binance_get_historical_klines(symbol='ETHUSDT', interval='1d', days=365)

            # Get 5-minute candles for last week
            binance_get_historical_klines(symbol='BNBUSDT', interval='5m', days=7)

        Note:
            - Data is READ-ONLY and does not execute any trades
            - Historical data availability depends on Binance's retention policies
            - Use shorter intervals (1m, 5m) for recent data only to avoid hitting API limits
            - Use longer intervals (1h, 1d) for extended historical analysis
            - For exact timestamp matching, use 1-minute or 5-minute intervals
        """
        logger.info(f"binance_get_historical_klines tool invoked by {requester} for {symbol}, interval={interval}, days={days}")

        # Call fetch_historical_klines function
        df = fetch_historical_klines(
            binance_client=local_binance_client,
            symbol=symbol,
            interval=interval,
            days=days
        )

        # Generate filename with unique identifier
        filename = f"klines_{symbol}_{interval}_{days}d_{str(uuid.uuid4())[:8]}.csv"
        filepath = csv_dir / filename

        # Save to CSV file
        df.to_csv(filepath, index=False)
        logger.info(f"Saved historical klines data to {filename} for {symbol}")

        # Return formatted response
        result = format_csv_response(filepath, df)

        # Log the request for audit trail
        log_request(
            requests_dir=requests_dir,
            requester=requester,
            tool_name="binance_get_historical_klines",
            input_params={"symbol": symbol, "interval": interval, "days": days},
            output_result=result
        )

        return result
