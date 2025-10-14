import logging
from decimal import Decimal
import uuid
from mcp_service import format_csv_response
import pandas as pd
from binance.client import Client

logger = logging.getLogger(__name__)


def fetch_avg_price(binance_client: Client, symbol: str = 'BTCUSDT') -> pd.DataFrame:
    """
    Fetch average price for a symbol and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')

    Returns:
        DataFrame with average price containing columns:
        - symbol: Trading pair symbol
        - avg_price: Volume-weighted average price
        - time_window_mins: Time window in minutes for the average (typically 5)

    Note:
        The average price is calculated as a volume-weighted average over
        a specific time period (typically 5 minutes). This provides a smoothed
        price that filters out short-term volatility.
    """
    logger.info(f"Fetching average price for {symbol}")

    try:
        # Fetch average price from Binance API
        avg_price_data = binance_client.get_avg_price(symbol=symbol)

        # Build record
        record = {
            'symbol': symbol,
            'avg_price': float(Decimal(avg_price_data['price'])),
            'time_window_mins': int(avg_price_data['mins'])
        }

        logger.info(f"Successfully fetched average price for {symbol}: {record['avg_price']} (over {record['time_window_mins']} minutes)")

    except Exception as e:
        logger.error(f"Error fetching average price from Binance API: {e}")
        raise

    # Create DataFrame with single row
    df = pd.DataFrame([record])

    return df


def register_binance_get_avg_price(local_mcp_instance, local_binance_client, csv_dir):
    """Register the binance_get_avg_price tool"""
    @local_mcp_instance.tool()
    def binance_get_avg_price(symbol: str = 'BTCUSDT') -> str:
        """
        Fetch volume-weighted average price for a trading symbol and save to CSV file for analysis.

        This tool retrieves the volume-weighted average price (typically over 5 minutes), providing
        a smoothed price that filters out short-term volatility and price anomalies. Useful for
        making trading decisions based on a more stable price reference rather than spot price.

        Parameters:
            symbol (str): Trading pair symbol (default: 'BTCUSDT')
                Examples: 'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT'

        Returns:
            str: Formatted response with CSV file info, schema, sample data, and Python snippet to load the file.

        CSV Output Columns:
            - symbol (string): Trading pair symbol
            - avg_price (float): Volume-weighted average price over the time window
            - time_window_mins (integer): Time window in minutes (typically 5)

        Understanding Average Price:
            - Volume-weighted: Trades with larger volume have more influence on the average
            - Smoothed: Filters out temporary price spikes and anomalies
            - Recent: Covers only the most recent time period (usually 5 minutes)
            - Fair value: Often considered more representative than spot price
            - Less volatile: More stable than current price for decision-making

        Market Analysis Use Cases:
            - Volatility filtering: Compare avg_price to current price to detect volatility
            - Price anomaly detection: Large deviation suggests unusual market activity
            - Fair value estimation: Use as reference price for trading decisions
            - Trend confirmation: Rising average price confirms uptrend
            - Mean reversion: Extreme deviations may revert to average
            - Momentum indicator: Rate of change in average price shows momentum
            - Stable reference: Use for calculations that need stable price input

        Trading Strategy Applications:
            - Entry signals: Buy when price < avg_price (potential undervalued)
            - Exit signals: Sell when price > avg_price (potential overvalued)
            - Stop loss: Place stops relative to average rather than current price
            - Limit orders: Use average price to set more realistic limit prices
            - Volatility trading: Trade when deviation from average exceeds threshold
            - Trend following: Follow direction of average price movement
            - Fair value arbitrage: Exploit gaps between current and average price

        Comparison with Other Price Tools:
            - vs binance_get_price: Average is smoothed, current is instantaneous
            - vs binance_get_ticker: Average is time-weighted, ticker shows raw statistics
            - vs binance_get_book_ticker: Average reflects executed trades, not order book
            - Use case: Choose average when you need stable, volatility-filtered pricing

        Always use the py_eval tool to analyze the saved CSV file for insights such as:
            - Fetch both avg_price and current price to calculate deviation
            - Deviation = (current_price - avg_price) / avg_price * 100
            - Positive deviation: Price above average (potential overbought)
            - Negative deviation: Price below average (potential oversold)
            - Track average price over time to identify trends
            - Compare deviation across symbols to find anomalies

        Example usage:
            binance_get_avg_price(symbol='BTCUSDT')
            binance_get_avg_price(symbol='ETHUSDT')
            binance_get_avg_price(symbol='BNBUSDT')

        Trading Examples:
            1. Mean Reversion Strategy:
               - If current_price > avg_price * 1.02 (2% above), consider selling
               - If current_price < avg_price * 0.98 (2% below), consider buying

            2. Trend Following:
               - Fetch average price at regular intervals
               - If consistently rising, maintain long positions
               - If consistently falling, avoid or short

            3. Volatility Detection:
               - Calculate: abs(current_price - avg_price) / avg_price
               - High ratio (>1%): High volatility, use wider stops
               - Low ratio (<0.2%): Low volatility, use tighter stops

        Note:
            The average price is typically calculated over 5 minutes but the time window
            may vary. Check the time_window_mins field for the actual period.
            Data is READ-ONLY and does not execute any trades.
        """
        logger.info(f"binance_get_avg_price tool invoked for symbol: {symbol}")

        # Call fetch_avg_price function
        df = fetch_avg_price(binance_client=local_binance_client, symbol=symbol)

        # Generate filename with unique identifier
        filename = f"avg_price_{symbol}_{str(uuid.uuid4())[:8]}.csv"
        filepath = csv_dir / filename

        # Save to CSV file
        df.to_csv(filepath, index=False)
        logger.info(f"Saved average price data to {filename} for {symbol}")

        # Return formatted response
        return format_csv_response(filepath, df)
