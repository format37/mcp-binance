import logging
from decimal import Decimal
import uuid
from mcp_service import format_csv_response
from request_logger import log_request
import pandas as pd
from binance.client import Client
from sentry_utils import with_sentry_tracing

logger = logging.getLogger(__name__)


@with_sentry_tracing("binance_get_book_ticker")
def fetch_book_ticker(binance_client: Client, symbol: str = 'BTCUSDT') -> pd.DataFrame:
    """
    Fetch order book ticker (best bid/ask) for a symbol and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')

    Returns:
        DataFrame with best bid/ask containing columns:
        - symbol: Trading pair symbol
        - bid_price: Best bid (buy) price
        - bid_qty: Quantity at best bid
        - ask_price: Best ask (sell) price
        - ask_qty: Quantity at best ask
        - spread: Price difference between ask and bid (ask - bid)
        - spread_percent: Spread as percentage of bid price
        - mid_price: Mid-market price ((bid + ask) / 2)
        - bid_value: Total value at best bid (bid_price * bid_qty)
        - ask_value: Total value at best ask (ask_price * ask_qty)

    Note:
        This is a lightweight alternative to fetching the full order book
        when you only need the best bid and ask prices with quantities.
    """
    logger.info(f"Fetching order book ticker for {symbol}")

    try:
        # Fetch order book ticker from Binance API
        ticker = binance_client.get_orderbook_ticker(symbol=symbol)

        # Extract and convert data
        bid_price = Decimal(ticker['bidPrice'])
        bid_qty = Decimal(ticker['bidQty'])
        ask_price = Decimal(ticker['askPrice'])
        ask_qty = Decimal(ticker['askQty'])

        # Calculate derived metrics
        spread = ask_price - bid_price
        spread_percent = float((spread / bid_price) * 100) if bid_price > 0 else 0.0
        mid_price = (bid_price + ask_price) / 2
        bid_value = bid_price * bid_qty
        ask_value = ask_price * ask_qty

        # Build record
        record = {
            'symbol': ticker['symbol'],
            'bid_price': float(bid_price),
            'bid_qty': float(bid_qty),
            'ask_price': float(ask_price),
            'ask_qty': float(ask_qty),
            'spread': float(spread),
            'spread_percent': spread_percent,
            'mid_price': float(mid_price),
            'bid_value': float(bid_value),
            'ask_value': float(ask_value)
        }

        logger.info(f"Successfully fetched book ticker for {symbol}")

    except Exception as e:
        logger.error(f"Error fetching book ticker from Binance API: {e}")
        raise

    # Create DataFrame with single row
    df = pd.DataFrame([record])

    return df


def register_binance_get_book_ticker(local_mcp_instance, local_binance_client, csv_dir, requests_dir):
    """Register the binance_get_book_ticker tool"""
    @local_mcp_instance.tool()
    def binance_get_book_ticker(requester: str, symbol: str = 'BTCUSDT') -> str:
        """
        Fetch best bid and ask prices with quantities and save to CSV file for analysis.

        This tool retrieves the current best bid (highest buy order) and best ask (lowest sell order)
        prices with their quantities. It's a lightweight alternative to the full order book when you
        only need the top-of-book data for spread monitoring and quick market sentiment analysis.

        Parameters:
            requester (str): Identifier of who is calling this tool (e.g., 'trading-agent', 'user-alex').
                Used for request logging and audit purposes.
            symbol (str): Trading pair symbol (default: 'BTCUSDT')
                Examples: 'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT'

        Returns:
            str: Formatted response with CSV file info, schema, sample data, and Python snippet to load the file.

        CSV Output Columns:
            - symbol (string): Trading pair symbol
            - bid_price (float): Best bid (buy) price - highest price buyers are willing to pay
            - bid_qty (float): Quantity available at best bid
            - ask_price (float): Best ask (sell) price - lowest price sellers are willing to accept
            - ask_qty (float): Quantity available at best ask
            - spread (float): Price difference between ask and bid (ask_price - bid_price)
            - spread_percent (float): Spread as percentage of bid price
            - mid_price (float): Mid-market price ((bid_price + ask_price) / 2)
            - bid_value (float): Total value at best bid (bid_price × bid_qty)
            - ask_value (float): Total value at best ask (ask_price × ask_qty)

        Understanding Bid/Ask:
            - Bid (Buy side): The highest price a buyer is willing to pay
            - Ask (Sell side): The lowest price a seller is willing to accept
            - Spread: The difference between ask and bid (cost of immediate execution)
            - Mid price: The average of bid and ask (often used as "fair value")
            - Tight spread: Indicates high liquidity and efficient market
            - Wide spread: Indicates low liquidity or volatile conditions

        Market Analysis Use Cases:
            - Spread monitoring: Track spread_percent to assess market liquidity
            - Market sentiment: Compare bid_qty vs ask_qty for buying/selling pressure
            - Liquidity assessment: Large quantities at best prices indicate good liquidity
            - Price discovery: Use mid_price as fair value estimate
            - Entry/exit timing: Check spread before placing market orders
            - Market efficiency: Narrow spreads indicate efficient markets
            - Volatility indicator: Widening spreads often precede volatility

        Trading Strategy Applications:
            - Order placement: Place limit orders between bid and ask to save on spread
            - Market vs limit orders: Use spread_percent to decide order type
            - Slippage estimation: Spread indicates minimum cost of immediate execution
            - Liquidity timing: Wait for favorable bid/ask quantities before trading
            - Market making: Analyze spread for potential profit opportunities
            - Quick sentiment: More bid volume suggests buying pressure
            - Fair price: Use mid_price for valuation and comparison

        Always use the py_eval tool to analyze the saved CSV file for insights such as:
            - Spread cost: spread as absolute value and percentage
            - Market pressure ratio: bid_qty / ask_qty (>1 = buying pressure, <1 = selling pressure)
            - Value imbalance: bid_value vs ask_value comparison
            - Fair value: mid_price calculation
            - Liquidity depth: Total value available at best prices
            - Spread efficiency: Compare spread_percent across different symbols
            - Execution cost estimate: spread / 2 for typical market order cost

        Example usage:
            binance_get_book_ticker(symbol='BTCUSDT')
            binance_get_book_ticker(symbol='ETHUSDT')
            binance_get_book_ticker(symbol='BNBUSDT')

        When to Use This Tool:
            - Quick spread checks without loading full order book
            - Monitoring liquidity at best prices
            - Fast market sentiment from bid/ask quantities
            - Lightweight alternative when rate limits are a concern

        When to Use Other Tools Instead:
            - For full market depth: use binance_get_orderbook
            - For detailed price statistics: use binance_get_ticker
            - For simple price only: use binance_get_price
            - For smoothed price: use binance_get_avg_price

        Note:
            This tool provides only the best bid and ask. For deeper market analysis
            with multiple price levels, use binance_get_orderbook.
            Data is READ-ONLY and does not execute any trades.
        """
        logger.info(f"binance_get_book_ticker tool invoked by {requester} for symbol: {symbol}")

        # Call fetch_book_ticker function
        df = fetch_book_ticker(binance_client=local_binance_client, symbol=symbol)

        # Generate filename with unique identifier
        filename = f"book_ticker_{symbol}_{str(uuid.uuid4())[:8]}.csv"
        filepath = csv_dir / filename

        # Save to CSV file
        df.to_csv(filepath, index=False)
        logger.info(f"Saved book ticker data to {filename} for {symbol}")

        # Return formatted response
        result = format_csv_response(filepath, df)

        # Log the request for audit trail
        log_request(
            requests_dir=requests_dir,
            requester=requester,
            tool_name="binance_get_book_ticker",
            input_params={"symbol": symbol},
            output_result=result
        )

        return result
