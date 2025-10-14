import logging
from decimal import Decimal
import uuid
from mcp_service import format_csv_response
import pandas as pd
from binance.client import Client

logger = logging.getLogger(__name__)


def fetch_orderbook(binance_client: Client, symbol: str = 'BTCUSDT', limit: int = 100) -> pd.DataFrame:
    """
    Fetch order book (market depth) for a symbol and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
        limit: Number of price levels to fetch (default: 100, max: 5000)
               Valid limits: 5, 10, 20, 50, 100, 500, 1000, 5000

    Returns:
        DataFrame with order book entries containing columns:
        - side: 'BID' for buy orders, 'ASK' for sell orders
        - price: Order price level
        - quantity: Total quantity available at this price level
        - total_value: Total value at this level (price * quantity)
        - level: Price level index (0 = best price, increasing away from mid)
        - distance_from_best: Price distance from best bid/ask
        - distance_percent: Percentage distance from best price

    Note:
        Bids are sorted from highest to lowest (best to worst).
        Asks are sorted from lowest to highest (best to worst).
        Best bid = highest buy price, Best ask = lowest sell price.
    """
    logger.info(f"Fetching order book for {symbol} with limit {limit}")

    try:
        # Fetch order book from Binance API
        order_book = binance_client.get_order_book(symbol=symbol, limit=limit)

        bids = order_book['bids']  # List of [price, quantity]
        asks = order_book['asks']  # List of [price, quantity]

        records = []

        # Process bids (buy orders) - already sorted highest to lowest
        if bids:
            best_bid_price = Decimal(bids[0][0])
            for level, (price_str, qty_str) in enumerate(bids):
                price = Decimal(price_str)
                qty = Decimal(qty_str)
                total_value = price * qty
                distance = best_bid_price - price
                distance_percent = float((distance / best_bid_price) * 100) if best_bid_price > 0 else 0.0

                records.append({
                    'side': 'BID',
                    'price': float(price),
                    'quantity': float(qty),
                    'total_value': float(total_value),
                    'level': level,
                    'distance_from_best': float(distance),
                    'distance_percent': distance_percent
                })

        # Process asks (sell orders) - already sorted lowest to highest
        if asks:
            best_ask_price = Decimal(asks[0][0])
            for level, (price_str, qty_str) in enumerate(asks):
                price = Decimal(price_str)
                qty = Decimal(qty_str)
                total_value = price * qty
                distance = price - best_ask_price
                distance_percent = float((distance / best_ask_price) * 100) if best_ask_price > 0 else 0.0

                records.append({
                    'side': 'ASK',
                    'price': float(price),
                    'quantity': float(qty),
                    'total_value': float(total_value),
                    'level': level,
                    'distance_from_best': float(distance),
                    'distance_percent': distance_percent
                })

        logger.info(f"Successfully fetched order book with {len(records)} levels for {symbol}")

    except Exception as e:
        logger.error(f"Error fetching order book from Binance API: {e}")
        raise

    # Create DataFrame
    df = pd.DataFrame(records)

    return df


def register_binance_get_orderbook(local_mcp_instance, local_binance_client, csv_dir):
    """Register the binance_get_orderbook tool"""
    @local_mcp_instance.tool()
    def binance_get_orderbook(symbol: str = 'BTCUSDT', limit: int = 100) -> str:
        """
        Fetch order book (market depth) for a trading symbol and save to CSV file for analysis.

        This tool retrieves the order book showing all pending buy (bid) and sell (ask) orders
        at different price levels. Essential for understanding market liquidity, support/resistance
        levels, and potential price impact of large trades.

        Parameters:
            symbol (str): Trading pair symbol (default: 'BTCUSDT')
                Examples: 'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT'
            limit (int): Number of price levels to fetch per side (default: 100, max: 5000)
                Valid values: 5, 10, 20, 50, 100, 500, 1000, 5000
                Higher limits provide deeper market view but use more data

        Returns:
            str: Formatted response with CSV file info, schema, sample data, and Python snippet to load the file.

        CSV Output Columns:
            - side (string): 'BID' for buy orders, 'ASK' for sell orders
            - price (float): Order price level
            - quantity (float): Total quantity available at this price level
            - total_value (float): Total value at this level (price × quantity)
            - level (integer): Price level index (0 = best price, increasing away from mid-market)
            - distance_from_best (float): Absolute price distance from best bid/ask
            - distance_percent (float): Percentage distance from best price

        Order Book Structure:
            - BID orders: Buy orders sorted from highest price (level 0 = best bid) to lowest
            - ASK orders: Sell orders sorted from lowest price (level 0 = best ask) to highest
            - Best bid: Highest price someone is willing to buy at
            - Best ask: Lowest price someone is willing to sell at
            - Spread: Difference between best ask and best bid

        Market Analysis Use Cases:
            - Liquidity assessment: Analyze quantity distribution across price levels
            - Support/resistance: Identify price levels with large order concentrations
            - Market depth: Calculate cumulative volume to understand market capacity
            - Price impact: Estimate slippage for large orders
            - Order placement: Find optimal entry/exit price levels
            - Market sentiment: Compare bid vs ask volumes to gauge pressure
            - Whale watching: Detect large orders (walls) that may influence price

        Trading Strategy Applications:
            - Limit order placement: Place orders at levels with good liquidity
            - Slippage estimation: Calculate expected price impact before trading
            - Support/resistance identification: Find strong price levels with high volume
            - Market making: Analyze spread and depth for profitability
            - Order flow analysis: Monitor changes in bid/ask distribution over time
            - Breakout detection: Identify thin areas where price may move quickly

        Always use the py_eval tool to analyze the saved CSV file for insights such as:
            - Spread calculation: min(ASK.price) - max(BID.price)
            - Cumulative volume: sum quantities by side and distance levels
            - Volume imbalance: compare total BID vs ASK quantities
            - Order walls: identify levels with unusually large quantities
            - Average order size: mean quantity by side
            - Depth distribution: quantity distribution by distance_percent
            - Liquidity zones: price ranges with highest cumulative volume

        Example usage:
            binance_get_orderbook(symbol='BTCUSDT', limit=100)
            binance_get_orderbook(symbol='ETHUSDT', limit=20)
            binance_get_orderbook(symbol='BNBUSDT', limit=500)

        Note:
            Order book data is a snapshot and changes rapidly in active markets.
            Data is READ-ONLY and does not execute any trades.
            Larger limits provide more comprehensive market view but consume more tokens.
        """
        logger.info(f"binance_get_orderbook tool invoked for symbol: {symbol}, limit: {limit}")

        # Validate limit
        valid_limits = [5, 10, 20, 50, 100, 500, 1000, 5000]
        if limit not in valid_limits:
            # Find closest valid limit
            limit = min(valid_limits, key=lambda x: abs(x - limit))
            logger.warning(f"Invalid limit provided, using closest valid limit: {limit}")

        # Call fetch_orderbook function
        df = fetch_orderbook(binance_client=local_binance_client, symbol=symbol, limit=limit)

        if df.empty:
            return f"No order book data found for {symbol}."

        # Generate filename with unique identifier
        filename = f"orderbook_{symbol}_{limit}_{str(uuid.uuid4())[:8]}.csv"
        filepath = csv_dir / filename

        # Save to CSV file
        df.to_csv(filepath, index=False)
        logger.info(f"Saved order book data to {filename} for {symbol} ({len(df)} levels)")

        # Return formatted response
        return format_csv_response(filepath, df)
