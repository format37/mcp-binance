import logging
from datetime import datetime
import uuid
from mcp_service import format_csv_response
from request_logger import log_request
import pandas as pd
from binance.client import Client
from typing import Optional
from sentry_utils import with_sentry_tracing

logger = logging.getLogger(__name__)


@with_sentry_tracing("binance_get_open_orders")
def fetch_open_orders(binance_client: Client, symbol: Optional[str] = None) -> pd.DataFrame:
    """
    Fetch open orders from Binance and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (optional, e.g., 'BTCUSDT'). If None, returns all open orders.

    Returns:
        DataFrame with open orders containing columns:
        - orderId: Unique order identifier
        - symbol: Trading pair symbol (e.g., 'BTCUSDT')
        - type: Order type (LIMIT, MARKET, STOP_LOSS, STOP_LOSS_LIMIT, TAKE_PROFIT, TAKE_PROFIT_LIMIT, LIMIT_MAKER)
        - side: Order side (BUY or SELL)
        - price: Order price (limit price for limit orders)
        - origQty: Original order quantity
        - executedQty: Executed quantity
        - status: Order status (NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED, EXPIRED)
        - timeInForce: Time in force (GTC, IOC, FOK)
        - time: Order creation timestamp
        - updateTime: Last update timestamp
        - stopPrice: Stop price for stop orders (if applicable)
        - icebergQty: Iceberg quantity (if applicable)
        - clientOrderId: Client order ID

    Note:
        Only returns currently open orders (status NEW or PARTIALLY_FILLED).
    """
    logger.info(f"Fetching open orders{f' for {symbol}' if symbol else ' (all symbols)'}")

    try:
        # Fetch open orders from Binance API
        if symbol:
            orders = binance_client.get_open_orders(symbol=symbol)
            logger.info(f"Fetched {len(orders)} open orders for {symbol}")
        else:
            orders = binance_client.get_open_orders()
            logger.info(f"Fetched {len(orders)} open orders across all symbols")

        if not orders:
            logger.info("No open orders found")
            # Return empty DataFrame with expected columns
            return pd.DataFrame(columns=[
                'orderId', 'symbol', 'type', 'side', 'price', 'origQty',
                'executedQty', 'status', 'timeInForce', 'time', 'updateTime',
                'stopPrice', 'icebergQty', 'clientOrderId'
            ])

        # Process orders into records
        records = []
        for order in orders:
            records.append({
                'orderId': order['orderId'],
                'symbol': order['symbol'],
                'type': order['type'],
                'side': order['side'],
                'price': float(order['price']) if order['price'] != '0.00000000' else None,
                'origQty': float(order['origQty']),
                'executedQty': float(order['executedQty']),
                'status': order['status'],
                'timeInForce': order.get('timeInForce', ''),
                'time': datetime.fromtimestamp(order['time'] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                'updateTime': datetime.fromtimestamp(order['updateTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                'stopPrice': float(order['stopPrice']) if 'stopPrice' in order and order['stopPrice'] != '0.00000000' else None,
                'icebergQty': float(order['icebergQty']) if 'icebergQty' in order and order['icebergQty'] != '0.00000000' else None,
                'clientOrderId': order['clientOrderId']
            })

        # Create DataFrame
        df = pd.DataFrame(records)

        # Sort by time (most recent first)
        df = df.sort_values('time', ascending=False).reset_index(drop=True)

        logger.info(f"Successfully processed {len(df)} open orders")

        return df

    except Exception as e:
        logger.error(f"Error fetching open orders from Binance API: {e}")
        raise


def register_binance_get_open_orders(local_mcp_instance, local_binance_client, csv_dir, requests_dir):
    """Register the binance_get_open_orders tool"""
    @local_mcp_instance.tool()
    def binance_get_open_orders(requester: str, symbol: str = None) -> str:
        """
        Fetch all currently open orders from Binance and save to CSV file for analysis.

        This tool retrieves all pending orders (limit orders, stop-loss orders, OCO orders, etc.)
        that are currently active on your Binance account. Results are saved to CSV for detailed
        analysis using the py_eval tool.

        Parameters:
            requester (str): Identifier of who is calling this tool (e.g., 'trading-agent', 'user-alex').
                Used for request logging and audit purposes.
            symbol (string, optional): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT').
                If not provided, returns all open orders across all trading pairs.

        Returns:
            str: Formatted response with CSV file info, schema, sample data, and Python snippet to load the file.

        CSV Output Columns:
            - orderId (integer): Unique order identifier
            - symbol (string): Trading pair symbol (e.g., 'BTCUSDT')
            - type (string): Order type (LIMIT, MARKET, STOP_LOSS, STOP_LOSS_LIMIT, TAKE_PROFIT, TAKE_PROFIT_LIMIT, LIMIT_MAKER)
            - side (string): Order side (BUY or SELL)
            - price (float): Order price (limit price for limit orders, None for market orders)
            - origQty (float): Original order quantity
            - executedQty (float): Quantity already executed (for partially filled orders)
            - status (string): Order status (NEW, PARTIALLY_FILLED)
            - timeInForce (string): Time in force (GTC = Good Till Cancelled, IOC = Immediate Or Cancel, FOK = Fill Or Kill)
            - time (string): Order creation timestamp (YYYY-MM-DD HH:MM:SS)
            - updateTime (string): Last update timestamp (YYYY-MM-DD HH:MM:SS)
            - stopPrice (float): Stop price for stop orders (None if not applicable)
            - icebergQty (float): Iceberg quantity (None if not applicable)
            - clientOrderId (string): Client-assigned order ID

        Use Cases:
            - Monitor pending limit orders awaiting execution
            - Review active stop-loss and take-profit orders
            - Check OCO order status (both legs appear as separate orders)
            - Identify partially filled orders
            - Audit order placement times and parameters
            - Verify order status before cancellation
            - Track all active trading strategies

        Always use the py_eval tool to analyze the saved CSV file for insights such as:
            - Count of open orders by symbol, type, or side
            - Total value locked in open limit orders
            - Orders approaching execution price
            - Long-running orders that haven't filled
            - Partially filled orders requiring attention

        Example usage:
            # Get all open orders
            binance_get_open_orders()

            # Get open orders for specific symbol
            binance_get_open_orders(symbol="BTCUSDT")

        Note:
            - This is a READ-ONLY operation that does not modify any orders
            - Returns only currently open orders (not historical or filled orders)
            - For order history, use binance_spot_trade_history tool
            - OCO orders appear as two separate orders (limit order and stop order)
            - Results are sorted by creation time (most recent first)
        """
        logger.info(f"binance_get_open_orders tool invoked by {requester}{f' for {symbol}' if symbol else ''}")

        # Call fetch_open_orders function
        df = fetch_open_orders(binance_client=local_binance_client, symbol=symbol)

        if df.empty:
            return f"No open orders found{f' for {symbol}' if symbol else ' on your Binance account'}."

        # Generate filename with unique identifier
        symbol_prefix = f"{symbol}_" if symbol else "all_"
        filename = f"open_orders_{symbol_prefix}{str(uuid.uuid4())[:8]}.csv"
        filepath = csv_dir / filename

        # Save to CSV file
        df.to_csv(filepath, index=False)
        logger.info(f"Saved open orders to {filename} ({len(df)} orders)")

        # Return formatted response
        result = format_csv_response(filepath, df)

        # Log the request for audit trail
        log_request(
            requests_dir=requests_dir,
            requester=requester,
            tool_name="binance_get_open_orders",
            input_params={"symbol": symbol},
            output_result=result
        )

        return result
