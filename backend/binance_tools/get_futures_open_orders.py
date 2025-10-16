import logging
from datetime import datetime
import uuid
from mcp_service import format_csv_response
import pandas as pd
from binance.client import Client
from typing import Optional
from sentry_utils import with_sentry_tracing

logger = logging.getLogger(__name__)


@with_sentry_tracing("binance_get_futures_open_orders")
def fetch_futures_open_orders(binance_client: Client, symbol: Optional[str] = None) -> pd.DataFrame:
    """
    Fetch open futures orders and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        symbol: Optional trading pair symbol to filter (e.g., 'BTCUSDT')

    Returns:
        DataFrame with open futures orders

    Note:
        Returns all pending futures orders that haven't been filled or cancelled.
    """
    logger.info(f"Fetching futures open orders" + (f" for {symbol}" if symbol else ""))

    try:
        # Fetch open orders
        if symbol:
            orders = binance_client.futures_get_open_orders(symbol=symbol)
        else:
            orders = binance_client.futures_get_open_orders()

        records = []
        for order in orders:
            records.append({
                'orderId': order['orderId'],
                'clientOrderId': order['clientOrderId'],
                'symbol': order['symbol'],
                'side': order['side'],
                'positionSide': order['positionSide'],
                'type': order['type'],
                'timeInForce': order['timeInForce'],
                'price': float(order['price']) if order['price'] else None,
                'stopPrice': float(order['stopPrice']) if order['stopPrice'] else None,
                'origQty': float(order['origQty']),
                'executedQty': float(order['executedQty']),
                'status': order['status'],
                'reduceOnly': order['reduceOnly'],
                'closePosition': order['closePosition'],
                'workingType': order.get('workingType', 'CONTRACT_PRICE'),
                'priceProtect': order.get('priceProtect', False),
                'time': datetime.fromtimestamp(order['time'] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                'updateTime': datetime.fromtimestamp(order['updateTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
            })

        df = pd.DataFrame(records) if records else pd.DataFrame(columns=[
            'orderId', 'clientOrderId', 'symbol', 'side', 'positionSide', 'type', 'timeInForce',
            'price', 'stopPrice', 'origQty', 'executedQty', 'status', 'reduceOnly',
            'closePosition', 'workingType', 'priceProtect', 'time', 'updateTime'
        ])

        logger.info(f"Retrieved {len(records)} open futures orders")

        return df

    except Exception as e:
        logger.error(f"Error fetching futures open orders: {e}")
        raise


def register_binance_get_futures_open_orders(local_mcp_instance, local_binance_client, csv_dir):
    """Register the binance_get_futures_open_orders tool"""
    @local_mcp_instance.tool()
    def binance_get_futures_open_orders(symbol: Optional[str] = None) -> str:
        """
        Fetch all open futures orders and save to CSV for analysis.

        This tool retrieves all pending futures orders that haven't been filled or cancelled yet.
        Use it to monitor your active orders, check order status, and manage your trading strategy.

        ✓ READ-ONLY OPERATION - Safe to run anytime

        Parameters:
            symbol (string, optional): Trading pair symbol (e.g., 'BTCUSDT')
                - If provided: Shows only open orders for this symbol
                - If omitted: Shows all open futures orders across all symbols

        Returns:
            str: Formatted response with CSV file containing all open futures orders.

        CSV Output Columns:
            - orderId (integer): Unique order identifier
            - clientOrderId (string): Client-assigned order ID
            - symbol (string): Trading pair (e.g., 'BTCUSDT')
            - side (string): Order side (BUY or SELL)
            - positionSide (string): BOTH, LONG, or SHORT
            - type (string): Order type (LIMIT, MARKET, STOP, STOP_MARKET, TAKE_PROFIT, etc.)
            - timeInForce (string): Time in force (GTC, IOC, FOK, GTX)
            - price (float): Limit price (None for market orders)
            - stopPrice (float): Stop trigger price (None for non-stop orders)
            - origQty (float): Original order quantity
            - executedQty (float): Quantity already executed (for partial fills)
            - status (string): Order status (NEW, PARTIALLY_FILLED)
            - reduceOnly (boolean): True if order can only reduce position size
            - closePosition (boolean): True if order closes entire position
            - workingType (string): Price type used for stop orders (CONTRACT_PRICE or MARK_PRICE)
            - priceProtect (boolean): True if price protection enabled
            - time (string): Order creation timestamp
            - updateTime (string): Last update timestamp

        Order Status Meanings:
            - NEW: Order is active and waiting to be filled
            - PARTIALLY_FILLED: Part of the order has executed, remainder still active

        Order Types:
            - LIMIT: Order executes at specified price or better
            - MARKET: Order executes immediately at current market price
            - STOP: Stop-loss order triggered by stop price
            - STOP_MARKET: Stop order that executes as market order
            - TAKE_PROFIT: Take-profit order triggered by stop price
            - TAKE_PROFIT_MARKET: Take-profit that executes as market order

        Use Cases:
            - Monitor all pending futures orders
            - Check if limit orders have been filled
            - Verify order parameters before cancellation
            - Track partially filled orders
            - Review stop-loss and take-profit orders
            - Audit active trading strategy
            - Identify stale orders that need cancellation

        Example usage:
            # View all open futures orders
            binance_get_futures_open_orders()

            # View open orders for specific symbol
            binance_get_futures_open_orders(symbol="BTCUSDT")

        Analysis with py_eval:
            - Sort by symbol to group orders by trading pair
            - Filter by type to see only limit orders or stop orders
            - Calculate total locked margin across all orders
            - Identify oldest orders that may need review
            - Find partially filled orders
            - Check for duplicate or conflicting orders

        Order Management Workflow:
            1. Check open orders: binance_get_futures_open_orders()
            2. Review order details and status
            3. Cancel unwanted orders: binance_cancel_futures_order()
            4. Place new orders if strategy changed

        When to Check Open Orders:
            - Before placing new orders (avoid conflicts)
            - After market volatility (check if stops triggered)
            - Daily trading review
            - Before changing position strategy
            - When checking why balance is locked
            - After partial fills to see remaining quantity

        Important Notes:
            - Only shows unfilled or partially filled orders
            - Filled orders don't appear here (use trade history instead)
            - Cancelled orders also don't appear
            - Orders remain until filled, cancelled, or expired
            - IOC/FOK orders typically don't appear if they didn't fill
            - Check regularly to avoid forgetting about old orders

        Related Tools:
            - Cancel orders: binance_cancel_futures_order(symbol, order_id)
            - Place limit order: binance_futures_limit_order(...)
            - View positions: binance_manage_futures_positions()
            - Check account: binance_get_futures_balances()

        Note:
            - Completely safe READ-ONLY operation
            - Run as frequently as needed
            - No API rate limit concerns for reasonable usage
            - CSV file saved for record keeping and analysis
        """
        logger.info(f"binance_get_futures_open_orders tool invoked")

        try:
            # Fetch open orders
            df = fetch_futures_open_orders(
                binance_client=local_binance_client,
                symbol=symbol
            )

            # Generate filename
            if symbol:
                filename = f"futures_open_orders_{symbol}_{str(uuid.uuid4())[:8]}.csv"
            else:
                filename = f"futures_open_orders_all_{str(uuid.uuid4())[:8]}.csv"

            filepath = csv_dir / filename

            # Save to CSV
            df.to_csv(filepath, index=False)
            logger.info(f"Saved futures open orders to {filename}")

            # Return formatted response
            result = format_csv_response(filepath, df)

            if df.empty:
                summary = f"""

═══════════════════════════════════════════════════════════════════════════════
NO OPEN FUTURES ORDERS
═══════════════════════════════════════════════════════════════════════════════
"""
                if symbol:
                    summary += f"No open orders found for {symbol}.\n"
                else:
                    summary += "No open futures orders found.\n"
                summary += "\nAll your orders have either been filled or cancelled.\n"
                summary += "═══════════════════════════════════════════════════════════════════════════════\n"
                return result + summary

            # Calculate summary statistics
            total_orders = len(df)
            by_type = df['type'].value_counts().to_dict()
            by_status = df['status'].value_counts().to_dict()
            partially_filled = len(df[df['status'] == 'PARTIALLY_FILLED'])

            summary = f"""

═══════════════════════════════════════════════════════════════════════════════
FUTURES OPEN ORDERS SUMMARY
═══════════════════════════════════════════════════════════════════════════════
Total Open Orders:   {total_orders}
Partially Filled:    {partially_filled}

By Order Type:
"""

            for order_type, count in sorted(by_type.items()):
                summary += f"  {order_type}: {count}\n"

            summary += "\nBy Status:\n"
            for status, count in sorted(by_status.items()):
                summary += f"  {status}: {count}\n"

            # List unique symbols
            symbols = df['symbol'].unique()
            summary += f"\nSymbols with Open Orders:\n"
            for sym in sorted(symbols):
                sym_count = len(df[df['symbol'] == sym])
                summary += f"  {sym}: {sym_count} order(s)\n"

            summary += """
═══════════════════════════════════════════════════════════════════════════════

To cancel an order:
  binance_cancel_futures_order(symbol="SYMBOL", order_id=ORDER_ID)

To cancel all orders for a symbol:
  binance_cancel_futures_order(symbol="SYMBOL", cancel_all=True)

═══════════════════════════════════════════════════════════════════════════════
"""

            return result + summary

        except Exception as e:
            logger.error(f"Error fetching futures open orders: {e}")
            return f"Error: {str(e)}\n\nCheck:\n- API credentials valid\n- Futures trading enabled\n- Network connectivity"
