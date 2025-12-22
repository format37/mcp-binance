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


@with_sentry_tracing("binance_cancel_order")
def cancel_order_operation(binance_client: Client, symbol: str, order_id: Optional[int] = None,
                          order_list_id: Optional[int] = None, cancel_all: bool = False) -> pd.DataFrame:
    """
    Cancel order(s) on Binance and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        order_id: Specific order ID to cancel (optional)
        order_list_id: OCO order list ID to cancel (optional)
        cancel_all: If True, cancels all open orders for symbol (default: False)

    Returns:
        DataFrame with cancellation details containing columns:
        - operation: Type of operation (cancel_single, cancel_oco, cancel_all)
        - symbol: Trading pair symbol
        - orderId: Order ID (for single cancel)
        - orderListId: Order list ID (for OCO cancel)
        - status: Cancellation status
        - cancelledCount: Number of orders cancelled (for cancel_all)

    Note:
        Only one of order_id, order_list_id, or cancel_all should be specified.
    """
    logger.info(f"Cancelling order(s) for {symbol}")

    # Validate parameters
    specified_params = sum([bool(order_id), bool(order_list_id), cancel_all])
    if specified_params != 1:
        raise ValueError("Must specify exactly one of: order_id, order_list_id, or cancel_all=True")

    try:
        records = []

        if cancel_all:
            # Cancel all open orders for symbol
            logger.warning(f"⚠️  CANCELLING ALL OPEN ORDERS for {symbol}")
            result = binance_client.cancel_all_open_orders(symbol=symbol)

            # Result format varies - may be a list or dict
            cancelled_count = len(result) if isinstance(result, list) else 1

            records.append({
                'operation': 'cancel_all',
                'symbol': symbol,
                'orderId': None,
                'orderListId': None,
                'status': 'ALL_CANCELLED',
                'cancelledCount': cancelled_count,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            logger.info(f"Cancelled all {cancelled_count} open orders for {symbol}")

        elif order_list_id:
            # Cancel OCO order
            logger.warning(f"⚠️  CANCELLING OCO ORDER {order_list_id} for {symbol}")
            result = binance_client.cancel_oco_order(
                symbol=symbol,
                orderListId=order_list_id
            )

            records.append({
                'operation': 'cancel_oco',
                'symbol': result['symbol'],
                'orderId': None,
                'orderListId': result['orderListId'],
                'status': result['listOrderStatus'],
                'cancelledCount': len(result.get('orderReports', [])),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            logger.info(f"Cancelled OCO order {order_list_id}")

        elif order_id:
            # Cancel single order
            logger.warning(f"⚠️  CANCELLING ORDER {order_id} for {symbol}")
            result = binance_client.cancel_order(
                symbol=symbol,
                orderId=order_id
            )

            records.append({
                'operation': 'cancel_single',
                'symbol': result['symbol'],
                'orderId': result['orderId'],
                'orderListId': None,
                'status': result['status'],
                'cancelledCount': 1,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            logger.info(f"Cancelled order {order_id}")

        # Create DataFrame
        df = pd.DataFrame(records)

        return df

    except Exception as e:
        logger.error(f"Error cancelling order: {e}")
        raise


def register_binance_cancel_order(local_mcp_instance, local_binance_client, csv_dir, requests_dir):
    """Register the binance_cancel_order tool"""
    @local_mcp_instance.tool()
    def binance_cancel_order(requester: str, symbol: str, order_id: int = None, order_list_id: int = None, cancel_all: bool = False) -> str:
        """
        Cancel one or more orders on Binance spot market and save cancellation details to CSV.

        This tool allows you to cancel individual orders, OCO orders, or all open orders for a
        specific trading pair. Use this when you need to modify your strategy, close unfilled
        orders, or free up locked balance.

        ⚠️  WARNING: THIS CANCELS REAL ORDERS ⚠️
        Cancelled orders cannot be restored. Make sure you're cancelling the correct order(s).

        Parameters:
            requester (string, required): Name/ID of the user making the request
            symbol (string, required): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
            order_id (integer, optional): Specific order ID to cancel (get from binance_get_open_orders)
            order_list_id (integer, optional): OCO order list ID to cancel entire OCO order
            cancel_all (boolean, optional): If True, cancels ALL open orders for the symbol (default: False)

        Returns:
            str: Formatted response with CSV file containing cancellation confirmation, including
                operation type, affected orders, and status.

        CSV Output Columns:
            - operation (string): Type of cancellation (cancel_single, cancel_oco, cancel_all)
            - symbol (string): Trading pair symbol (e.g., 'BTCUSDT')
            - orderId (integer): Order ID for single cancellation (None for other types)
            - orderListId (integer): Order list ID for OCO cancellation (None for other types)
            - status (string): Cancellation status (CANCELED, ALL_CANCELLED, etc.)
            - cancelledCount (integer): Number of orders cancelled
            - timestamp (string): Cancellation timestamp (YYYY-MM-DD HH:MM:SS)

        Parameter Rules:
            - Must specify EXACTLY ONE of: order_id, order_list_id, or cancel_all=True
            - Cannot combine multiple cancellation types in one call
            - symbol is always required

        Cancellation Types:

        1. Cancel Single Order:
           - Use order_id parameter
           - Cancels one specific order
           - Order must be in NEW or PARTIALLY_FILLED status
           - Frees up locked balance from that order

        2. Cancel OCO Order:
           - Use order_list_id parameter
           - Cancels entire OCO order (both legs)
           - Both take-profit and stop-loss orders are cancelled
           - Use order list ID from binance_spot_oco_order response

        3. Cancel All Orders:
           - Use cancel_all=True parameter
           - Cancels ALL open orders for the specified symbol
           - Includes limit orders, stop orders, and OCO orders
           - Use with caution - cannot be undone

        Use Cases:
            - Cancel unfilled limit order that's no longer wanted
            - Close OCO order when manually closing position
            - Clean up multiple stale orders at once
            - Free up locked balance for other trades
            - Modify strategy by cancelling and placing new orders
            - Emergency closure of all pending orders

        What Happens When You Cancel:
            - Order is immediately removed from order book
            - Locked balance is freed and becomes available again
            - No trading fees charged (fees only on executed trades)
            - Order cannot be restored after cancellation
            - For partially filled orders, only unfilled portion is cancelled

        Example usage:
            # Cancel a specific order
            binance_cancel_order(symbol="BTCUSDT", order_id=12345678)

            # Cancel an OCO order
            binance_cancel_order(symbol="ETHUSDT", order_list_id=9876)

            # Cancel all open orders for BTCUSDT
            binance_cancel_order(symbol="BTCUSDT", cancel_all=True)

        Before Cancelling:
            - Check order status with binance_get_open_orders
            - Verify order_id or order_list_id is correct
            - Consider if you really want to cancel
            - For cancel_all, be absolutely certain

        After Cancelling:
            - Verify with binance_get_open_orders (should not show cancelled orders)
            - Check binance_get_account to confirm balance freed
            - CSV file saved for your cancellation records

        Common Errors:
            - "Unknown order": Order already filled, cancelled, or doesn't exist
            - "Invalid symbol": Symbol name incorrect
            - "Order does not exist": Wrong order_id for that symbol
            - Check error message for specific issue

        Note:
            - This operation is immediate and cannot be undone
            - Cancelled orders don't appear in open orders list
            - Partially filled orders: Executed portion remains, unfilled portion cancelled
            - Balance locked in cancelled orders becomes immediately available
            - CSV file saved for audit trail and record keeping
            - Can be used to implement order modification (cancel + new order)
        """
        logger.info(f"binance_cancel_order tool invoked for {symbol} by {requester}")

        # Validate parameters
        if not symbol:
            return "Error: symbol is required (e.g., 'BTCUSDT')"

        # Count specified parameters
        specified = sum([bool(order_id), bool(order_list_id), cancel_all])
        if specified == 0:
            return "Error: Must specify one of: order_id, order_list_id, or cancel_all=True"
        if specified > 1:
            return "Error: Can only specify ONE of: order_id, order_list_id, or cancel_all=True"

        try:
            # Execute cancellation
            df = cancel_order_operation(
                binance_client=local_binance_client,
                symbol=symbol,
                order_id=order_id,
                order_list_id=order_list_id,
                cancel_all=cancel_all
            )

            # Generate filename with unique identifier
            operation_type = df.iloc[0]['operation']
            filename = f"cancel_{operation_type}_{symbol}_{str(uuid.uuid4())[:8]}.csv"
            filepath = csv_dir / filename

            # Save to CSV file
            df.to_csv(filepath, index=False)
            logger.info(f"Saved cancellation to {filename}")

            # Log the request
            # Return formatted response
            result = format_csv_response(filepath, df)

            # Log request
            log_request(
                requests_dir=requests_dir,
                requester=requester,
                tool_name='binance_cancel_order',
                input_params={
                    'symbol': symbol,
                    'order_id': order_id,
                    'order_list_id': order_list_id,
                    'cancel_all': cancel_all
                },
                output_result=result
            )

            # Add cancellation summary to response
            cancel_data = df.iloc[0]

            summary = f"""

═══════════════════════════════════════════════════════════════════════════════
ORDER(S) CANCELLED SUCCESSFULLY
═══════════════════════════════════════════════════════════════════════════════
Operation:       {cancel_data['operation'].replace('_', ' ').title()}
Symbol:          {cancel_data['symbol']}
"""

            if cancel_data['orderId']:
                summary += f"Order ID:        {int(cancel_data['orderId'])}\n"

            if cancel_data['orderListId']:
                summary += f"Order List ID:   {int(cancel_data['orderListId'])}\n"

            summary += f"""Status:          {cancel_data['status']}
Cancelled:       {int(cancel_data['cancelledCount'])} order(s)
Time:            {cancel_data['timestamp']}
═══════════════════════════════════════════════════════════════════════════════

The cancelled order(s) have been removed from your account.
Locked balance has been freed and is now available for trading.

Verify cancellation:
  binance_get_open_orders(symbol="{cancel_data['symbol']}")

Check freed balance:
  binance_get_account()
═══════════════════════════════════════════════════════════════════════════════
"""

            return result + summary

        except ValueError as e:
            logger.error(f"Validation error: {e}")
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Error cancelling order: {e}")
            error_msg = str(e)

            # Provide helpful error messages
            if "Unknown order" in error_msg or "does not exist" in error_msg:
                return f"Error: Order not found.\n\nPossible reasons:\n- Order already filled or cancelled\n- Wrong order_id for this symbol\n- Order_id belongs to different symbol\n\nCheck open orders with:\n  binance_get_open_orders(symbol=\"{symbol}\")"
            else:
                return f"Error cancelling order: {error_msg}\n\nPlease check:\n- API credentials are valid\n- Symbol is correct\n- Order ID or Order List ID is correct\n- Order still exists and is cancellable\n- API key has trading permissions"
