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


@with_sentry_tracing("binance_cancel_futures_order")
def cancel_futures_order_operation(binance_client: Client, symbol: str, order_id: Optional[int] = None,
                                   cancel_all: bool = False) -> pd.DataFrame:
    """
    Cancel futures order(s) and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        order_id: Specific order ID to cancel (optional)
        cancel_all: If True, cancels all open orders for symbol (default: False)

    Returns:
        DataFrame with cancellation details

    Note:
        Uses futures_cancel_order API for futures-specific cancellation.
    """
    logger.info(f"Cancelling futures order(s) for {symbol}")

    # Validate parameters
    if not order_id and not cancel_all:
        raise ValueError("Must specify either order_id or cancel_all=True")

    if order_id and cancel_all:
        raise ValueError("Cannot specify both order_id and cancel_all=True")

    try:
        records = []

        if cancel_all:
            # Cancel all open futures orders for symbol
            logger.warning(f"⚠️  CANCELLING ALL OPEN FUTURES ORDERS for {symbol}")
            result = binance_client.futures_cancel_all_open_orders(symbol=symbol)

            # Result format is a dict with code and msg
            records.append({
                'operation': 'cancel_all',
                'symbol': symbol,
                'orderId': None,
                'status': 'ALL_CANCELLED',
                'code': result.get('code', 200),
                'msg': result.get('msg', 'Success'),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            logger.info(f"Cancelled all open futures orders for {symbol}")

        elif order_id:
            # Cancel single futures order
            logger.warning(f"⚠️  CANCELLING FUTURES ORDER {order_id} for {symbol}")
            result = binance_client.futures_cancel_order(
                symbol=symbol,
                orderId=order_id
            )

            records.append({
                'operation': 'cancel_single',
                'symbol': result['symbol'],
                'orderId': result['orderId'],
                'status': result['status'],
                'code': 200,
                'msg': 'Success',
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            logger.info(f"Cancelled futures order {order_id}")

        # Create DataFrame
        df = pd.DataFrame(records)

        return df

    except Exception as e:
        logger.error(f"Error cancelling futures order: {e}")
        raise


def register_binance_cancel_futures_order(local_mcp_instance, local_binance_client, csv_dir, requests_dir):
    """Register the binance_cancel_futures_order tool"""
    @local_mcp_instance.tool()
    def binance_cancel_futures_order(requester: str, symbol: str, order_id: Optional[int] = None, cancel_all: bool = False) -> str:
        """
        Cancel one or all futures orders for a trading pair and save cancellation details to CSV.

        This tool allows you to cancel individual futures orders or all open orders for a
        specific symbol. Use this when you need to modify your strategy, close unfilled
        orders, or free up locked margin.

        ⚠️  WARNING: THIS CANCELS REAL FUTURES ORDERS ⚠️
        Cancelled orders cannot be restored. Verify order_id before cancelling.

        Parameters:
            requester (string, required): Name of the requester making this call (for request logging)
            symbol (string, required): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
            order_id (integer, optional): Specific order ID to cancel (get from binance_get_futures_open_orders)
            cancel_all (boolean, optional): If True, cancels ALL open futures orders for symbol (default: False)

        Returns:
            str: Formatted response with CSV file containing cancellation confirmation.

        CSV Output Columns:
            - operation (string): Type of cancellation (cancel_single or cancel_all)
            - symbol (string): Trading pair symbol
            - orderId (integer): Order ID for single cancellation (None for cancel_all)
            - status (string): Cancellation status (CANCELED, ALL_CANCELLED)
            - code (integer): Response code (200 = success)
            - msg (string): Response message
            - timestamp (string): Cancellation timestamp

        Parameter Rules:
            - Must specify EXACTLY ONE of: order_id or cancel_all=True
            - Cannot combine order_id and cancel_all
            - symbol is always required

        Cancellation Types:

        1. Cancel Single Order:
           - Use order_id parameter
           - Cancels one specific futures order
           - Order must be in NEW or PARTIALLY_FILLED status
           - Frees up locked margin from that order

        2. Cancel All Orders:
           - Use cancel_all=True parameter
           - Cancels ALL open futures orders for the symbol
           - Includes limit orders, stop orders, and take-profit orders
           - Use with extreme caution - cannot be undone!

        Use Cases:
            - Cancel unfilled limit order that's no longer wanted
            - Remove stop-loss orders when manually closing position
            - Clean up multiple stale orders at once
            - Free up locked margin for other trades
            - Modify strategy by cancelling and placing new orders
            - Emergency closure of all pending orders before major news

        What Happens When You Cancel:
            - Order immediately removed from order book
            - Locked margin is freed and becomes available
            - No trading fees charged (fees only on executed trades)
            - Order cannot be restored after cancellation
            - For partially filled orders, only unfilled portion is cancelled
            - Position (if any) remains unchanged - only pending orders cancelled

        Futures vs Spot Cancellation:
            - Futures cancellation uses different API endpoint
            - Frees margin instead of base/quote assets
            - Can cancel reduce-only orders
            - Can cancel orders with position_side parameter

        Example usage:
            # Cancel a specific futures order
            binance_cancel_futures_order(symbol="BTCUSDT", order_id=12345678)

            # Cancel all open futures orders for ETHUSDT
            binance_cancel_futures_order(symbol="ETHUSDT", cancel_all=True)

        Before Cancelling:
            - Check order status with binance_get_futures_open_orders
            - Verify order_id is correct
            - Consider if you really want to cancel
            - For cancel_all, be absolutely certain - ALL orders will be cancelled

        After Cancelling:
            - Verify with binance_get_futures_open_orders (should not show cancelled orders)
            - Check binance_get_futures_balances to confirm margin freed
            - CSV file saved for your cancellation records
            - Consider if you need to place replacement orders

        Risk Management Considerations:
            - Cancelling stop-loss orders removes your protection
            - Cancelling take-profit orders may prevent profit-taking
            - cancel_all removes ALL protective orders at once
            - After cancelling protective orders, monitor position manually
            - Consider replacing cancelled orders with new ones

        Common Errors:
            - "Unknown order": Order already filled, cancelled, or doesn't exist
            - "Invalid symbol": Symbol name incorrect
            - "Order does not exist": Wrong order_id for that symbol
            - Network errors: Retry if cancellation didn't complete

        Position Impact:
            - Cancelling orders does NOT close positions
            - Positions remain open after order cancellation
            - To close position, use binance_manage_futures_positions
            - To close position AND cancel orders, do both operations

        Example Workflow:
            1. Check open orders: binance_get_futures_open_orders(symbol="BTCUSDT")
            2. Review which orders to cancel
            3. Cancel specific order: binance_cancel_futures_order(symbol="BTCUSDT", order_id=123)
            4. Verify cancellation: binance_get_futures_open_orders(symbol="BTCUSDT")
            5. Check freed margin: binance_get_futures_balances()

        CRITICAL Safety Notes:
            - This operation is immediate and cannot be undone
            - Cancelled orders don't appear in open orders list
            - Partially filled: Executed portion remains, unfilled cancelled
            - Margin locked in cancelled orders becomes immediately available
            - cancel_all is very powerful - use with extreme caution
            - Always verify before using cancel_all

        Note:
            - CSV file saved for audit trail
            - Works for all futures order types (LIMIT, STOP, TAKE_PROFIT, etc.)
            - Different from spot order cancellation API
            - Can cancel orders in NEW or PARTIALLY_FILLED status only
            - Filled orders cannot be cancelled (they're already executed)
        """
        logger.info(f"binance_cancel_futures_order tool invoked for {symbol} by {requester}")

        # Validate parameters
        if not symbol:
            return "Error: symbol is required (e.g., 'BTCUSDT')"

        if not order_id and not cancel_all:
            return "Error: Must specify either order_id or cancel_all=True"

        if order_id and cancel_all:
            return "Error: Cannot specify both order_id and cancel_all=True"

        try:
            # Execute cancellation
            df = cancel_futures_order_operation(
                binance_client=local_binance_client,
                symbol=symbol,
                order_id=order_id,
                cancel_all=cancel_all
            )

            # Generate filename
            operation_type = df.iloc[0]['operation']
            filename = f"cancel_futures_{operation_type}_{symbol}_{str(uuid.uuid4())[:8]}.csv"
            filepath = csv_dir / filename

            # Save to CSV
            df.to_csv(filepath, index=False)
            logger.info(f"Saved futures cancellation to {filename}")

            # Return formatted response
            result = format_csv_response(filepath, df)

            # Log request
            log_request(
                requests_dir=requests_dir,
                requester=requester,
                tool_name="binance_cancel_futures_order",
                input_params={
                    "symbol": symbol,
                    "order_id": order_id,
                    "cancel_all": cancel_all
                },
                output_result=result
            )

            # Add cancellation summary
            cancel_data = df.iloc[0]

            summary = f"""

═══════════════════════════════════════════════════════════════════════════════
FUTURES ORDER(S) CANCELLED
═══════════════════════════════════════════════════════════════════════════════
Operation:       {cancel_data['operation'].replace('_', ' ').title()}
Symbol:          {cancel_data['symbol']}
"""

            if cancel_data['orderId']:
                summary += f"Order ID:        {int(cancel_data['orderId'])}\n"

            summary += f"""Status:          {cancel_data['status']}
Code:            {int(cancel_data['code'])}
Message:         {cancel_data['msg']}
Time:            {cancel_data['timestamp']}
═══════════════════════════════════════════════════════════════════════════════

The cancelled order(s) have been removed from your futures account.
Locked margin has been freed and is now available for trading.

Verify cancellation:
  binance_get_futures_open_orders(symbol="{cancel_data['symbol']}")

Check freed margin:
  binance_get_futures_balances()
"""

            if cancel_data['operation'] == 'cancel_all':
                summary += """
⚠️  IMPORTANT: All open orders for this symbol have been cancelled.
This includes stop-loss and take-profit orders!
Monitor your positions manually or set new protective orders.
"""

            summary += "═══════════════════════════════════════════════════════════════════════════════\n"

            return result + summary

        except ValueError as e:
            logger.error(f"Validation error: {e}")
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Error cancelling futures order: {e}")
            error_msg = str(e)

            # Provide helpful error messages
            if "Unknown order" in error_msg or "does not exist" in error_msg:
                return f"Error: Order not found.\n\nPossible reasons:\n- Order already filled or cancelled\n- Wrong order_id for this symbol\n- Order_id belongs to different symbol\n\nCheck open orders with:\n  binance_get_futures_open_orders(symbol=\"{symbol}\")"
            else:
                return f"Error: {error_msg}\n\nCheck:\n- API credentials valid\n- Symbol correct\n- Order ID correct\n- Order still exists and is cancellable"
