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


@with_sentry_tracing("binance_cancel_algo_order")
def cancel_algo_order_operation(binance_client: Client, symbol: str, algo_id: Optional[int] = None,
                                client_algo_id: Optional[str] = None, cancel_all: bool = False) -> pd.DataFrame:
    """
    Cancel algo/conditional futures order(s) and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        algo_id: Algo order ID to cancel (from binance_get_futures_conditional_orders)
        client_algo_id: Client-assigned algo order ID to cancel
        cancel_all: If True, cancels all open algo orders for symbol (default: False)

    Returns:
        DataFrame with cancellation details

    Note:
        Uses Binance Algo Service endpoints:
        - DELETE /fapi/v1/algoOrder for single order
        - DELETE /fapi/v1/allOpenAlgoOrders for all orders

        As of December 9, 2025, conditional orders (STOP_MARKET, TAKE_PROFIT_MARKET,
        TRAILING_STOP_MARKET) are managed by the Algo Service and use algoId, not orderId.
    """
    logger.info(f"Cancelling algo order(s) for {symbol}")

    # Validate parameters
    has_identifier = algo_id is not None or client_algo_id is not None
    if not has_identifier and not cancel_all:
        raise ValueError("Must specify algo_id, client_algo_id, or cancel_all=True")

    if has_identifier and cancel_all:
        raise ValueError("Cannot specify both an identifier (algo_id/client_algo_id) and cancel_all=True")

    if algo_id is not None and client_algo_id is not None:
        raise ValueError("Cannot specify both algo_id and client_algo_id - use one identifier")

    try:
        records = []

        if cancel_all:
            # Cancel all open algo orders for symbol
            # DELETE /fapi/v1/allOpenAlgoOrders
            logger.warning(f"CANCELLING ALL OPEN ALGO ORDERS for {symbol}")
            result = binance_client._request_futures_api(
                'delete', 'allOpenAlgoOrders', signed=True, data={'symbol': symbol}
            )

            # Result format: {"code": "000000", "msg": "success", "data": {...}}
            records.append({
                'operation': 'cancel_all_algo',
                'symbol': symbol,
                'algoId': None,
                'clientAlgoId': None,
                'status': 'ALL_CANCELLED',
                'code': result.get('code', '000000'),
                'msg': result.get('msg', 'success'),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            logger.info(f"Cancelled all open algo orders for {symbol}")

        elif algo_id is not None:
            # Cancel single algo order by algoId
            # DELETE /fapi/v1/algoOrder
            logger.warning(f"CANCELLING ALGO ORDER {algo_id} for {symbol}")
            result = binance_client._request_futures_api(
                'delete', 'algoOrder', signed=True, data={'algoId': algo_id}
            )

            # Result format: {"code": "000000", "msg": "success", "data": {...}}
            data = result.get('data', {})
            records.append({
                'operation': 'cancel_single_algo',
                'symbol': symbol,
                'algoId': data.get('algoId', algo_id),
                'clientAlgoId': data.get('clientAlgoId', ''),
                'status': data.get('algoStatus', 'CANCELLED'),
                'code': result.get('code', '000000'),
                'msg': result.get('msg', 'success'),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            logger.info(f"Cancelled algo order {algo_id}")

        elif client_algo_id is not None:
            # Cancel single algo order by clientAlgoId
            # DELETE /fapi/v1/algoOrder
            logger.warning(f"CANCELLING ALGO ORDER with clientAlgoId={client_algo_id} for {symbol}")
            result = binance_client._request_futures_api(
                'delete', 'algoOrder', signed=True, data={'clientAlgoId': client_algo_id}
            )

            # Result format: {"code": "000000", "msg": "success", "data": {...}}
            data = result.get('data', {})
            records.append({
                'operation': 'cancel_single_algo',
                'symbol': symbol,
                'algoId': data.get('algoId', ''),
                'clientAlgoId': data.get('clientAlgoId', client_algo_id),
                'status': data.get('algoStatus', 'CANCELLED'),
                'code': result.get('code', '000000'),
                'msg': result.get('msg', 'success'),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            logger.info(f"Cancelled algo order with clientAlgoId={client_algo_id}")

        # Create DataFrame
        df = pd.DataFrame(records)

        return df

    except Exception as e:
        logger.error(f"Error cancelling algo order: {e}")
        raise


def register_binance_cancel_algo_order(local_mcp_instance, local_binance_client, csv_dir, requests_dir):
    """Register the binance_cancel_algo_order tool"""
    @local_mcp_instance.tool()
    def binance_cancel_algo_order(requester: str, symbol: str, algo_id: Optional[int] = None,
                                  client_algo_id: Optional[str] = None, cancel_all: bool = False) -> str:
        """
        Cancel conditional/algo futures orders (TP/SL/Trailing) and save cancellation details to CSV.

        This tool cancels algo orders managed by the Binance Algo Service. As of December 9, 2025,
        conditional orders (STOP_MARKET, TAKE_PROFIT_MARKET, TRAILING_STOP_MARKET) use different
        API endpoints than basic orders:

        - Basic orders (LIMIT, MARKET): Use binance_cancel_futures_order() with orderId
        - Conditional orders (TP/SL/Trailing): Use THIS tool with algoId

        WARNING: THIS CANCELS REAL FUTURES ALGO ORDERS
        Cancelled orders cannot be restored. Verify algo_id before cancelling.

        Parameters:
            requester (string, required): Name of the requester making this call (for request logging)
            symbol (string, required): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
            algo_id (integer, optional): Algo order ID to cancel (get from binance_get_futures_conditional_orders)
            client_algo_id (string, optional): Client-assigned algo order ID to cancel
            cancel_all (boolean, optional): If True, cancels ALL open algo orders for symbol (default: False)

        Returns:
            str: Formatted response with CSV file containing cancellation confirmation.

        CSV Output Columns:
            - operation (string): Type of cancellation (cancel_single_algo or cancel_all_algo)
            - symbol (string): Trading pair symbol
            - algoId (integer): Algo order ID (None for cancel_all)
            - clientAlgoId (string): Client algo order ID
            - status (string): Cancellation status (CANCELLED, ALL_CANCELLED)
            - code (string): Response code ('000000' = success)
            - msg (string): Response message
            - timestamp (string): Cancellation timestamp

        Parameter Rules:
            - Must specify EXACTLY ONE of: algo_id, client_algo_id, or cancel_all=True
            - Cannot combine identifiers or use with cancel_all
            - symbol is always required

        Where to Get algo_id:
            Run binance_get_futures_conditional_orders(symbol="BTCUSDT") first.
            The 'algoId' column contains the IDs needed for this tool.

            DO NOT use 'orderId' from binance_get_futures_open_orders() - that's for basic orders!

        Why Two Cancellation Tools?

            Binance API change (December 9, 2025):
            - binance_cancel_futures_order() -> Uses DELETE /fapi/v1/order with orderId
            - binance_cancel_algo_order() -> Uses DELETE /fapi/v1/algoOrder with algoId

            If you get error -4130 "Order not found" when trying to cancel a conditional
            order with binance_cancel_futures_order, use this tool instead!

        Order Types This Tool Handles:
            - STOP_MARKET: Stop-loss orders
            - TAKE_PROFIT_MARKET: Take-profit orders
            - TRAILING_STOP_MARKET: Trailing stop orders

            For basic LIMIT and MARKET orders, use binance_cancel_futures_order() instead.

        Use Cases:
            - Cancel unfilled stop-loss order to set a new one
            - Remove take-profit orders when changing strategy
            - Clear all protective orders before manually closing position
            - Cancel trailing stop to lock in profits manually
            - Clean up stale conditional orders

        What Happens When You Cancel:
            - Algo order immediately removed from algo order book
            - No locked margin to free (algo orders don't lock margin)
            - Order cannot be restored after cancellation
            - Position (if any) remains unchanged - only algo order cancelled
            - You may need to place new protective orders

        Example usage:
            # First, get the algo order IDs
            binance_get_futures_conditional_orders(symbol="BTCUSDT")
            # Returns algoId: 1000000409355876

            # Cancel a specific algo order
            binance_cancel_algo_order(symbol="BTCUSDT", algo_id=1000000409355876)

            # Cancel using client-assigned ID
            binance_cancel_algo_order(symbol="BTCUSDT", client_algo_id="my_sl_order_001")

            # Cancel all algo orders for ETHUSDT (use with caution!)
            binance_cancel_algo_order(symbol="ETHUSDT", cancel_all=True)

        Workflow Example:
            1. Check conditional orders: binance_get_futures_conditional_orders(symbol="BTCUSDT")
            2. Note the algoId you want to cancel
            3. Cancel specific order: binance_cancel_algo_order(symbol="BTCUSDT", algo_id=123456)
            4. Verify cancellation: binance_get_futures_conditional_orders(symbol="BTCUSDT")
            5. Optionally place new protective order: binance_futures_stop_order(...)

        Fixing Error -4130:
            If you tried binance_cancel_futures_order() and got:
            "Error -4130: Order not found"

            This means the order is an algo order, not a basic order. Use this tool instead!

        Risk Management Considerations:
            - Cancelling stop-loss orders removes your protection
            - Cancelling take-profit orders may prevent profit-taking
            - cancel_all removes ALL protective orders at once
            - After cancelling protective orders, monitor position manually
            - Consider placing new protective orders immediately

        Common Errors:
            - "Algo order not found": Order already triggered, cancelled, or wrong algo_id
            - "Invalid algo_id": Must be a valid positive integer
            - "Order is not pending": Order already triggered or filled
            - Network errors: Retry if cancellation didn't complete

        Related Tools:
            - Get algo order IDs: binance_get_futures_conditional_orders()
            - Cancel basic orders: binance_cancel_futures_order()
            - Place stop orders: binance_futures_stop_order()
            - View positions: binance_manage_futures_positions()

        CRITICAL Safety Notes:
            - This operation is immediate and cannot be undone
            - Cancelled algo orders don't appear in conditional orders list
            - Always verify algoId before cancelling
            - cancel_all is very powerful - use with extreme caution
            - Positions remain open after cancelling protective orders
        """
        logger.info(f"binance_cancel_algo_order tool invoked for {symbol} by {requester}")

        # Validate parameters
        if not symbol:
            return "Error: symbol is required (e.g., 'BTCUSDT')"

        has_identifier = algo_id is not None or client_algo_id is not None
        if not has_identifier and not cancel_all:
            return "Error: Must specify algo_id, client_algo_id, or cancel_all=True"

        if has_identifier and cancel_all:
            return "Error: Cannot specify both an identifier (algo_id/client_algo_id) and cancel_all=True"

        if algo_id is not None and client_algo_id is not None:
            return "Error: Cannot specify both algo_id and client_algo_id - use one identifier"

        # Validate algo_id is positive integer
        if algo_id is not None and algo_id <= 0:
            return "Error: algo_id must be a positive integer"

        try:
            # Execute cancellation
            df = cancel_algo_order_operation(
                binance_client=local_binance_client,
                symbol=symbol,
                algo_id=algo_id,
                client_algo_id=client_algo_id,
                cancel_all=cancel_all
            )

            # Generate filename
            operation_type = df.iloc[0]['operation']
            filename = f"cancel_algo_{operation_type}_{symbol}_{str(uuid.uuid4())[:8]}.csv"
            filepath = csv_dir / filename

            # Save to CSV
            df.to_csv(filepath, index=False)
            logger.info(f"Saved algo cancellation to {filename}")

            # Return formatted response
            result = format_csv_response(filepath, df)

            # Log request
            log_request(
                requests_dir=requests_dir,
                requester=requester,
                tool_name="binance_cancel_algo_order",
                input_params={
                    "symbol": symbol,
                    "algo_id": algo_id,
                    "client_algo_id": client_algo_id,
                    "cancel_all": cancel_all
                },
                output_result=result
            )

            # Add cancellation summary
            cancel_data = df.iloc[0]

            summary = f"""

═══════════════════════════════════════════════════════════════════════════════
ALGO ORDER(S) CANCELLED
═══════════════════════════════════════════════════════════════════════════════
Operation:       {cancel_data['operation'].replace('_', ' ').title()}
Symbol:          {cancel_data['symbol']}
"""

            if cancel_data['algoId']:
                summary += f"Algo ID:         {cancel_data['algoId']}\n"

            if cancel_data['clientAlgoId']:
                summary += f"Client Algo ID:  {cancel_data['clientAlgoId']}\n"

            summary += f"""Status:          {cancel_data['status']}
Code:            {cancel_data['code']}
Message:         {cancel_data['msg']}
Time:            {cancel_data['timestamp']}
═══════════════════════════════════════════════════════════════════════════════

The cancelled algo order(s) have been removed.

Verify cancellation:
  binance_get_futures_conditional_orders(symbol="{cancel_data['symbol']}")

Check your positions:
  binance_manage_futures_positions()
"""

            if cancel_data['operation'] == 'cancel_all_algo':
                summary += """
IMPORTANT: All conditional orders for this symbol have been cancelled.
This includes stop-loss and take-profit orders!
Your positions are now without automatic protection.
Consider placing new protective orders or manually monitoring positions.
"""

            summary += "═══════════════════════════════════════════════════════════════════════════════\n"

            return result + summary

        except ValueError as e:
            logger.error(f"Validation error: {e}")
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Error cancelling algo order: {e}")
            error_msg = str(e)

            # Provide helpful error messages
            if "not found" in error_msg.lower() or "-4130" in error_msg:
                return f"""Error: Algo order not found.

Possible reasons:
- Algo order already triggered (became a market order)
- Algo order already cancelled
- Wrong algo_id for this symbol
- Using orderId instead of algoId

To find correct algoId:
  binance_get_futures_conditional_orders(symbol="{symbol}")

Note: If you're trying to cancel a basic LIMIT/MARKET order, use:
  binance_cancel_futures_order(symbol="{symbol}", order_id=ORDER_ID)
"""
            elif "not pending" in error_msg.lower():
                return f"""Error: Algo order is not pending.

The order may have already:
- Triggered and became a market order
- Been filled
- Been cancelled

Check order status:
  binance_get_futures_conditional_orders(symbol="{symbol}")
"""
            else:
                return f"""Error: {error_msg}

Check:
- API credentials valid
- Symbol correct (e.g., 'BTCUSDT')
- algo_id is from binance_get_futures_conditional_orders(), not binance_get_futures_open_orders()
- Algo order still exists and is cancellable
"""
