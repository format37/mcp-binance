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


@with_sentry_tracing("binance_get_futures_conditional_orders")
def fetch_futures_conditional_orders(binance_client: Client, symbol: Optional[str] = None) -> pd.DataFrame:
    """
    Fetch open conditional orders (TP/SL/Trailing) using Binance Algo Service.
    Uses: GET /fapi/v1/openAlgoOrders

    Args:
        binance_client: Initialized Binance Client
        symbol: Optional trading pair symbol to filter (e.g., 'BTCUSDT')

    Returns:
        DataFrame with open conditional/algo futures orders

    Note:
        As of December 9, 2025, Binance moved conditional orders (STOP_MARKET,
        TAKE_PROFIT_MARKET, TRAILING_STOP_MARKET) to the Algo Service.
        Basic orders (LIMIT, MARKET) are returned by get_futures_open_orders.
    """
    logger.info(f"Fetching futures conditional orders" + (f" for {symbol}" if symbol else ""))

    try:
        # Build parameters
        params = {}
        if symbol:
            params['symbol'] = symbol

        # Direct API call to the Algo Service endpoint
        # python-binance may not have this endpoint yet, so we use _request_futures_api
        orders = binance_client._request_futures_api('get', 'openAlgoOrders', signed=True, data=params)

        records = []
        for order in orders:
            records.append({
                'algoId': order.get('algoId', order.get('orderId', '')),
                'orderId': order.get('orderId', ''),
                'clientOrderId': order.get('clientOrderId', ''),
                'symbol': order['symbol'],
                'side': order['side'],
                'positionSide': order.get('positionSide', 'BOTH'),
                'type': order['type'],  # STOP_MARKET, TAKE_PROFIT_MARKET, TRAILING_STOP_MARKET
                'stopPrice': float(order.get('stopPrice', 0)),
                'activatePrice': float(order.get('activatePrice', 0)),
                'priceRate': float(order.get('priceRate', order.get('callbackRate', 0))),
                'origQty': float(order.get('origQty', 0)),
                'executedQty': float(order.get('executedQty', 0)),
                'status': order.get('status', 'NEW'),
                'reduceOnly': order.get('reduceOnly', False),
                'closePosition': order.get('closePosition', False),
                'workingType': order.get('workingType', 'MARK_PRICE'),
                'time': datetime.fromtimestamp(order['time'] / 1000).strftime('%Y-%m-%d %H:%M:%S') if order.get('time') else '',
                'updateTime': datetime.fromtimestamp(order['updateTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S') if order.get('updateTime') else ''
            })

        df = pd.DataFrame(records) if records else pd.DataFrame(columns=[
            'algoId', 'orderId', 'clientOrderId', 'symbol', 'side', 'positionSide', 'type',
            'stopPrice', 'activatePrice', 'priceRate', 'origQty', 'executedQty', 'status',
            'reduceOnly', 'closePosition', 'workingType', 'time', 'updateTime'
        ])

        logger.info(f"Retrieved {len(records)} conditional futures orders")

        return df

    except Exception as e:
        logger.error(f"Error fetching futures conditional orders: {e}")
        raise


def register_binance_get_futures_conditional_orders(local_mcp_instance, local_binance_client, csv_dir, requests_dir):
    """Register the binance_get_futures_conditional_orders tool"""
    @local_mcp_instance.tool()
    def binance_get_futures_conditional_orders(requester: str, symbol: Optional[str] = None) -> str:
        """
        Fetch all open conditional/algo futures orders (TP/SL/Trailing) and save to CSV.

        This tool retrieves conditional orders from the Binance Algo Service endpoint.
        As of December 9, 2025, Binance separated conditional orders from basic orders:
        - Basic orders (LIMIT, MARKET): Use binance_get_futures_open_orders()
        - Conditional orders (STOP_MARKET, TAKE_PROFIT_MARKET, TRAILING_STOP_MARKET): Use THIS tool

        ✓ READ-ONLY OPERATION - Safe to run anytime

        Parameters:
            requester (string, required): Identifier of the user/system making the request
            symbol (string, optional): Trading pair symbol (e.g., 'BTCUSDT')
                - If provided: Shows only conditional orders for this symbol
                - If omitted: Shows all conditional orders across all symbols

        Returns:
            str: Formatted response with CSV file containing conditional orders.

        CSV Output Columns:
            - algoId (integer): Algo order identifier
            - orderId (integer): Order identifier
            - clientOrderId (string): Client-assigned order ID
            - symbol (string): Trading pair (e.g., 'BTCUSDT')
            - side (string): Order side (BUY or SELL)
            - positionSide (string): BOTH, LONG, or SHORT
            - type (string): Order type:
                - STOP_MARKET: Stop-loss order
                - TAKE_PROFIT_MARKET: Take-profit order
                - TRAILING_STOP_MARKET: Trailing stop order
            - stopPrice (float): Trigger price for stop/TP orders
            - activatePrice (float): Activation price for trailing stops
            - priceRate (float): Callback rate for trailing stops (percentage)
            - origQty (float): Original order quantity (0 if closePosition=True)
            - executedQty (float): Quantity executed (0 for pending orders)
            - status (string): Order status (NEW, FILLED, CANCELED)
            - reduceOnly (boolean): True if order only reduces position
            - closePosition (boolean): True if order closes entire position
            - workingType (string): Price type (MARK_PRICE or CONTRACT_PRICE)
            - time (string): Order creation timestamp
            - updateTime (string): Last update timestamp

        Order Types Explained:
            - STOP_MARKET: Executes as market order when price hits stop
            - TAKE_PROFIT_MARKET: Executes as market order at target price
            - TRAILING_STOP_MARKET: Dynamic stop that follows price movement

        Use Cases:
            - Check active stop-loss orders protecting your positions
            - Verify take-profit orders are properly set
            - Monitor trailing stop configuration
            - Audit risk management orders
            - Review all conditional orders before market close
            - Ensure positions have proper protection

        Example usage:
            # View all conditional futures orders
            binance_get_futures_conditional_orders()

            # View conditional orders for specific symbol
            binance_get_futures_conditional_orders(symbol="BTCUSDT")

        Analysis with py_eval:
            - Group by symbol to see protection per position
            - Filter by type to see only stop-losses or take-profits
            - Check closePosition orders vs quantity-based orders
            - Verify workingType is MARK_PRICE (recommended)
            - Identify positions without protection

        Why Two Order Tools?
            Binance API change (December 9, 2025):
            - binance_get_futures_open_orders() → Basic orders (LIMIT, MARKET)
            - binance_get_futures_conditional_orders() → Conditional orders (TP/SL/Trailing)

            If you're looking for stop-loss or take-profit orders and they don't
            appear in binance_get_futures_open_orders(), use this tool instead!

        Order Management:
            - Place stop orders: binance_futures_stop_order(...)
            - Cancel order: binance_cancel_futures_order(symbol, order_id)
            - View positions: binance_manage_futures_positions()

        Important Notes:
            - Only shows PENDING conditional orders (not filled/cancelled)
            - Orders with closePosition=True have origQty=0
            - priceRate is callback percentage for trailing stops
            - activatePrice is when trailing stop starts tracking
            - Combine with binance_get_futures_open_orders() for complete picture

        Related Tools:
            - Basic orders: binance_get_futures_open_orders()
            - Place stop order: binance_futures_stop_order(...)
            - Cancel order: binance_cancel_futures_order(symbol, order_id)
            - View positions: binance_manage_futures_positions()

        Note:
            - Completely safe READ-ONLY operation
            - Run as frequently as needed
            - CSV file saved for record keeping and analysis
        """
        logger.info(f"binance_get_futures_conditional_orders tool invoked by {requester}")

        try:
            # Fetch conditional orders
            df = fetch_futures_conditional_orders(
                binance_client=local_binance_client,
                symbol=symbol
            )

            # Generate filename
            if symbol:
                filename = f"futures_conditional_orders_{symbol}_{str(uuid.uuid4())[:8]}.csv"
            else:
                filename = f"futures_conditional_orders_all_{str(uuid.uuid4())[:8]}.csv"

            filepath = csv_dir / filename

            # Save to CSV
            df.to_csv(filepath, index=False)
            logger.info(f"Saved futures conditional orders to {filename}")

            # Return formatted response
            result = format_csv_response(filepath, df)

            if df.empty:
                summary = f"""

═══════════════════════════════════════════════════════════════════════════════
NO OPEN CONDITIONAL ORDERS
═══════════════════════════════════════════════════════════════════════════════
"""
                if symbol:
                    summary += f"No conditional orders found for {symbol}.\n"
                else:
                    summary += "No conditional futures orders found.\n"
                summary += """
This means no STOP_MARKET, TAKE_PROFIT_MARKET, or TRAILING_STOP_MARKET orders.

⚠️  If you have open positions without stop-loss orders, consider adding them!

To place a stop-loss order:
  binance_futures_stop_order(symbol="BTCUSDT", side="SELL", order_type="STOP_MARKET",
                            stop_price=90000, close_position=True, position_side="LONG")

To check basic orders (LIMIT, MARKET):
  binance_get_futures_open_orders()

═══════════════════════════════════════════════════════════════════════════════
"""
                return result + summary

            # Calculate summary statistics
            total_orders = len(df)
            by_type = df['type'].value_counts().to_dict()
            by_symbol = df['symbol'].value_counts().to_dict()
            close_position_count = len(df[df['closePosition'] == True])

            summary = f"""

═══════════════════════════════════════════════════════════════════════════════
CONDITIONAL ORDERS SUMMARY
═══════════════════════════════════════════════════════════════════════════════
Total Conditional Orders:  {total_orders}
Close Position Orders:     {close_position_count}

By Order Type:
"""

            for order_type, count in sorted(by_type.items()):
                type_display = order_type.replace('_MARKET', '').replace('_', ' ')
                summary += f"  {type_display}: {count}\n"

            summary += "\nBy Symbol:\n"
            for sym, count in sorted(by_symbol.items()):
                summary += f"  {sym}: {count} order(s)\n"

            # List order details
            summary += "\nOrder Details:\n"
            for _, row in df.iterrows():
                order_type = row['type'].replace('_MARKET', '')
                if row['type'] == 'TRAILING_STOP_MARKET':
                    trigger_info = f"Trail {row['priceRate']}%"
                    if row['activatePrice'] > 0:
                        trigger_info += f" (activate @ {row['activatePrice']})"
                else:
                    trigger_info = f"@ {row['stopPrice']}"

                qty_info = "CLOSE ALL" if row['closePosition'] else f"Qty: {row['origQty']}"
                summary += f"  {row['symbol']} {row['side']} {order_type} {trigger_info} [{qty_info}]\n"

            summary += """
═══════════════════════════════════════════════════════════════════════════════

To cancel a conditional order:
  binance_cancel_futures_order(symbol="SYMBOL", order_id=ORDER_ID)

To check basic orders (LIMIT, MARKET):
  binance_get_futures_open_orders()

═══════════════════════════════════════════════════════════════════════════════
"""

            log_request(
                requests_dir=requests_dir,
                requester=requester,
                tool_name="binance_get_futures_conditional_orders",
                input_params={"symbol": symbol},
                output_result=result + summary
            )

            return result + summary

        except Exception as e:
            logger.error(f"Error fetching futures conditional orders: {e}")
            return f"Error: {str(e)}\n\nCheck:\n- API credentials valid\n- Futures trading enabled\n- Network connectivity"
