import logging
from datetime import datetime
from decimal import Decimal
import uuid
from mcp_service import format_csv_response
import pandas as pd
from binance.client import Client
from typing import Optional
from sentry_utils import with_sentry_tracing
from binance_tools.validation_helpers import validate_and_adjust_quantity, create_lot_size_error_message, format_decimal
import json

logger = logging.getLogger(__name__)


@with_sentry_tracing("binance_spot_oco_order")
def execute_oco_order(binance_client: Client, symbol: str, side: str, quantity: float,
                     take_profit_price: float, stop_loss_price: float,
                     stop_limit_price: Optional[float] = None,
                     time_in_force: str = "GTC") -> pd.DataFrame:
    """
    Execute an OCO (One-Cancels-Other) order on Binance spot market and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        side: Order side - 'BUY' or 'SELL' (typically SELL to exit a long position)
        quantity: Amount of base asset (e.g., 0.001 BTC)
        take_profit_price: Price for take-profit limit order
        stop_loss_price: Trigger price for stop-loss order
        stop_limit_price: Limit price for stop-loss (optional, defaults to stop_loss_price)
        time_in_force: Time in force for stop-loss order (default: 'GTC')

    Returns:
        DataFrame with OCO order details containing columns:
        - orderListId: Unique OCO order list identifier
        - symbol: Trading pair symbol
        - contingencyType: OCO
        - listOrderStatus: Status of OCO order list
        - orders: JSON string with both order details

    Note:
        When one order executes, the other is automatically cancelled.
        Take-profit order uses LIMIT_MAKER, stop-loss uses STOP_LOSS_LIMIT.
        WARNING: This executes REAL TRADES with REAL MONEY.
    """
    logger.info(f"Placing OCO {side} order for {symbol}")

    # Validate parameters
    side = side.upper()
    if side not in ['BUY', 'SELL']:
        raise ValueError("side must be 'BUY' or 'SELL'")

    if not quantity or quantity <= 0:
        raise ValueError("quantity must be positive")

    if not take_profit_price or take_profit_price <= 0:
        raise ValueError("take_profit_price must be positive")

    if not stop_loss_price or stop_loss_price <= 0:
        raise ValueError("stop_loss_price must be positive")

    # If stop_limit_price not provided, use stop_loss_price
    if not stop_limit_price:
        stop_limit_price = stop_loss_price

    # Validate time in force parameter
    time_in_force = time_in_force.upper()
    if time_in_force not in ['GTC', 'IOC', 'FOK']:
        raise ValueError("time_in_force must be 'GTC', 'IOC', or 'FOK'")

    try:
        # Validate and adjust quantity according to LOT_SIZE filter
        logger.info(f"Validating quantity for {symbol}: {quantity}")
        adjusted_quantity, error_msg = validate_and_adjust_quantity(binance_client, symbol, quantity)

        if error_msg:
            logger.error(f"Quantity validation failed: {error_msg}")
            raise ValueError(error_msg)

        if adjusted_quantity != quantity:
            logger.info(f"Quantity adjusted from {quantity} to {adjusted_quantity} to meet LOT_SIZE requirements")
            quantity = adjusted_quantity

        # Execute the OCO order using the new Binance API v3 format
        # Convert all numeric parameters to decimal strings to avoid scientific notation
        # (e.g., 0.00009 -> "0.00009" not "9e-05" which Binance rejects)
        logger.warning(f"⚠️  PLACING REAL OCO ORDER: {side} {quantity} {symbol}, TP: {take_profit_price}, SL: {stop_loss_price}")

        # OCO order structure depends on side:
        # SELL: Take profit ABOVE current price (LIMIT_MAKER), Stop loss BELOW (STOP_LOSS_LIMIT)
        # BUY: Take profit BELOW current price (LIMIT_MAKER), Stop loss ABOVE (STOP_LOSS_LIMIT)
        if side == 'SELL':
            order = binance_client.create_oco_order(
                symbol=symbol,
                side=side,
                quantity=format_decimal(quantity),
                aboveType="LIMIT_MAKER",
                abovePrice=format_decimal(take_profit_price),
                aboveTimeInForce="GTC",
                belowType="STOP_LOSS_LIMIT",
                belowStopPrice=format_decimal(stop_loss_price),
                belowPrice=format_decimal(stop_limit_price),
                belowTimeInForce=time_in_force
            )
        else:  # BUY
            order = binance_client.create_oco_order(
                symbol=symbol,
                side=side,
                quantity=format_decimal(quantity),
                belowType="LIMIT_MAKER",
                belowPrice=format_decimal(take_profit_price),
                belowTimeInForce="GTC",
                aboveType="STOP_LOSS_LIMIT",
                aboveStopPrice=format_decimal(stop_loss_price),
                abovePrice=format_decimal(stop_limit_price),
                aboveTimeInForce=time_in_force
            )
        logger.info(f"OCO order placed successfully. Order List ID: {order['orderListId']}")

        # Extract order details
        orders_info = []
        for order_report in order.get('orderReports', []):
            orders_info.append({
                'orderId': order_report['orderId'],
                'clientOrderId': order_report['clientOrderId'],
                'type': order_report['type'],
                'side': order_report['side'],
                'price': order_report.get('price', ''),
                'stopPrice': order_report.get('stopPrice', ''),
                'origQty': order_report['origQty'],
                'status': order_report['status']
            })

        # Create record
        record = {
            'orderListId': order['orderListId'],
            'symbol': order['symbol'],
            'contingencyType': order['contingencyType'],
            'listOrderStatus': order['listOrderStatus'],
            'transactTime': datetime.fromtimestamp(order['transactionTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
            'orders': json.dumps(orders_info)
        }

        # Create DataFrame
        df = pd.DataFrame([record])

        logger.info(f"OCO order placed: List ID {order['orderListId']}, Status: {order['listOrderStatus']}")

        return df

    except ValueError as e:
        # Re-raise validation errors as-is
        logger.error(f"Validation error placing OCO order: {e}")
        raise
    except Exception as e:
        error_str = str(e)
        logger.error(f"Error placing OCO order: {e}")

        # Check if it's a LOT_SIZE error
        if 'LOT_SIZE' in error_str or '-1013' in error_str:
            detailed_error = create_lot_size_error_message(symbol, quantity, error_str)
            raise Exception(detailed_error)

        raise


def register_binance_spot_oco_order(local_mcp_instance, local_binance_client, csv_dir):
    """Register the binance_spot_oco_order tool"""
    @local_mcp_instance.tool()
    def binance_spot_oco_order(symbol: str, side: str, quantity: float, take_profit_price: float,
                               stop_loss_price: float, stop_limit_price: float = None,
                               time_in_force: str = "GTC") -> str:
        """
        Place an OCO (One-Cancels-Other) order for advanced risk management and save details to CSV.

        An OCO order consists of two orders: a take-profit limit order and a stop-loss order.
        When one order executes, the other is automatically cancelled. This is essential for
        risk management as it allows you to set both profit target and stop-loss simultaneously.

        ⚠️  WARNING: THIS EXECUTES REAL TRADES WITH REAL MONEY ⚠️
        OCO orders execute REAL trades when price conditions are met. Always verify parameters
        before execution.

        Parameters:
            symbol (string, required): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
            side (string, required): Order side - 'BUY' or 'SELL' (case-insensitive)
                Typically 'SELL' for exiting long positions with risk management
            quantity (float, required): Amount of base asset to trade (e.g., 0.001 for 0.001 BTC)
                Note: Will be auto-adjusted to meet symbol's LOT_SIZE requirements
                Also handles small quantities (e.g., 0.00009) without scientific notation errors
            take_profit_price (float, required): Price for take-profit limit order (profit target)
            stop_loss_price (float, required): Trigger price for stop-loss order (stop loss level)
            stop_limit_price (float, optional): Limit price for stop-loss order (defaults to stop_loss_price)
                Setting this slightly below stop_loss_price can help ensure execution
            time_in_force (string, optional): Time in force for stop-loss order (default: 'GTC')
                Options: 'GTC' (Good Till Cancel), 'IOC' (Immediate or Cancel), 'FOK' (Fill or Kill)

        Returns:
            str: Formatted response with CSV file containing OCO order details, including
                order list ID, both order details, and management instructions.

        CSV Output Columns:
            - orderListId (integer): Unique OCO order list identifier (use for cancellation)
            - symbol (string): Trading pair symbol (e.g., 'BTCUSDT')
            - contingencyType (string): 'OCO'
            - listOrderStatus (string): Status of the OCO order list (EXECUTING, ALL_DONE, REJECT)
            - transactTime (string): Transaction timestamp (YYYY-MM-DD HH:MM:S)
            - orders (string): JSON array with details of both orders (take-profit and stop-loss)

        OCO Order Structure (for SELL orders):
            1. Take-Profit Order (LIMIT_MAKER - automatically set):
               - Executes when price RISES to take_profit_price
               - Secures profit at target price
               - Acts as a sell limit order
               - No fees as maker order

            2. Stop-Loss Order (STOP_LOSS_LIMIT - automatically set):
               - Triggers when price FALLS to stop_loss_price
               - Limits losses by exiting position
               - Executes as limit order at stop_limit_price
               - Controlled by time_in_force parameter

        Price Relationships (SELL OCO):
            For a SELL OCO order to exit a long position:
            - take_profit_price > current_price > stop_loss_price
            - Example: Current price = $50,000
              * Take profit = $52,000 (2% gain)
              * Stop loss = $48,000 (4% loss)
              * Risk/Reward ratio = 1:0.5

        Price Relationships (BUY OCO):
            For a BUY OCO order (less common):
            - take_profit_price < current_price < stop_loss_price

        Use Cases:
            - Exit long position with defined profit target and stop-loss
            - Automated risk management without constant monitoring
            - Bracket orders for systematic trading
            - Implementing risk/reward ratios in trading strategy
            - Protecting profits while limiting downside risk

        Advantages:
            - Automatic risk management (no need to monitor constantly)
            - Both profit target and stop-loss set simultaneously
            - One order cancels the other (no over-execution)
            - Peace of mind - position protected both ways
            - Essential tool for disciplined trading

        Disadvantages:
            - More complex than single orders
            - Both orders consume balance/margin
            - Stop-loss may trigger in volatile markets (stop hunting)
            - Requires careful price level selection

        Risk Management:
            - Calculate risk/reward ratio before placing order
            - Ensure stop_loss_price isn't too tight (avoid premature stop-out)
            - Set take_profit_price at realistic technical levels
            - Use stop_limit_price slightly below stop_loss_price for better fill probability
            - Consider market volatility when setting levels
            - Quantity is automatically validated and adjusted to meet LOT_SIZE requirements

        Example usage:
            # SELL OCO: Exit long BTC position with 4% profit target and 2% stop-loss
            # Current price: ~$50,000, Target: $52,000 (+4%), Stop: $49,000 (-2%)
            binance_spot_oco_order(
                symbol="BTCUSDT",
                side="SELL",
                quantity=0.001,
                take_profit_price=52000,
                stop_loss_price=49000,
                stop_limit_price=48900
            )

            # SELL OCO: Exit long ETH position
            binance_spot_oco_order(
                symbol="ETHUSDT",
                side="SELL",
                quantity=0.1,
                take_profit_price=3600,
                stop_loss_price=3400
            )

        Managing OCO Orders:
            - Check status: Use binance_get_open_orders (both orders appear separately)
            - Cancel entire OCO: Use binance_cancel_order with order_list_id parameter
            - Modify: Cancel existing OCO and place new one (cannot modify in-place)

        What Happens When Order Executes:
            - If take-profit executes → stop-loss automatically cancelled → profit secured
            - If stop-loss executes → take-profit automatically cancelled → loss limited
            - Only ONE of the two orders can execute

        Note:
            - CSV file saved for your records and order tracking
            - Order List ID needed to cancel entire OCO order
            - Both orders appear in open orders list
            - Commission deducted when either order fills
            - If one order partially fills, the OCO relationship is maintained
            - Consider using stop_limit_price to improve stop-loss fill probability
        """
        logger.info(f"binance_spot_oco_order tool invoked: {side} {quantity} {symbol}")

        # Validate parameters
        if not symbol:
            return "Error: symbol is required (e.g., 'BTCUSDT')"

        if not side:
            return "Error: side is required ('BUY' or 'SELL')"

        try:
            # Execute OCO order
            df = execute_oco_order(
                binance_client=local_binance_client,
                symbol=symbol,
                side=side,
                quantity=quantity,
                take_profit_price=take_profit_price,
                stop_loss_price=stop_loss_price,
                stop_limit_price=stop_limit_price,
                time_in_force=time_in_force
            )

            # Generate filename with unique identifier
            filename = f"oco_order_{symbol}_{side.lower()}_{str(uuid.uuid4())[:8]}.csv"
            filepath = csv_dir / filename

            # Save to CSV file
            df.to_csv(filepath, index=False)
            logger.info(f"Saved OCO order to {filename}")

            # Return formatted response
            result = format_csv_response(filepath, df)

            # Add execution summary to response
            order_data = df.iloc[0]
            orders_info = json.loads(order_data['orders'])

            summary = f"""

═══════════════════════════════════════════════════════════════════════════════
OCO ORDER PLACED SUCCESSFULLY
═══════════════════════════════════════════════════════════════════════════════
Order List ID:   {order_data['orderListId']}
Symbol:          {order_data['symbol']}
Contingency:     {order_data['contingencyType']}
Status:          {order_data['listOrderStatus']}
Time:            {order_data['transactTime']}

INDIVIDUAL ORDERS:
"""

            for i, order_info in enumerate(orders_info, 1):
                summary += f"""
Order #{i}: {order_info['type']}
  Order ID:      {order_info['orderId']}
  Side:          {order_info['side']}
  Quantity:      {order_info['origQty']}
  Status:        {order_info['status']}
"""
                if order_info['type'] == 'LIMIT_MAKER':
                    summary += f"  Price:         {order_info['price']} (Take-Profit)\n"
                elif order_info['type'] in ['STOP_LOSS', 'STOP_LOSS_LIMIT']:
                    summary += f"  Stop Price:    {order_info.get('stopPrice', 'N/A')} (Stop-Loss Trigger)\n"
                    if order_info['type'] == 'STOP_LOSS_LIMIT':
                        summary += f"  Limit Price:   {order_info['price']} (Stop-Loss Limit)\n"

            summary += f"""
═══════════════════════════════════════════════════════════════════════════════
IMPORTANT NOTES:
• Both orders are now active
• When ONE order fills, the OTHER will be automatically cancelled
• Monitor your position until one order executes

To check OCO order status:
  binance_get_open_orders(symbol="{order_data['symbol']}")

To cancel entire OCO order:
  binance_cancel_order(symbol="{order_data['symbol']}", order_list_id={order_data['orderListId']})
═══════════════════════════════════════════════════════════════════════════════
"""

            return result + summary

        except ValueError as e:
            logger.error(f"Validation error: {e}")
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Error placing OCO order: {e}")
            return f"Error placing OCO order: {str(e)}\n\nPlease check:\n- API credentials are valid\n- Symbol is correct\n- Price levels are valid (TP > current > SL for SELL)\n- Quantity meets minimum requirements\n- Sufficient balance available\n- API key has trading permissions"
