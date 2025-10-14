import logging
from datetime import datetime
from decimal import Decimal
import uuid
from mcp_service import format_csv_response
import pandas as pd
from binance.client import Client
from typing import Optional
from sentry_utils import with_sentry_tracing
import json

logger = logging.getLogger(__name__)


@with_sentry_tracing("binance_spot_oco_order")
def execute_oco_order(binance_client: Client, symbol: str, side: str, quantity: float,
                     take_profit_price: float, stop_loss_price: float,
                     stop_limit_price: Optional[float] = None) -> pd.DataFrame:
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

    Returns:
        DataFrame with OCO order details containing columns:
        - orderListId: Unique OCO order list identifier
        - symbol: Trading pair symbol
        - contingencyType: OCO
        - listOrderStatus: Status of OCO order list
        - orders: JSON string with both order details

    Note:
        When one order executes, the other is automatically cancelled.
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

    try:
        # Execute the OCO order
        logger.warning(f"⚠️  PLACING REAL OCO ORDER: {side} {quantity} {symbol}, TP: {take_profit_price}, SL: {stop_loss_price}")
        order = binance_client.create_oco_order(
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=take_profit_price,
            stopPrice=stop_loss_price,
            stopLimitPrice=stop_limit_price,
            stopLimitTimeInForce='GTC'
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

    except Exception as e:
        logger.error(f"Error placing OCO order: {e}")
        raise


def register_binance_spot_oco_order(local_mcp_instance, local_binance_client, csv_dir):
    """Register the binance_spot_oco_order tool"""
    @local_mcp_instance.tool()
    def binance_spot_oco_order(symbol: str, side: str, quantity: float, take_profit_price: float,
                               stop_loss_price: float, stop_limit_price: float = None) -> str:
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
            take_profit_price (float, required): Price for take-profit limit order (profit target)
            stop_loss_price (float, required): Trigger price for stop-loss order (stop loss level)
            stop_limit_price (float, optional): Limit price for stop-loss order (defaults to stop_loss_price)
                Setting this slightly below stop_loss_price can help ensure execution

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
            1. Take-Profit Order (LIMIT_MAKER):
               - Executes when price RISES to take_profit_price
               - Secures profit at target price
               - Acts as a sell limit order

            2. Stop-Loss Order (STOP_LOSS_LIMIT):
               - Triggers when price FALLS to stop_loss_price
               - Limits losses by exiting position
               - Executes as limit order at stop_limit_price

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
                stop_limit_price=stop_limit_price
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
