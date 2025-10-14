import logging
from datetime import datetime
from decimal import Decimal
import uuid
from mcp_service import format_csv_response
import pandas as pd
from binance.client import Client
from sentry_utils import with_sentry_tracing

logger = logging.getLogger(__name__)


@with_sentry_tracing("binance_spot_limit_order")
def execute_limit_order(binance_client: Client, symbol: str, side: str,
                       quantity: float, price: float, time_in_force: str = 'GTC') -> pd.DataFrame:
    """
    Execute a limit order on Binance spot market and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        side: Order side - 'BUY' or 'SELL'
        quantity: Amount of base asset to buy/sell (e.g., 0.001 BTC)
        price: Limit price for the order
        time_in_force: Order duration - 'GTC', 'IOC', or 'FOK' (default: 'GTC')

    Returns:
        DataFrame with order placement details containing columns:
        - orderId: Unique order identifier
        - clientOrderId: Client order ID
        - symbol: Trading pair symbol
        - side: Order side (BUY or SELL)
        - type: Order type (LIMIT)
        - timeInForce: Time in force setting
        - price: Limit price
        - origQty: Original quantity
        - executedQty: Executed quantity (may be 0 for new orders)
        - status: Order status
        - transactTime: Transaction timestamp
        - fills: Number of immediate fills (for IOC/FOK)

    Note:
        Limit orders allow precise price control but may not fill immediately.
        WARNING: This executes REAL TRADES with REAL MONEY.
    """
    logger.info(f"Placing limit {side} order for {symbol} @ {price}")

    # Validate parameters
    side = side.upper()
    if side not in ['BUY', 'SELL']:
        raise ValueError("side must be 'BUY' or 'SELL'")

    time_in_force = time_in_force.upper()
    if time_in_force not in ['GTC', 'IOC', 'FOK']:
        raise ValueError("time_in_force must be 'GTC', 'IOC', or 'FOK'")

    if not quantity or quantity <= 0:
        raise ValueError("quantity must be positive")

    if not price or price <= 0:
        raise ValueError("price must be positive")

    try:
        # Execute the order
        logger.warning(f"⚠️  PLACING REAL LIMIT ORDER: {side} {quantity} {symbol} @ {price}")
        order = binance_client.order_limit(
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            timeInForce=time_in_force
        )
        logger.info(f"Limit order placed successfully. Order ID: {order['orderId']}, Status: {order['status']}")

        # Count fills
        fills_count = len(order.get('fills', []))

        # Create record
        record = {
            'orderId': order['orderId'],
            'clientOrderId': order['clientOrderId'],
            'symbol': order['symbol'],
            'side': order['side'],
            'type': order['type'],
            'timeInForce': order['timeInForce'],
            'price': float(order['price']),
            'origQty': float(order['origQty']),
            'executedQty': float(order['executedQty']),
            'status': order['status'],
            'transactTime': datetime.fromtimestamp(order['transactTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
            'fills': fills_count
        }

        # Create DataFrame
        df = pd.DataFrame([record])

        logger.info(f"Limit order placed: {quantity} @ {price}, Status: {order['status']}")

        return df

    except Exception as e:
        logger.error(f"Error placing limit order: {e}")
        raise


def register_binance_spot_limit_order(local_mcp_instance, local_binance_client, csv_dir):
    """Register the binance_spot_limit_order tool"""
    @local_mcp_instance.tool()
    def binance_spot_limit_order(symbol: str, side: str, quantity: float, price: float, time_in_force: str = 'GTC') -> str:
        """
        Place a limit order on Binance spot market at a specific price and save details to CSV.

        ⚠️  WARNING: THIS EXECUTES REAL TRADES WITH REAL MONEY ⚠️
        Limit orders will execute when market reaches your specified price. Always verify
        parameters before execution. Orders may be cancelled if not filled.

        Parameters:
            symbol (string, required): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
            side (string, required): Order side - 'BUY' or 'SELL' (case-insensitive)
            quantity (float, required): Amount of base asset to trade (e.g., 0.001 for 0.001 BTC)
            price (float, required): Limit price for order execution
            time_in_force (string, optional): Order duration (default: 'GTC')
                - 'GTC' (Good Till Cancelled): Order stays active until filled or manually cancelled
                - 'IOC' (Immediate Or Cancel): Fill immediately at limit price or better, cancel remainder
                - 'FOK' (Fill Or Kill): Fill entire order immediately at limit price or better, or cancel entirely

        Returns:
            str: Formatted response with CSV file containing order placement details, including
                order ID, status, and instructions for checking/cancelling the order.

        CSV Output Columns:
            - orderId (integer): Unique order identifier for tracking
            - clientOrderId (string): Client-assigned order ID
            - symbol (string): Trading pair symbol (e.g., 'BTCUSDT')
            - side (string): Order side (BUY or SELL)
            - type (string): Order type (LIMIT)
            - timeInForce (string): Time in force setting (GTC, IOC, FOK)
            - price (float): Limit price
            - origQty (float): Original order quantity
            - executedQty (float): Quantity already executed (0 for new orders, >0 for partial fills)
            - status (string): Order status (NEW, PARTIALLY_FILLED, FILLED, CANCELED)
            - transactTime (string): Transaction timestamp (YYYY-MM-DD HH:MM:SS)
            - fills (integer): Number of immediate fills (typically 0 for GTC orders)

        Limit Order Behavior:
            - BUY limit orders execute when market price drops to or below your limit price
            - SELL limit orders execute when market price rises to or above your limit price
            - Orders may partially fill if full quantity isn't available at your price
            - GTC orders remain open until filled or manually cancelled
            - IOC orders fill immediately or cancel unfilled portion
            - FOK orders must fill completely immediately or are cancelled entirely

        Use Cases:
            - Buy at a lower price than current market (set buy limit below market)
            - Sell at a higher price than current market (set sell limit above market)
            - Avoid slippage on large orders
            - Implement precise entry/exit strategies
            - Better price control compared to market orders
            - Automated trading with specific price targets

        Advantages:
            - Precise price control (no worse than specified price)
            - Protection against slippage
            - Can be used for automatic execution at target prices
            - Better for less liquid markets or large orders

        Disadvantages:
            - No guarantee of execution (may never fill)
            - Requires monitoring if urgent execution needed
            - May miss market moves if price doesn't reach limit
            - Must cancel manually if no longer wanted (GTC orders)

        Risk Management:
            - Verify price is reasonable for current market conditions
            - Check if price follows symbol's tick size rules
            - Ensure quantity meets minimum order size requirements
            - Monitor open orders to prevent over-commitment of funds
            - Consider market volatility when setting limit prices

        Price Guidance:
            - For BUY orders: Set limit price AT or BELOW current market price
            - For SELL orders: Set limit price AT or ABOVE current market price
            - Setting buy limit above market acts like market order (fills immediately)
            - Setting sell limit below market acts like market order (fills immediately)

        Example usage:
            # Buy 0.001 BTC when price drops to $50,000
            binance_spot_limit_order(symbol="BTCUSDT", side="BUY", quantity=0.001, price=50000)

            # Sell 0.01 ETH when price rises to $3,500
            binance_spot_limit_order(symbol="ETHUSDT", side="SELL", quantity=0.01, price=3500)

            # Buy with IOC (fill immediately at $50,000 or better, cancel remainder)
            binance_spot_limit_order(symbol="BTCUSDT", side="BUY", quantity=0.001, price=50000, time_in_force="IOC")

        Managing Orders:
            - Check order status: Use binance_get_open_orders tool
            - Cancel order: Use binance_cancel_order tool with the orderId
            - View all orders: Use binance_get_open_orders without symbol parameter

        Note:
            - CSV file saved for your records and order tracking
            - Order ID is needed to cancel or check status later
            - Partially filled orders can be cancelled (unfilled portion)
            - Commission automatically deducted when order fills
            - Check balance before placing orders to avoid rejection
        """
        logger.info(f"binance_spot_limit_order tool invoked: {side} {quantity} {symbol} @ {price}")

        # Validate parameters
        if not symbol:
            return "Error: symbol is required (e.g., 'BTCUSDT')"

        if not side:
            return "Error: side is required ('BUY' or 'SELL')"

        try:
            # Execute limit order
            df = execute_limit_order(
                binance_client=local_binance_client,
                symbol=symbol,
                side=side,
                quantity=quantity,
                price=price,
                time_in_force=time_in_force
            )

            # Generate filename with unique identifier
            filename = f"limit_order_{symbol}_{side.lower()}_{str(uuid.uuid4())[:8]}.csv"
            filepath = csv_dir / filename

            # Save to CSV file
            df.to_csv(filepath, index=False)
            logger.info(f"Saved limit order to {filename}")

            # Return formatted response
            result = format_csv_response(filepath, df)

            # Add execution summary to response
            order_data = df.iloc[0]
            summary = f"""

═══════════════════════════════════════════════════════════════════════════════
LIMIT ORDER PLACED SUCCESSFULLY
═══════════════════════════════════════════════════════════════════════════════
Order ID:        {order_data['orderId']}
Symbol:          {order_data['symbol']}
Side:            {order_data['side']}
Limit Price:     {order_data['price']:.8f}
Quantity:        {order_data['origQty']:.8f}
Time in Force:   {order_data['timeInForce']}
Status:          {order_data['status']}
Executed Qty:    {order_data['executedQty']:.8f}
Time:            {order_data['transactTime']}
═══════════════════════════════════════════════════════════════════════════════
"""

            # Add status-specific guidance
            if order_data['status'] == 'NEW':
                summary += f"""
Order Status: OPEN
Your limit order is now active and waiting to be filled.
It will execute when the market price reaches {order_data['price']:.8f}.

To check order status:
  binance_get_open_orders(symbol="{order_data['symbol']}")

To cancel this order:
  binance_cancel_order(symbol="{order_data['symbol']}", order_id={order_data['orderId']})
"""
            elif order_data['status'] == 'FILLED':
                summary += "\nOrder Status: FILLED\nYour order was filled immediately!\n"
            elif order_data['status'] == 'PARTIALLY_FILLED':
                summary += f"\nOrder Status: PARTIALLY FILLED\n{order_data['executedQty']:.8f} / {order_data['origQty']:.8f} executed.\n"
            elif order_data['status'] == 'CANCELED':
                summary += "\nOrder Status: CANCELED\nOrder was not filled and has been canceled (IOC/FOK).\n"

            summary += "═══════════════════════════════════════════════════════════════════════════════\n"

            return result + summary

        except ValueError as e:
            logger.error(f"Validation error: {e}")
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Error placing limit order: {e}")
            return f"Error placing limit order: {str(e)}\n\nPlease check:\n- API credentials are valid\n- Symbol is correct\n- Price and quantity meet minimum requirements\n- Price follows tick size rules\n- Sufficient balance available\n- API key has trading permissions"
