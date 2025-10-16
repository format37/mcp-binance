import logging
from datetime import datetime
from decimal import Decimal
import uuid
from mcp_service import format_csv_response
import pandas as pd
from binance.client import Client
from sentry_utils import with_sentry_tracing

logger = logging.getLogger(__name__)


@with_sentry_tracing("binance_futures_limit_order")
def execute_futures_limit_order(binance_client: Client, symbol: str, side: str,
                                quantity: float, price: float, position_side: str = 'BOTH',
                                time_in_force: str = 'GTC', reduce_only: bool = False) -> pd.DataFrame:
    """
    Execute a futures limit order and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        side: Order side - 'BUY' or 'SELL'
        quantity: Amount of contracts to trade
        price: Limit price for the order
        position_side: Position side - 'BOTH' (one-way), 'LONG', or 'SHORT' (hedge mode)
        time_in_force: Order duration - 'GTC', 'IOC', 'FOK', or 'GTX' (default: 'GTC')
        reduce_only: If True, order can only reduce position size (default: False)

    Returns:
        DataFrame with order placement details

    Note:
        Futures limit orders with leverage - monitor liquidation prices carefully!
        WARNING: This executes REAL TRADES with REAL MONEY.
    """
    logger.info(f"Placing futures limit {side} order for {symbol} @ {price}")

    # Validate parameters
    side = side.upper()
    if side not in ['BUY', 'SELL']:
        raise ValueError("side must be 'BUY' or 'SELL'")

    position_side = position_side.upper()
    if position_side not in ['BOTH', 'LONG', 'SHORT']:
        raise ValueError("position_side must be 'BOTH', 'LONG', or 'SHORT'")

    time_in_force = time_in_force.upper()
    if time_in_force not in ['GTC', 'IOC', 'FOK', 'GTX']:
        raise ValueError("time_in_force must be 'GTC', 'IOC', 'FOK', or 'GTX'")

    if not quantity or quantity <= 0:
        raise ValueError("quantity must be positive")

    if not price or price <= 0:
        raise ValueError("price must be positive")

    try:
        # Build order parameters
        params = {
            'symbol': symbol,
            'side': side,
            'type': 'LIMIT',
            'quantity': quantity,
            'price': price,
            'timeInForce': time_in_force,
            'positionSide': position_side
        }

        if reduce_only:
            params['reduceOnly'] = True

        # Execute the order
        logger.warning(f"⚠️  PLACING REAL FUTURES LIMIT ORDER: {side} {quantity} {symbol} @ {price}")
        order = binance_client.futures_create_order(**params)
        logger.info(f"Futures limit order placed. Order ID: {order['orderId']}, Status: {order['status']}")

        # Create record
        record = {
            'orderId': order['orderId'],
            'clientOrderId': order['clientOrderId'],
            'symbol': order['symbol'],
            'side': order['side'],
            'positionSide': order['positionSide'],
            'type': order['type'],
            'timeInForce': order['timeInForce'],
            'price': float(order['price']),
            'origQty': float(order['origQty']),
            'executedQty': float(order['executedQty']),
            'status': order['status'],
            'reduceOnly': reduce_only,
            'updateTime': datetime.fromtimestamp(order['updateTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        }

        df = pd.DataFrame([record])
        logger.info(f"Futures limit order placed: {quantity} @ {price}, Status: {order['status']}")

        return df

    except Exception as e:
        logger.error(f"Error placing futures limit order: {e}")
        raise


def register_binance_futures_limit_order(local_mcp_instance, local_binance_client, csv_dir):
    """Register the binance_futures_limit_order tool"""
    @local_mcp_instance.tool()
    def binance_futures_limit_order(symbol: str, side: str, quantity: float, price: float,
                                    position_side: str = 'BOTH', time_in_force: str = 'GTC',
                                    reduce_only: bool = False) -> str:
        """
        Place a futures limit order at a specific price with leverage and save details to CSV.

        ⚠️  EXTREME RISK WARNING - FUTURES TRADING WITH LEVERAGE ⚠️
        Futures limit orders with leverage can result in LIQUIDATION and TOTAL LOSS of funds.
        Losses can EXCEED your initial investment. Monitor liquidation prices constantly!

        Parameters:
            symbol (string, required): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
            side (string, required): Order side - 'BUY' or 'SELL' (case-insensitive)
            quantity (float, required): Amount of contracts to trade
            price (float, required): Limit price for order execution
            position_side (string, optional): Position side (default: 'BOTH')
                - 'BOTH': One-way mode (simpler, default on most accounts)
                - 'LONG': Hedge mode long position
                - 'SHORT': Hedge mode short position
            time_in_force (string, optional): Order duration (default: 'GTC')
                - 'GTC' (Good Till Cancelled): Order stays active until filled or manually cancelled
                - 'IOC' (Immediate Or Cancel): Fill immediately at limit price or better, cancel remainder
                - 'FOK' (Fill Or Kill): Fill entire order immediately at limit price or better, or cancel
                - 'GTX' (Good Till Crossing): Post-only order, won't fill immediately as taker
            reduce_only (boolean, optional): If True, can only reduce position size (default: False)

        Returns:
            str: Formatted response with CSV file containing order placement details.

        CSV Output Columns:
            - orderId (integer): Unique order identifier for tracking
            - clientOrderId (string): Client-assigned order ID
            - symbol (string): Trading pair symbol
            - side (string): Order side (BUY or SELL)
            - positionSide (string): BOTH, LONG, or SHORT
            - type (string): Order type (LIMIT)
            - timeInForce (string): Time in force setting
            - price (float): Limit price
            - origQty (float): Original order quantity
            - executedQty (float): Quantity already executed (0 for new orders)
            - status (string): Order status (NEW, PARTIALLY_FILLED, FILLED, CANCELED)
            - reduceOnly (boolean): True if order can only reduce position
            - updateTime (string): Order update timestamp

        Order Logic:
            Opening Positions:
            - BUY + LONG = Open/increase long position (bet on price increase)
            - SELL + SHORT = Open/increase short position (bet on price decrease)

            Closing/Reducing Positions:
            - SELL + LONG = Close/reduce long position
            - BUY + SHORT = Close/reduce short position
            - Use reduce_only=True to ensure order only closes existing position

        Limit Order Behavior:
            - BUY limit orders execute when market price drops to or below your limit price
            - SELL limit orders execute when market price rises to or above your limit price
            - Orders may partially fill if full quantity isn't available at your price
            - GTC orders remain open until filled or manually cancelled
            - GTX orders ensure you're a maker (get maker fees, not taker fees)

        Use Cases:
            - Enter position at better price than current market
            - Exit position at target profit level
            - Avoid slippage on leveraged orders
            - Implement precise entry/exit strategies
            - Better price control vs market orders
            - Take advantage of price dips/spikes

        Advantages vs Market Orders:
            - Precise price control (no worse than specified price)
            - Protection against slippage
            - Can be used for automatic execution at target prices
            - Lower fees with GTX (maker fees vs taker fees)

        Disadvantages vs Market Orders:
            - No guarantee of execution (may never fill)
            - Requires monitoring if urgent execution needed
            - May miss market moves if price doesn't reach limit
            - Must cancel manually if no longer wanted (GTC orders)

        Risk Management:
            - Verify price is reasonable for current market
            - Ensure quantity meets minimum order size
            - Check leverage setting before placing order
            - Monitor liquidation price after order fills
            - Consider market volatility when setting limit price
            - Use stop-loss orders to protect leveraged positions

        Example usage:
            # Open long when BTC drops to $50,000 (10x leverage assumed already set)
            binance_futures_limit_order(symbol="BTCUSDT", side="BUY", quantity=0.001, price=50000, position_side="LONG")

            # Take profit on long when BTC rises to $55,000 (reduce only)
            binance_futures_limit_order(symbol="BTCUSDT", side="SELL", quantity=0.001, price=55000, position_side="LONG", reduce_only=True)

            # Open short when ETH rises to $3,500
            binance_futures_limit_order(symbol="ETHUSDT", side="SELL", quantity=0.01, price=3500, position_side="SHORT")

            # Maker-only order (GTX ensures you pay maker fees)
            binance_futures_limit_order(symbol="BTCUSDT", side="BUY", quantity=0.001, price=49000, time_in_force="GTX")

        Managing Orders:
            - Check order status: Use binance_get_futures_open_orders tool
            - Cancel order: Use binance_cancel_futures_order tool with orderId
            - View positions: Use binance_manage_futures_positions tool

        CRITICAL Safety Rules:
            - ALWAYS check leverage before placing orders (use binance_set_futures_leverage)
            - NEVER use maximum leverage without extensive experience
            - Set stop-loss orders immediately after opening positions
            - Monitor liquidation price constantly
            - Start with LOW leverage (2x-5x maximum for beginners)
            - Use reduce_only=True when closing positions to avoid flipping direction

        Note:
            - CSV file saved for your records and order tracking
            - Order ID needed to cancel or check status later
            - Partially filled orders can be cancelled (unfilled portion)
            - Leverage affects margin requirements and liquidation risk
            - Check futures account balance before placing orders
        """
        logger.info(f"binance_futures_limit_order tool invoked: {side} {quantity} {symbol} @ {price}")

        # Validate parameters
        if not symbol:
            return "Error: symbol is required (e.g., 'BTCUSDT')"

        if not side:
            return "Error: side is required ('BUY' or 'SELL')"

        try:
            # Execute futures limit order
            df = execute_futures_limit_order(
                binance_client=local_binance_client,
                symbol=symbol,
                side=side,
                quantity=quantity,
                price=price,
                position_side=position_side,
                time_in_force=time_in_force,
                reduce_only=reduce_only
            )

            # Generate filename with unique identifier
            filename = f"futures_limit_{symbol}_{side.lower()}_{str(uuid.uuid4())[:8]}.csv"
            filepath = csv_dir / filename

            # Save to CSV file
            df.to_csv(filepath, index=False)
            logger.info(f"Saved futures limit order to {filename}")

            # Return formatted response
            result = format_csv_response(filepath, df)

            # Add execution summary
            order_data = df.iloc[0]
            summary = f"""

═══════════════════════════════════════════════════════════════════════════════
FUTURES LIMIT ORDER PLACED
═══════════════════════════════════════════════════════════════════════════════
Order ID:        {order_data['orderId']}
Symbol:          {order_data['symbol']}
Side:            {order_data['side']}
Position Side:   {order_data['positionSide']}
Limit Price:     {order_data['price']:.8f}
Quantity:        {order_data['origQty']:.8f}
Time in Force:   {order_data['timeInForce']}
Status:          {order_data['status']}
Executed Qty:    {order_data['executedQty']:.8f}
Reduce Only:     {order_data['reduceOnly']}
Time:            {order_data['updateTime']}
═══════════════════════════════════════════════════════════════════════════════
"""

            # Add status-specific guidance
            if order_data['status'] == 'NEW':
                if order_data['side'] == 'BUY':
                    price_action = f"drops to or below {order_data['price']:.8f}"
                else:
                    price_action = f"rises to or above {order_data['price']:.8f}"

                summary += f"""
Order Status: OPEN (Not Yet Filled)
Your futures limit order is now active and waiting to be filled.
It will execute when the market price {price_action}.

⚠️  Remember to monitor your leverage and liquidation price after order fills!

To check order status:
  binance_get_futures_open_orders(symbol="{order_data['symbol']}")

To cancel this order:
  binance_cancel_futures_order(symbol="{order_data['symbol']}", order_id={order_data['orderId']})
"""
            elif order_data['status'] == 'FILLED':
                summary += """
Order Status: FILLED
Your futures order was filled immediately!

⚠️  IMPORTANT: Check your position now:
  binance_manage_futures_positions(symbol="{}")
  binance_calculate_liquidation_risk(symbol="{}")
""".format(order_data['symbol'], order_data['symbol'])
            elif order_data['status'] == 'PARTIALLY_FILLED':
                summary += f"""
Order Status: PARTIALLY FILLED
{order_data['executedQty']:.8f} / {order_data['origQty']:.8f} executed.
Remaining order is still active.
"""
            elif order_data['status'] == 'CANCELED':
                summary += "\nOrder Status: CANCELED\nOrder was not filled and has been canceled (IOC/FOK).\n"

            summary += "═══════════════════════════════════════════════════════════════════════════════\n"

            return result + summary

        except ValueError as e:
            logger.error(f"Validation error: {e}")
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Error placing futures limit order: {e}")
            return f"Error: {str(e)}\n\nCheck:\n- API credentials valid\n- Futures trading enabled\n- Sufficient margin\n- Correct symbol and parameters\n- Leverage is set appropriately"
