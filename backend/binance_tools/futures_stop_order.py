import logging
from datetime import datetime
import uuid
from mcp_service import format_csv_response
from request_logger import log_request
import pandas as pd
from binance.client import Client
from sentry_utils import with_sentry_tracing

logger = logging.getLogger(__name__)

VALID_ORDER_TYPES = ['STOP_MARKET', 'TAKE_PROFIT_MARKET', 'TRAILING_STOP_MARKET']


@with_sentry_tracing("binance_futures_stop_order")
def execute_futures_stop_order(binance_client: Client, symbol: str, side: str,
                               order_type: str, stop_price: float = 0,
                               callback_rate: float = 0, activation_price: float = 0,
                               quantity: float = 0, position_side: str = 'BOTH',
                               close_position: bool = False,
                               working_type: str = 'MARK_PRICE') -> pd.DataFrame:
    """
    Execute a futures conditional order (stop-loss, take-profit, or trailing stop).

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        side: Order side - 'BUY' or 'SELL'
        order_type: 'STOP_MARKET', 'TAKE_PROFIT_MARKET', or 'TRAILING_STOP_MARKET'
        stop_price: Trigger price (required for STOP_MARKET/TAKE_PROFIT_MARKET)
        callback_rate: Trailing percentage 0.1-5.0 (required for TRAILING_STOP_MARKET)
        activation_price: Price to activate trailing stop (optional)
        quantity: Amount of contracts to trade
        position_side: Position side - 'BOTH', 'LONG', or 'SHORT'
        close_position: If True, close entire position (quantity ignored)
        working_type: Price type for trigger - 'MARK_PRICE' or 'CONTRACT_PRICE'

    Returns:
        DataFrame with order placement details
    """
    logger.info(f"Placing futures {order_type} order for {symbol}")

    # Validate side
    side = side.upper()
    if side not in ['BUY', 'SELL']:
        raise ValueError("side must be 'BUY' or 'SELL'")

    # Validate order_type
    order_type = order_type.upper()
    if order_type not in VALID_ORDER_TYPES:
        raise ValueError(f"order_type must be one of: {', '.join(VALID_ORDER_TYPES)}")

    # Validate position_side
    position_side = position_side.upper()
    if position_side not in ['BOTH', 'LONG', 'SHORT']:
        raise ValueError("position_side must be 'BOTH', 'LONG', or 'SHORT'")

    # Validate working_type
    working_type = working_type.upper()
    if working_type not in ['MARK_PRICE', 'CONTRACT_PRICE']:
        raise ValueError("working_type must be 'MARK_PRICE' or 'CONTRACT_PRICE'")

    # Validate order-type specific parameters
    if order_type == 'TRAILING_STOP_MARKET':
        if callback_rate < 0.1 or callback_rate > 5.0:
            raise ValueError("callback_rate must be between 0.1 and 5.0 (percent)")
    else:  # STOP_MARKET or TAKE_PROFIT_MARKET
        if stop_price <= 0:
            raise ValueError(f"stop_price is required for {order_type}")

    # Validate quantity/close_position
    if quantity <= 0 and not close_position:
        raise ValueError("Either quantity > 0 or close_position=True is required")

    try:
        # Build order parameters
        params = {
            'symbol': symbol,
            'side': side,
            'type': order_type,
            'positionSide': position_side,
            'workingType': working_type,
            'reduceOnly': 'true',  # Stop orders should only reduce position
        }

        # Add order-type specific parameters
        if order_type == 'TRAILING_STOP_MARKET':
            params['callbackRate'] = callback_rate
            if activation_price > 0:
                params['activationPrice'] = activation_price
        else:
            params['stopPrice'] = stop_price

        # Add quantity or closePosition
        if close_position:
            params['closePosition'] = 'true'
            # Remove reduceOnly when using closePosition (they conflict)
            del params['reduceOnly']
        else:
            params['quantity'] = quantity

        # Execute the order
        logger.warning(f"PLACING FUTURES {order_type}: {side} {symbol} position_side={position_side}")
        order = binance_client.futures_create_order(**params)

        # Handle both basic orders (orderId) and algo orders (algoId)
        order_id = order.get('orderId') or order.get('algoId')
        logger.info(f"Futures stop order placed. Order ID: {order_id}, Status: {order['status']}")

        # Build record with all relevant fields
        record = {
            'orderId': order_id,
            'symbol': order['symbol'],
            'side': order['side'],
            'positionSide': order['positionSide'],
            'type': order['type'],
            'status': order['status'],
            'origQty': float(order.get('origQty', 0)),
            'executedQty': float(order.get('executedQty', 0)),
            'stopPrice': float(order.get('stopPrice', 0)),
            'activatePrice': float(order.get('activatePrice', 0)),
            'priceRate': float(order.get('priceRate', 0)),  # callbackRate in response
            'workingType': order.get('workingType', working_type),
            'closePosition': close_position,
            'reduceOnly': order.get('reduceOnly', not close_position),
            'updateTime': datetime.fromtimestamp(order['updateTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        }

        df = pd.DataFrame([record])
        logger.info(f"Futures {order_type} order placed successfully")

        return df

    except Exception as e:
        logger.error(f"Error placing futures stop order: {e}")
        raise


def register_binance_futures_stop_order(local_mcp_instance, local_binance_client, csv_dir, requests_dir):
    """Register the binance_futures_stop_order tool"""
    @local_mcp_instance.tool()
    def binance_futures_stop_order(requester: str, symbol: str, side: str,
                                   order_type: str = 'STOP_MARKET',
                                   stop_price: float = 0,
                                   callback_rate: float = 0,
                                   activation_price: float = 0,
                                   quantity: float = 0,
                                   position_side: str = 'BOTH',
                                   close_position: bool = False,
                                   working_type: str = 'MARK_PRICE') -> str:
        """
        Place a futures conditional order (stop-loss, take-profit, or trailing stop).

        RISK WARNING: These orders execute at MARKET price when triggered.
        Slippage may occur in volatile markets. Monitor your positions!

        Parameters:
            requester (string, required): Identifier of the user/system making the request
            symbol (string, required): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
            side (string, required): Order side - 'BUY' or 'SELL' (case-insensitive)
            order_type (string, optional): Type of conditional order (default: 'STOP_MARKET')
                - 'STOP_MARKET': Stop-loss - triggers market sell/buy when price hits stop
                - 'TAKE_PROFIT_MARKET': Take-profit - triggers market sell/buy at target
                - 'TRAILING_STOP_MARKET': Dynamic stop that follows price movement
            stop_price (float, optional): Trigger price (required for STOP_MARKET/TAKE_PROFIT_MARKET)
            callback_rate (float, optional): Trailing percentage 0.1-5.0 (required for TRAILING_STOP_MARKET)
                - Example: 2.0 means stop trails 2% behind the peak/bottom price
            activation_price (float, optional): Price at which trailing stop activates
                - If not set, trailing activates immediately
            quantity (float, optional): Amount of contracts (use 0 with close_position=True)
            position_side (string, optional): Position side (default: 'BOTH')
                - 'BOTH': One-way mode (simpler, default on most accounts)
                - 'LONG': Hedge mode long position
                - 'SHORT': Hedge mode short position
            close_position (boolean, optional): If True, closes entire position (default: False)
            working_type (string, optional): Price type for trigger (default: 'MARK_PRICE')
                - 'MARK_PRICE': Uses mark price (recommended, avoids manipulation)
                - 'CONTRACT_PRICE': Uses last traded price

        Trigger Logic:
            STOP_MARKET (Stop-Loss):
            - SELL: Triggers when price <= stop_price (protect LONG from falling)
            - BUY: Triggers when price >= stop_price (protect SHORT from rising)

            TAKE_PROFIT_MARKET:
            - SELL: Triggers when price >= stop_price (take profit on LONG)
            - BUY: Triggers when price <= stop_price (take profit on SHORT)

            TRAILING_STOP_MARKET:
            - SELL: Activates, then triggers if price drops callback_rate% from peak
            - BUY: Activates, then triggers if price rises callback_rate% from bottom

        Returns:
            str: Formatted response with CSV file containing order details.

        CSV Output Columns:
            - orderId (integer): Unique order identifier for tracking
            - symbol (string): Trading pair symbol
            - side (string): Order side (BUY or SELL)
            - positionSide (string): BOTH, LONG, or SHORT
            - type (string): Order type (STOP_MARKET, TAKE_PROFIT_MARKET, TRAILING_STOP_MARKET)
            - status (string): Order status (NEW, FILLED, CANCELED)
            - origQty (float): Order quantity
            - executedQty (float): Quantity executed (0 for pending orders)
            - stopPrice (float): Stop/trigger price (0 for trailing)
            - activatePrice (float): Trailing activation price
            - priceRate (float): Trailing callback rate
            - workingType (string): MARK_PRICE or CONTRACT_PRICE
            - closePosition (boolean): True if closing entire position
            - reduceOnly (boolean): True (stop orders only reduce positions)
            - updateTime (string): Order update timestamp

        Example usage:
            # STOP-LOSS for LONG position (close if BTC drops to $90,000)
            binance_futures_stop_order(
                symbol="BTCUSDT",
                side="SELL",
                order_type="STOP_MARKET",
                stop_price=90000,
                close_position=True,
                position_side="LONG"
            )

            # TAKE-PROFIT for LONG (close if BTC rises to $110,000)
            binance_futures_stop_order(
                symbol="BTCUSDT",
                side="SELL",
                order_type="TAKE_PROFIT_MARKET",
                stop_price=110000,
                close_position=True,
                position_side="LONG"
            )

            # STOP-LOSS for SHORT (close if ETH rises to $4,000)
            binance_futures_stop_order(
                symbol="ETHUSDT",
                side="BUY",
                order_type="STOP_MARKET",
                stop_price=4000,
                close_position=True,
                position_side="SHORT"
            )

            # TRAILING STOP for LONG (activate at $100k, trail by 2%)
            binance_futures_stop_order(
                symbol="BTCUSDT",
                side="SELL",
                order_type="TRAILING_STOP_MARKET",
                callback_rate=2.0,
                activation_price=100000,
                close_position=True,
                position_side="LONG"
            )

            # TRAILING STOP for SHORT (immediate activation, trail by 1.5%)
            binance_futures_stop_order(
                symbol="ETHUSDT",
                side="BUY",
                order_type="TRAILING_STOP_MARKET",
                callback_rate=1.5,
                close_position=True,
                position_side="SHORT"
            )

        Managing Orders:
            - Check order status: binance_get_futures_conditional_orders(symbol="BTCUSDT")
            - Cancel order: binance_cancel_algo_order(symbol="BTCUSDT", algo_id=12345)

        Best Practices:
            - ALWAYS set stop-loss immediately after opening a position
            - Use MARK_PRICE (default) to avoid manipulation-triggered stops
            - For trailing stops: 1-3% callback_rate for volatile assets, 0.5-1% for stable
            - Combine stop-loss AND take-profit for complete risk management
            - Monitor liquidation price - stop-loss should trigger BEFORE liquidation

        Note:
            - Stop orders do not consume margin until triggered
            - Orders are reduceOnly by default (cannot accidentally flip position)
            - Trailing stops follow the highest/lowest price since activation
            - CSV file saved for order tracking and audit
        """
        logger.info(f"binance_futures_stop_order tool invoked: {order_type} {side} {symbol} by {requester}")

        # Validate required parameters
        if not symbol:
            return "Error: symbol is required (e.g., 'BTCUSDT')"

        if not side:
            return "Error: side is required ('BUY' or 'SELL')"

        try:
            # Execute the stop order
            df = execute_futures_stop_order(
                binance_client=local_binance_client,
                symbol=symbol,
                side=side,
                order_type=order_type,
                stop_price=stop_price,
                callback_rate=callback_rate,
                activation_price=activation_price,
                quantity=quantity,
                position_side=position_side,
                close_position=close_position,
                working_type=working_type
            )

            # Generate filename with unique identifier
            order_type_short = order_type.lower().replace('_market', '')
            filename = f"futures_{order_type_short}_{symbol}_{side.lower()}_{str(uuid.uuid4())[:8]}.csv"
            filepath = csv_dir / filename

            # Save to CSV file
            df.to_csv(filepath, index=False)
            logger.info(f"Saved futures stop order to {filename}")

            # Return formatted response
            result = format_csv_response(filepath, df)

            # Add execution summary
            order_data = df.iloc[0]

            # Build order-type specific info
            if order_type.upper() == 'TRAILING_STOP_MARKET':
                trigger_info = f"Callback Rate:   {callback_rate}%"
                if activation_price > 0:
                    trigger_info += f"\nActivation:      {activation_price}"
                else:
                    trigger_info += "\nActivation:      Immediate"
            else:
                trigger_info = f"Stop Price:      {order_data['stopPrice']}"

            summary = f"""

FUTURES {order_type.replace('_', ' ')} ORDER PLACED
Order ID:        {order_data['orderId']}
Symbol:          {order_data['symbol']}
Side:            {order_data['side']}
Position Side:   {order_data['positionSide']}
{trigger_info}
Quantity:        {'CLOSE ALL' if close_position else order_data['origQty']}
Working Type:    {order_data['workingType']}
Status:          {order_data['status']}
Time:            {order_data['updateTime']}
"""

            # Add status-specific guidance
            if order_data['status'] == 'NEW':
                if order_type.upper() == 'TRAILING_STOP_MARKET':
                    summary += f"""
Order Status: ACTIVE (Trailing)
Your trailing stop is now active and will follow the market price.
It will trigger when price moves {callback_rate}% against you from the peak/bottom.
"""
                else:
                    if order_data['side'] == 'SELL':
                        if order_type.upper() == 'STOP_MARKET':
                            trigger_condition = f"drops to or below {order_data['stopPrice']}"
                        else:  # TAKE_PROFIT_MARKET
                            trigger_condition = f"rises to or above {order_data['stopPrice']}"
                    else:  # BUY
                        if order_type.upper() == 'STOP_MARKET':
                            trigger_condition = f"rises to or above {order_data['stopPrice']}"
                        else:  # TAKE_PROFIT_MARKET
                            trigger_condition = f"drops to or below {order_data['stopPrice']}"

                    summary += f"""
Order Status: ACTIVE (Pending Trigger)
Your conditional order is now active and waiting.
It will execute when the {order_data['workingType'].replace('_', ' ').lower()} {trigger_condition}.

To check order status:
  binance_get_futures_conditional_orders(symbol="{order_data['symbol']}")

To cancel this order:
  binance_cancel_algo_order(symbol="{order_data['symbol']}", algo_id={order_data['orderId']})
"""
            elif order_data['status'] == 'FILLED':
                summary += """
Order Status: FILLED
Your stop order was triggered and filled immediately!
Check your position with binance_manage_futures_positions().
"""

            log_request(
                requests_dir=requests_dir,
                requester=requester,
                tool_name="binance_futures_stop_order",
                input_params={
                    "symbol": symbol,
                    "side": side,
                    "order_type": order_type,
                    "stop_price": stop_price,
                    "callback_rate": callback_rate,
                    "activation_price": activation_price,
                    "quantity": quantity,
                    "position_side": position_side,
                    "close_position": close_position,
                    "working_type": working_type
                },
                output_result=result + summary
            )

            return result + summary

        except ValueError as e:
            logger.error(f"Validation error: {e}")
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Error placing futures stop order: {e}")
            return f"Error: {str(e)}\n\nCheck:\n- API credentials valid\n- Futures trading enabled\n- Correct symbol format (e.g., 'BTCUSDT')\n- Valid order_type: STOP_MARKET, TAKE_PROFIT_MARKET, TRAILING_STOP_MARKET"
