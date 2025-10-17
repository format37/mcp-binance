import logging
from datetime import datetime
from decimal import Decimal
import uuid
from mcp_service import format_csv_response
import pandas as pd
from binance.client import Client
from sentry_utils import with_sentry_tracing
from .validation_helpers import validate_futures_margin

logger = logging.getLogger(__name__)


@with_sentry_tracing("binance_trade_futures_market")
def execute_futures_market_order(binance_client: Client, symbol: str, side: str,
                                quantity: float, position_side: str = 'BOTH',
                                close_position: bool = False) -> pd.DataFrame:
    """
    Execute a futures market order and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        side: Order side - 'BUY' or 'SELL'
        quantity: Amount of contracts to trade
        position_side: Position side - 'BOTH' (one-way), 'LONG', or 'SHORT' (hedge mode)
        close_position: If True, close entire position (quantity ignored)

    Returns:
        DataFrame with order execution details

    Note:
        BUY + LONG = Open long, SELL + LONG = Close long
        SELL + SHORT = Open short, BUY + SHORT = Close short
        WARNING: Futures trading involves liquidation risk!
    """
    logger.info(f"Executing futures market {side} order for {symbol}")

    # Validate parameters
    side = side.upper()
    if side not in ['BUY', 'SELL']:
        raise ValueError("side must be 'BUY' or 'SELL'")

    position_side = position_side.upper()
    if position_side not in ['BOTH', 'LONG', 'SHORT']:
        raise ValueError("position_side must be 'BOTH', 'LONG', or 'SHORT'")

    try:
        # Get current position if closing
        if close_position:
            positions = binance_client.futures_position_information(symbol=symbol)
            for pos in positions:
                if pos['positionSide'] == position_side:
                    pos_amt = Decimal(pos['positionAmt'])
                    if pos_amt != 0:
                        quantity = float(abs(pos_amt))
                        break
            if quantity == 0:
                raise ValueError(f"No open position found for {symbol} {position_side}")

        # Validate margin availability before placing order (unless closing position)
        if not close_position:
            is_valid, error_msg = validate_futures_margin(
                binance_client=binance_client,
                symbol=symbol,
                quantity=quantity,
                side=side
            )
            if not is_valid:
                logger.warning(f"Margin validation failed for {symbol}: {error_msg}")
                raise ValueError(error_msg)

        # Execute the order
        params = {
            'symbol': symbol,
            'side': side,
            'type': 'MARKET',
            'quantity': quantity,
            'positionSide': position_side
        }

        if close_position:
            params['reduceOnly'] = True

        logger.warning(f"⚠️  EXECUTING REAL FUTURES ORDER: {side} {quantity} {symbol} {position_side}")
        order = binance_client.futures_create_order(**params)
        logger.info(f"Futures order executed. Order ID: {order['orderId']}")

        # Get updated position
        positions = binance_client.futures_position_information(symbol=symbol)
        position_info = {}
        for pos in positions:
            if pos['positionSide'] == position_side:
                pos_amt = Decimal(pos['positionAmt'])
                if pos_amt != 0 or close_position:
                    position_info = {
                        'positionAmt': pos['positionAmt'],
                        'entryPrice': pos['entryPrice'],
                        'markPrice': pos['markPrice'],
                        'liquidationPrice': pos['liquidationPrice'],
                        'unRealizedProfit': pos['unRealizedProfit'],
                        'leverage': pos['leverage']
                    }
                    break

        # Create record
        record = {
            'orderId': order['orderId'],
            'symbol': order['symbol'],
            'side': order['side'],
            'positionSide': order['positionSide'],
            'type': order['type'],
            'status': order['status'],
            'executedQty': float(order['executedQty']),
            'avgPrice': float(order.get('avgPrice', 0)),
            'updateTime': datetime.fromtimestamp(order['updateTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
            'reduceOnly': close_position,
            'positionAmt': position_info.get('positionAmt', '0'),
            'entryPrice': position_info.get('entryPrice', '0'),
            'markPrice': position_info.get('markPrice', '0'),
            'liquidationPrice': position_info.get('liquidationPrice', '0'),
            'unRealizedProfit': position_info.get('unRealizedProfit', '0'),
            'leverage': position_info.get('leverage', '0')
        }

        df = pd.DataFrame([record])
        logger.info(f"Futures market order executed successfully")

        return df

    except Exception as e:
        logger.error(f"Error executing futures market order: {e}")
        raise


def register_binance_trade_futures_market(local_mcp_instance, local_binance_client, csv_dir):
    """Register the binance_trade_futures_market tool"""
    @local_mcp_instance.tool()
    def binance_trade_futures_market(symbol: str, side: str, quantity: float,
                                     position_side: str = 'BOTH', close_position: bool = False) -> str:
        """
        Execute futures market order with leverage and save execution details to CSV.

        ⚠️  EXTREME RISK WARNING - FUTURES TRADING ⚠️
        Futures trading with leverage can result in LIQUIDATION and TOTAL LOSS of funds.
        Losses can EXCEED your initial investment. Only trade with funds you can afford to lose.
        ALWAYS use stop-loss orders and monitor liquidation prices.

        Parameters:
            symbol (string, required): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
            side (string, required): Order side - 'BUY' or 'SELL'
            quantity (float, required): Amount of contracts to trade
            position_side (string, optional): Position side (default: 'BOTH')
                - 'BOTH': One-way mode (simpler, default on most accounts)
                - 'LONG': Hedge mode long position
                - 'SHORT': Hedge mode short position
            close_position (boolean, optional): If True, closes entire position (default: False)

        Order Logic:
            Opening Positions:
            - BUY + LONG = Open long position (bet on price increase)
            - SELL + SHORT = Open short position (bet on price decrease)

            Closing Positions:
            - SELL + LONG = Close long position
            - BUY + SHORT = Close short position
            - Use close_position=True to close entire position automatically

        Returns:
            str: Formatted response with CSV file containing order execution and position details.

        CSV Output Columns:
            - orderId (integer): Unique order identifier
            - symbol (string): Trading pair
            - side (string): BUY or SELL
            - positionSide (string): BOTH, LONG, or SHORT
            - type (string): MARKET
            - status (string): Order status (typically FILLED)
            - executedQty (float): Quantity executed
            - avgPrice (float): Average execution price
            - updateTime (string): Execution timestamp
            - reduceOnly (boolean): True if closing position
            - positionAmt (float): Current position size after execution
            - entryPrice (float): Position entry price
            - markPrice (float): Current mark price
            - liquidationPrice (float): Liquidation price
            - unRealizedProfit (float): Current unrealized P&L
            - leverage (integer): Position leverage

        Risk Factors:
            - Leverage amplifies BOTH gains AND losses
            - 10x leverage = 10% adverse move = liquidation
            - 50x leverage = 2% adverse move = liquidation
            - 125x leverage = 0.8% adverse move = liquidation
            - Funding rates paid/received every 8 hours
            - Volatile markets can cause rapid liquidation

        Use Cases:
            - Quick entry into leveraged positions
            - Fast exit from positions
            - High-frequency trading strategies
            - Market orders when speed > price precision

        Example usage:
            # Open long position (bet on price increase)
            binance_trade_futures_market(symbol="BTCUSDT", side="BUY", quantity=0.001, position_side="LONG")

            # Open short position (bet on price decrease)
            binance_trade_futures_market(symbol="ETHUSDT", side="SELL", quantity=0.01, position_side="SHORT")

            # Close long position
            binance_trade_futures_market(symbol="BTCUSDT", side="SELL", quantity=0.001, position_side="LONG")

            # Close entire position automatically
            binance_trade_futures_market(symbol="BTCUSDT", side="SELL", quantity=0, position_side="LONG", close_position=True)

        CRITICAL Safety Rules:
            - Start with LOW leverage (2x-5x maximum for beginners)
            - ALWAYS set stop-loss orders immediately after opening
            - NEVER risk more than 1-2% of capital per trade
            - Monitor liquidation price constantly
            - Be aware of funding rates
            - Understand liquidation mechanics before trading

        Note:
            - Requires futures trading permissions on API key
            - Check leverage setting with binance_set_futures_leverage before trading
            - Monitor positions with binance_get_futures_balances
            - Calculate risk with binance_calculate_liquidation_risk
            - This operation is irreversible once executed
        """
        logger.info(f"binance_trade_futures_market tool invoked: {side} {symbol}")

        if not symbol or not side:
            return "Error: symbol and side are required"

        if quantity <= 0 and not close_position:
            return "Error: quantity must be positive (or use close_position=True)"

        try:
            # Execute order
            df = execute_futures_market_order(
                binance_client=local_binance_client,
                symbol=symbol,
                side=side,
                quantity=quantity,
                position_side=position_side,
                close_position=close_position
            )

            # Save to CSV
            filename = f"futures_market_{symbol}_{side.lower()}_{str(uuid.uuid4())[:8]}.csv"
            filepath = csv_dir / filename
            df.to_csv(filepath, index=False)
            logger.info(f"Saved futures order to {filename}")

            result = format_csv_response(filepath, df)

            # Add execution summary
            order_data = df.iloc[0]
            summary = f"""

═══════════════════════════════════════════════════════════════════════════════
FUTURES ORDER EXECUTED
═══════════════════════════════════════════════════════════════════════════════
Order ID:          {order_data['orderId']}
Symbol:            {order_data['symbol']}
Side:              {order_data['side']}
Position Side:     {order_data['positionSide']}
Status:            {order_data['status']}
Executed Qty:      {order_data['executedQty']}
Avg Price:         {order_data['avgPrice']}
Time:              {order_data['updateTime']}

POSITION AFTER EXECUTION:
Position Size:     {order_data['positionAmt']} contracts
Entry Price:       {order_data['entryPrice']}
Mark Price:        {order_data['markPrice']}
Liquidation Price: {order_data['liquidationPrice']}
Unrealized P&L:    {order_data['unRealizedProfit']} USDT
Leverage:          {order_data['leverage']}x
"""

            # Calculate liquidation distance
            if order_data['positionAmt'] != '0' and order_data['liquidationPrice'] != '0':
                liq_price = float(order_data['liquidationPrice'])
                mark_price = float(order_data['markPrice'])
                if mark_price > 0:
                    liq_distance = abs((liq_price - mark_price) / mark_price * 100)
                    summary += f"\n⚠️  Distance to liquidation: {liq_distance:.2f}%\n"
                    if liq_distance < 5:
                        summary += "🚨 CRITICAL: Very close to liquidation!\n"
                    elif liq_distance < 10:
                        summary += "⚠️  WARNING: High liquidation risk!\n"

            summary += "═══════════════════════════════════════════════════════════════════════════════\n"

            return result + summary

        except ValueError as e:
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Error executing futures order: {e}")
            return f"Error: {str(e)}\n\nCheck:\n- API credentials valid\n- Futures trading enabled\n- Sufficient margin\n- Correct symbol and parameters"
