import logging
from datetime import datetime
from decimal import Decimal
import uuid
from mcp_service import format_csv_response
import pandas as pd
from binance.client import Client
from typing import Optional
from sentry_utils import with_sentry_tracing

logger = logging.getLogger(__name__)


@with_sentry_tracing("binance_spot_market_order")
def execute_market_order(binance_client: Client, symbol: str, side: str,
                        quantity: Optional[float] = None, quote_quantity: Optional[float] = None) -> pd.DataFrame:
    """
    Execute a market order on Binance spot market and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        side: Order side - 'BUY' or 'SELL'
        quantity: Amount of base asset to buy/sell (e.g., 0.001 BTC)
        quote_quantity: Amount of quote asset to spend (BUY only, e.g., 100 USDT)

    Returns:
        DataFrame with order execution details containing columns:
        - orderId: Unique order identifier
        - symbol: Trading pair symbol
        - side: Order side (BUY or SELL)
        - type: Order type (MARKET)
        - status: Order status (typically FILLED)
        - executedQty: Quantity executed
        - cummulativeQuoteQty: Total cost/proceeds
        - avgPrice: Average execution price
        - transactTime: Transaction timestamp
        - fills: Number of fills
        - commission: Total commission paid
        - commissionAsset: Asset in which commission was paid

    Note:
        Market orders execute immediately at current market price.
        WARNING: This executes REAL TRADES with REAL MONEY.
    """
    logger.info(f"Executing market {side} order for {symbol}")

    # Validate parameters
    side = side.upper()
    if side not in ['BUY', 'SELL']:
        raise ValueError("side must be 'BUY' or 'SELL'")

    if side == 'SELL' and quote_quantity:
        raise ValueError("quote_quantity is only supported for BUY orders")

    if not quantity and not quote_quantity:
        raise ValueError("Must specify either quantity or quote_quantity")

    if quantity and quote_quantity:
        raise ValueError("Cannot specify both quantity and quote_quantity")

    try:
        # Prepare order parameters
        order_params = {
            'symbol': symbol,
            'side': side,
            'type': 'MARKET'
        }

        if quantity:
            order_params['quantity'] = quantity
            logger.info(f"Order quantity: {quantity}")
        else:
            order_params['quoteOrderQty'] = quote_quantity
            logger.info(f"Order quote quantity: {quote_quantity}")

        # Execute the order
        logger.warning(f"⚠️  EXECUTING REAL MARKET ORDER: {side} {symbol}")
        order = binance_client.order_market(**order_params)
        logger.info(f"Order executed successfully. Order ID: {order['orderId']}")

        # Calculate execution details
        executed_qty = Decimal(order['executedQty'])
        cumulative_quote_qty = Decimal(order['cummulativeQuoteQty'])
        avg_price = cumulative_quote_qty / executed_qty if executed_qty > 0 else Decimal('0')

        # Calculate total commission
        total_commission = Decimal('0')
        commission_asset = None
        if 'fills' in order and order['fills']:
            for fill in order['fills']:
                total_commission += Decimal(fill['commission'])
                commission_asset = fill['commissionAsset']

        # Create record
        record = {
            'orderId': order['orderId'],
            'symbol': order['symbol'],
            'side': order['side'],
            'type': order['type'],
            'status': order['status'],
            'executedQty': float(executed_qty),
            'cummulativeQuoteQty': float(cumulative_quote_qty),
            'avgPrice': float(avg_price),
            'transactTime': datetime.fromtimestamp(order['transactTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
            'fills': len(order.get('fills', [])),
            'commission': float(total_commission),
            'commissionAsset': commission_asset if commission_asset else ''
        }

        # Create DataFrame
        df = pd.DataFrame([record])

        logger.info(f"Market order executed: {executed_qty} @ {avg_price}, total: {cumulative_quote_qty}")

        return df

    except Exception as e:
        logger.error(f"Error executing market order: {e}")
        raise


def register_binance_spot_market_order(local_mcp_instance, local_binance_client, csv_dir):
    """Register the binance_spot_market_order tool"""
    @local_mcp_instance.tool()
    def binance_spot_market_order(symbol: str, side: str, quantity: float = None, quote_quantity: float = None) -> str:
        """
        Execute a market order on Binance spot market and save execution details to CSV.

        ⚠️  WARNING: THIS EXECUTES REAL TRADES WITH REAL MONEY ⚠️
        Market orders execute IMMEDIATELY at the current market price. Use with caution and
        always verify parameters before execution. This operation cannot be undone.

        Parameters:
            symbol (string, required): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
            side (string, required): Order side - 'BUY' or 'SELL' (case-insensitive)
            quantity (float, optional): Amount of base asset to trade (e.g., 0.001 for 0.001 BTC)
            quote_quantity (float, optional): Amount of quote asset to spend (BUY orders only, e.g., 100 for $100 USDT)

        Returns:
            str: Formatted response with CSV file containing order execution details, including
                order ID, executed quantity, average price, total cost, and commission paid.

        CSV Output Columns:
            - orderId (integer): Unique order identifier for tracking and reference
            - symbol (string): Trading pair symbol (e.g., 'BTCUSDT')
            - side (string): Order side (BUY or SELL)
            - type (string): Order type (MARKET)
            - status (string): Order status (typically FILLED for market orders)
            - executedQty (float): Total quantity executed
            - cummulativeQuoteQty (float): Total cost (BUY) or proceeds (SELL) in quote asset
            - avgPrice (float): Average execution price
            - transactTime (string): Transaction timestamp (YYYY-MM-DD HH:MM:SS)
            - fills (integer): Number of fills/trades that completed this order
            - commission (float): Total commission/fee paid
            - commissionAsset (string): Asset in which commission was paid (e.g., 'BNB', 'USDT')

        Parameter Rules:
            - For BUY orders: Specify EITHER quantity OR quote_quantity (not both)
                * quantity: Buys specified amount of base asset at market price
                * quote_quantity: Spends specified amount of quote asset at market price
            - For SELL orders: Must specify quantity only (quote_quantity not supported)
            - Cannot specify both quantity and quote_quantity
            - Must specify at least one of quantity or quote_quantity

        Use Cases:
            - Quick market entry when speed is critical
            - Exit positions immediately at current price
            - High liquidity pairs where slippage is minimal
            - When exact price is less important than immediate execution
            - Stop-loss execution (though stop-loss orders are better)
            - Taking profit at market price

        Advantages:
            - Immediate execution (no waiting for price to reach limit)
            - Guaranteed fill in liquid markets
            - Simple to use (no price calculation needed)

        Disadvantages:
            - No price control (executes at current market price)
            - Subject to slippage in volatile or illiquid markets
            - May get worse price than expected in fast-moving markets

        Risk Management:
            - Always verify you have sufficient balance before execution
            - Double-check symbol and side to avoid costly mistakes
            - Use limit orders when precise price control is needed
            - Be aware of potential slippage in volatile markets
            - Consider market depth for large orders

        Example usage:
            # Buy 0.001 BTC at current market price
            binance_spot_market_order(symbol="BTCUSDT", side="BUY", quantity=0.001)

            # Spend 100 USDT to buy ETH at current market price
            binance_spot_market_order(symbol="ETHUSDT", side="BUY", quote_quantity=100)

            # Sell 0.01 ETH at current market price
            binance_spot_market_order(symbol="ETHUSDT", side="SELL", quantity=0.01)

        Note:
            - Market orders typically fill instantly but price is not guaranteed
            - CSV file is saved for your records and transaction history
            - Always check your account balance before placing orders
            - Commission is automatically deducted from the transaction
            - For better price control, consider using binance_spot_limit_order instead
            - This is an irreversible operation - orders cannot be cancelled once executed
        """
        logger.info(f"binance_spot_market_order tool invoked: {side} {symbol}")

        # Validate parameters
        if not symbol:
            return "Error: symbol is required (e.g., 'BTCUSDT')"

        if not side:
            return "Error: side is required ('BUY' or 'SELL')"

        try:
            # Execute market order
            df = execute_market_order(
                binance_client=local_binance_client,
                symbol=symbol,
                side=side,
                quantity=quantity,
                quote_quantity=quote_quantity
            )

            # Generate filename with unique identifier
            filename = f"market_order_{symbol}_{side.lower()}_{str(uuid.uuid4())[:8]}.csv"
            filepath = csv_dir / filename

            # Save to CSV file
            df.to_csv(filepath, index=False)
            logger.info(f"Saved market order to {filename}")

            # Return formatted response
            result = format_csv_response(filepath, df)

            # Add execution summary to response
            order_data = df.iloc[0]
            summary = f"""

═══════════════════════════════════════════════════════════════════════════════
ORDER EXECUTED SUCCESSFULLY
═══════════════════════════════════════════════════════════════════════════════
Order ID:        {order_data['orderId']}
Symbol:          {order_data['symbol']}
Side:            {order_data['side']}
Status:          {order_data['status']}
Executed Qty:    {order_data['executedQty']:.8f}
Average Price:   {order_data['avgPrice']:.8f}
Total Cost:      {order_data['cummulativeQuoteQty']:.8f}
Commission:      {order_data['commission']:.8f} {order_data['commissionAsset']}
Time:            {order_data['transactTime']}
═══════════════════════════════════════════════════════════════════════════════
"""

            return result + summary

        except ValueError as e:
            logger.error(f"Validation error: {e}")
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Error executing market order: {e}")
            return f"Error executing market order: {str(e)}\n\nPlease check:\n- API credentials are valid\n- Symbol is correct\n- Sufficient balance available\n- API key has trading permissions"
