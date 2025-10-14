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


@with_sentry_tracing("binance_list_futures_positions")
def list_futures_positions(binance_client: Client, symbol: Optional[str] = None) -> pd.DataFrame:
    """
    List all open futures positions and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        symbol: Optional specific symbol to filter (e.g., 'BTCUSDT')

    Returns:
        DataFrame with position details including P&L, liquidation prices, risk metrics

    Note:
        Only returns positions with non-zero position amounts.
    """
    logger.info(f"Listing futures positions" + (f" for {symbol}" if symbol else ""))

    try:
        # Get positions
        if symbol:
            positions = binance_client.futures_position_information(symbol=symbol)
        else:
            positions = binance_client.futures_position_information()

        position_records = []
        for position in positions:
            pos_amt = Decimal(position['positionAmt'])
            if pos_amt == 0:
                continue  # Skip zero positions

            entry_price = Decimal(position['entryPrice'])
            mark_price = Decimal(position['markPrice'])
            liquidation_price = Decimal(position['liquidationPrice']) if position['liquidationPrice'] != '0' else None
            unrealized_pnl = Decimal(position['unRealizedProfit'])
            leverage = int(position['leverage'])

            # Determine direction
            direction = "LONG" if pos_amt > 0 else "SHORT"

            # Calculate price change
            if entry_price > 0:
                price_change_pct = float(((mark_price - entry_price) / entry_price) * 100)
            else:
                price_change_pct = 0.0

            # Calculate liquidation distance
            if liquidation_price and mark_price > 0:
                liq_distance_pct = float(abs((liquidation_price - mark_price) / mark_price * 100))
            else:
                liq_distance_pct = None

            # Calculate ROI
            notional = abs(pos_amt * entry_price)
            margin = notional / leverage if leverage > 0 else notional
            roi_pct = float((unrealized_pnl / margin) * 100) if margin > 0 else 0.0

            # Risk assessment
            if liq_distance_pct:
                if liq_distance_pct < 5:
                    risk_level = "CRITICAL"
                elif liq_distance_pct < 10:
                    risk_level = "HIGH"
                elif liq_distance_pct < 20:
                    risk_level = "MEDIUM"
                else:
                    risk_level = "LOW"
            else:
                risk_level = "UNKNOWN"

            position_records.append({
                'symbol': position['symbol'],
                'direction': direction,
                'positionSide': position['positionSide'],
                'positionAmt': float(pos_amt),
                'entryPrice': float(entry_price),
                'markPrice': float(mark_price),
                'priceChangePct': price_change_pct,
                'liquidationPrice': float(liquidation_price) if liquidation_price else None,
                'liqDistancePct': liq_distance_pct,
                'riskLevel': risk_level,
                'unRealizedProfit': float(unrealized_pnl),
                'roiPct': roi_pct,
                'leverage': leverage,
                'notionalValue': float(notional),
                'margin': float(margin),
                'initialMargin': float(position['initialMargin']),
                'maintMargin': float(position['maintMargin']),
                'isolated': position['isolated'],
                'updateTime': datetime.fromtimestamp(int(position['updateTime']) / 1000).strftime('%Y-%m-%d %H:%M:%S')
            })

        df = pd.DataFrame(position_records) if position_records else pd.DataFrame(columns=[
            'symbol', 'direction', 'positionSide', 'positionAmt', 'entryPrice', 'markPrice',
            'priceChangePct', 'liquidationPrice', 'liqDistancePct', 'riskLevel', 'unRealizedProfit',
            'roiPct', 'leverage', 'notionalValue', 'margin', 'initialMargin', 'maintMargin',
            'isolated', 'updateTime'
        ])

        logger.info(f"Retrieved {len(position_records)} open positions")

        return df

    except Exception as e:
        logger.error(f"Error listing positions: {e}")
        raise


@with_sentry_tracing("binance_close_futures_position")
def close_futures_position(binance_client: Client, symbol: str, position_side: str = 'BOTH') -> pd.DataFrame:
    """
    Close a futures position by executing a market order.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        position_side: Position side to close - 'BOTH', 'LONG', or 'SHORT'

    Returns:
        DataFrame with position closure details

    Note:
        Executes market order with reduceOnly=True to close position.
    """
    logger.info(f"Closing {position_side} position for {symbol}")

    position_side = position_side.upper()
    if position_side not in ['BOTH', 'LONG', 'SHORT']:
        raise ValueError("position_side must be 'BOTH', 'LONG', or 'SHORT'")

    try:
        # Get current position
        positions = binance_client.futures_position_information(symbol=symbol)

        target_position = None
        for position in positions:
            if position['positionSide'] == position_side:
                pos_amt = Decimal(position['positionAmt'])
                if pos_amt != 0:
                    target_position = position
                    break

        if not target_position:
            raise ValueError(f"No open {position_side} position found for {symbol}")

        pos_amt = Decimal(target_position['positionAmt'])
        quantity = abs(pos_amt)

        # Determine close side
        if pos_amt > 0:
            close_side = 'SELL'
            direction = 'LONG'
        else:
            close_side = 'BUY'
            direction = 'SHORT'

        # Store pre-close data
        pre_close_pnl = Decimal(target_position['unRealizedProfit'])
        pre_close_mark_price = Decimal(target_position['markPrice'])

        # Execute market order to close
        logger.warning(f"⚠️  CLOSING {direction} POSITION: {symbol} {quantity} contracts via {close_side}")
        order = binance_client.futures_create_order(
            symbol=symbol,
            side=close_side,
            type='MARKET',
            quantity=float(quantity),
            positionSide=position_side,
            reduceOnly=True
        )

        # Get post-close position (should be zero)
        positions_after = binance_client.futures_position_information(symbol=symbol)
        post_position = None
        for pos in positions_after:
            if pos['positionSide'] == position_side:
                post_position = pos
                break

        record = {
            'symbol': order['symbol'],
            'direction': direction,
            'positionSide': order['positionSide'],
            'orderId': order['orderId'],
            'closeSide': order['side'],
            'executedQty': float(order['executedQty']),
            'avgPrice': float(order.get('avgPrice', 0)),
            'status': order['status'],
            'realizedPnl': float(pre_close_pnl),  # Approximate realized P&L
            'closePrice': float(pre_close_mark_price),
            'remainingPosition': float(post_position['positionAmt']) if post_position else 0.0,
            'timestamp': datetime.fromtimestamp(order['updateTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        }

        df = pd.DataFrame([record])
        logger.info(f"Position closed successfully: {symbol} {direction}")

        return df

    except Exception as e:
        logger.error(f"Error closing position: {e}")
        raise


def register_binance_manage_futures_positions(local_mcp_instance, local_binance_client, csv_dir):
    """Register the binance_manage_futures_positions tool"""
    @local_mcp_instance.tool()
    def binance_manage_futures_positions(symbol: Optional[str] = None,
                                         close_position: bool = False,
                                         position_side: str = 'BOTH') -> str:
        """
        View and manage futures positions - list open positions or close specific position.

        This tool allows you to monitor all open leveraged positions with detailed metrics
        including P&L, liquidation risk, and ROI, or close positions when needed.

        ⚠️  FUTURES POSITION WARNING ⚠️
        Futures positions involve leverage and liquidation risk. Monitor positions regularly
        and be aware of liquidation prices, especially in volatile markets.

        Parameters:
            symbol (string, optional): Trading pair symbol (e.g., 'BTCUSDT')
                - If provided: Shows/closes only this symbol's position
                - If omitted: Shows all open positions (when close_position=False)
            close_position (boolean, optional): If True, closes the position (default: False)
            position_side (string, optional): Position side - 'BOTH', 'LONG', or 'SHORT' (default: 'BOTH')
                - Used when closing positions in hedge mode
                - 'BOTH': One-way mode (most common)

        Returns:
            str: Formatted response with CSV file(s) containing position details or closure confirmation.

        CSV Output Columns (list positions):
            - symbol (string): Trading pair (e.g., 'BTCUSDT')
            - direction (string): LONG or SHORT
            - positionSide (string): BOTH, LONG, or SHORT
            - positionAmt (float): Position size (positive=LONG, negative=SHORT)
            - entryPrice (float): Average entry price
            - markPrice (float): Current mark price
            - priceChangePct (float): Price change % since entry
            - liquidationPrice (float): Liquidation price (None if no risk)
            - liqDistancePct (float): Distance to liquidation in %
            - riskLevel (string): Risk assessment (LOW, MEDIUM, HIGH, CRITICAL)
            - unRealizedProfit (float): Unrealized P&L in USDT
            - roiPct (float): Return on investment %
            - leverage (integer): Leverage multiplier
            - notionalValue (float): Position notional value
            - margin (float): Margin used for position
            - initialMargin (float): Initial margin required
            - maintMargin (float): Maintenance margin required
            - isolated (boolean): True if isolated margin mode
            - updateTime (string): Last update timestamp

        CSV Output Columns (close position):
            - symbol (string): Trading pair
            - direction (string): LONG or SHORT
            - positionSide (string): Position side closed
            - orderId (integer): Close order ID
            - closeSide (string): Order side used to close (BUY or SELL)
            - executedQty (float): Quantity closed
            - avgPrice (float): Average close price
            - status (string): Order status (typically FILLED)
            - realizedPnl (float): Realized P&L from closure
            - closePrice (float): Close execution price
            - remainingPosition (float): Remaining position size (should be 0)
            - timestamp (string): Closure timestamp

        Risk Levels:
            - LOW: >20% from liquidation - Safer position with good margin buffer
            - MEDIUM: 10-20% from liquidation - Monitor regularly
            - HIGH: 5-10% from liquidation - Caution advised, consider reducing size
            - CRITICAL: <5% from liquidation - IMMEDIATE action needed!

        Use Cases:
            - Monitor all open leveraged positions
            - Check specific position details and risk
            - Track unrealized P&L across portfolio
            - Assess liquidation risk before market volatility
            - Close positions at market price
            - Emergency position closure
            - Portfolio rebalancing

        Position Management:
            - Regular monitoring recommended, especially with high leverage
            - Check liquidation distances during volatile markets
            - Consider closing or reducing size if risk level is HIGH or CRITICAL
            - Use stop-loss orders to protect positions
            - Monitor margin ratios and add margin if needed

        Example usage:
            # List all open positions
            binance_manage_futures_positions()

            # View specific position
            binance_manage_futures_positions(symbol="BTCUSDT")

            # Close position at market price
            binance_manage_futures_positions(symbol="BTCUSDT", close_position=True)

            # Close specific position side in hedge mode
            binance_manage_futures_positions(symbol="ETHUSDT", close_position=True, position_side="LONG")

        Position Analysis Tips:
            - Use py_eval to analyze positions CSV
            - Sort by riskLevel to identify dangerous positions
            - Calculate total portfolio P&L
            - Identify positions with best/worst ROI
            - Monitor correlation between positions

        When to Close a Position:
            - Target profit reached
            - Stop-loss triggered
            - Risk level becomes too high (HIGH or CRITICAL)
            - Market conditions change
            - Need to free up margin
            - Reducing overall leverage

        CRITICAL Notes:
            - Closing executes MARKET order - immediate execution at current price
            - Market orders may have slippage in volatile conditions
            - Once closed, position cannot be reopened at same price
            - Realized P&L may differ slightly from unrealized P&L
            - In hedge mode, specify correct position_side
            - This operation is irreversible

        Common Errors:
            - "No open position found": Position already closed or symbol incorrect
            - Check symbol spelling and ensure position exists
            - For hedge mode, verify correct position_side

        Note:
            - List operation is READ-ONLY and safe to run frequently
            - Close operation executes REAL TRADES and is irreversible
            - Monitor positions regularly to avoid liquidation
            - CSV saved for position tracking and analysis
            - Use with binance_calculate_liquidation_risk for detailed risk analysis
        """
        logger.info(f"binance_manage_futures_positions tool invoked")

        try:
            if close_position:
                # Close position mode
                if not symbol:
                    return "Error: symbol is required when close_position=True"

                df = close_futures_position(
                    binance_client=local_binance_client,
                    symbol=symbol,
                    position_side=position_side
                )

                filename = f"close_position_{symbol}_{position_side.lower()}_{str(uuid.uuid4())[:8]}.csv"
                filepath = csv_dir / filename
                df.to_csv(filepath, index=False)
                logger.info(f"Saved position closure to {filename}")

                result = format_csv_response(filepath, df)

                close_data = df.iloc[0]
                summary = f"""

═══════════════════════════════════════════════════════════════════════════════
POSITION CLOSED
═══════════════════════════════════════════════════════════════════════════════
Symbol:              {close_data['symbol']}
Direction:           {close_data['direction']}
Position Side:       {close_data['positionSide']}
Order ID:            {close_data['orderId']}
Close Side:          {close_data['closeSide']}
Executed Qty:        {close_data['executedQty']}
Average Price:       {close_data['avgPrice']:.8f}
Status:              {close_data['status']}
Realized P&L:        {close_data['realizedPnl']:+.2f} USDT
Remaining Position:  {close_data['remainingPosition']}
Time:                {close_data['timestamp']}
═══════════════════════════════════════════════════════════════════════════════

Position successfully closed at market price.

Verify closure:
  binance_manage_futures_positions(symbol="{close_data['symbol']}")

Check account balance:
  binance_get_futures_balances()
═══════════════════════════════════════════════════════════════════════════════
"""

                return result + summary

            else:
                # List positions mode
                df = list_futures_positions(
                    binance_client=local_binance_client,
                    symbol=symbol
                )

                if symbol:
                    filename = f"positions_{symbol}_{str(uuid.uuid4())[:8]}.csv"
                else:
                    filename = f"positions_all_{str(uuid.uuid4())[:8]}.csv"

                filepath = csv_dir / filename
                df.to_csv(filepath, index=False)
                logger.info(f"Saved positions to {filename}")

                result = format_csv_response(filepath, df)

                if df.empty:
                    summary = f"""

═══════════════════════════════════════════════════════════════════════════════
NO OPEN POSITIONS
═══════════════════════════════════════════════════════════════════════════════
"""
                    if symbol:
                        summary += f"No open positions found for {symbol}.\n"
                    else:
                        summary += "No open positions found.\n"
                    summary += "═══════════════════════════════════════════════════════════════════════════════\n"
                    return result + summary

                # Calculate summary statistics
                total_positions = len(df)
                total_unrealized_pnl = df['unRealizedProfit'].sum()
                total_margin = df['margin'].sum()
                avg_leverage = df['leverage'].mean()

                risk_counts = df['riskLevel'].value_counts().to_dict()
                critical_count = risk_counts.get('CRITICAL', 0)
                high_count = risk_counts.get('HIGH', 0)

                summary = f"""

═══════════════════════════════════════════════════════════════════════════════
FUTURES POSITIONS SUMMARY
═══════════════════════════════════════════════════════════════════════════════
Total Positions:     {total_positions}
Total Unrealized P&L: {total_unrealized_pnl:+,.2f} USDT
Total Margin Used:   {total_margin:,.2f} USDT
Avg Leverage:        {avg_leverage:.1f}x

Risk Breakdown:
"""

                for risk_level in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
                    count = risk_counts.get(risk_level, 0)
                    if count > 0:
                        emoji = {'CRITICAL': '🚨', 'HIGH': '⚠️', 'MEDIUM': '⚡', 'LOW': '✓'}.get(risk_level, '')
                        summary += f"  {emoji} {risk_level}: {count}\n"

                summary += "\n"

                if critical_count > 0:
                    summary += f"🚨 ALERT: {critical_count} position(s) at CRITICAL risk of liquidation!\n"
                    summary += "   Take immediate action - add margin or close positions.\n"
                elif high_count > 0:
                    summary += f"⚠️  WARNING: {high_count} position(s) at HIGH risk.\n"
                    summary += "   Monitor closely and consider risk reduction.\n"

                summary += "\n═══════════════════════════════════════════════════════════════════════════════\n"

                return result + summary

        except ValueError as e:
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Error managing positions: {e}")
            return f"Error: {str(e)}\n\nCheck:\n- API credentials valid\n- Futures trading enabled\n- Symbol correct\n- Position exists\n- Network connectivity"
