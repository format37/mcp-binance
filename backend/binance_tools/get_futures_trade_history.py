import logging
from datetime import datetime
import uuid
from mcp_service import format_csv_response
import pandas as pd
from binance.client import Client
from typing import Optional
from sentry_utils import with_sentry_tracing

logger = logging.getLogger(__name__)


@with_sentry_tracing("binance_get_futures_trade_history")
def fetch_futures_trade_history(binance_client: Client, symbol: str, limit: int = 100,
                                from_id: Optional[int] = None) -> pd.DataFrame:
    """
    Fetch futures trade history and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        limit: Number of trades to fetch (max 1000, default 100)
        from_id: Trade ID to fetch from (optional, for pagination)

    Returns:
        DataFrame with trade execution history

    Note:
        Returns historical completed trades (executions) for the symbol.
    """
    logger.info(f"Fetching futures trade history for {symbol}")

    try:
        # Build parameters
        params = {
            'symbol': symbol,
            'limit': min(limit, 1000)  # Cap at API maximum
        }

        if from_id:
            params['fromId'] = from_id

        # Fetch trade history
        trades = binance_client.futures_account_trades(**params)

        records = []
        for trade in trades:
            records.append({
                'id': trade['id'],
                'orderId': trade['orderId'],
                'symbol': trade['symbol'],
                'side': trade['side'],
                'positionSide': trade['positionSide'],
                'price': float(trade['price']),
                'qty': float(trade['qty']),
                'quoteQty': float(trade['quoteQty']),
                'realizedPnl': float(trade['realizedPnl']),
                'commission': float(trade['commission']),
                'commissionAsset': trade['commissionAsset'],
                'buyer': trade['buyer'],
                'maker': trade['maker'],
                'time': datetime.fromtimestamp(trade['time'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
            })

        df = pd.DataFrame(records) if records else pd.DataFrame(columns=[
            'id', 'orderId', 'symbol', 'side', 'positionSide', 'price', 'qty', 'quoteQty',
            'realizedPnl', 'commission', 'commissionAsset', 'buyer', 'maker', 'time'
        ])

        logger.info(f"Retrieved {len(records)} futures trades for {symbol}")

        return df

    except Exception as e:
        logger.error(f"Error fetching futures trade history: {e}")
        raise


def register_binance_get_futures_trade_history(local_mcp_instance, local_binance_client, csv_dir):
    """Register the binance_get_futures_trade_history tool"""
    @local_mcp_instance.tool()
    def binance_get_futures_trade_history(symbol: str, limit: int = 100, from_id: Optional[int] = None) -> str:
        """
        Fetch historical futures trades (executions) for a symbol and save to CSV for analysis.

        This tool retrieves completed trade executions for a futures trading pair, including
        execution prices, quantities, fees, and realized P&L. Use it to review trading history,
        calculate performance metrics, and analyze trading patterns.

        ✓ READ-ONLY OPERATION - Safe to run anytime

        Parameters:
            symbol (string, required): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
            limit (integer, optional): Number of trades to retrieve (default: 100, max: 1000)
            from_id (integer, optional): Trade ID to fetch from (for pagination)

        Returns:
            str: Formatted response with CSV file containing trade execution history.

        CSV Output Columns:
            - id (integer): Unique trade identifier
            - orderId (integer): Order ID that generated this trade
            - symbol (string): Trading pair (e.g., 'BTCUSDT')
            - side (string): Trade side (BUY or SELL)
            - positionSide (string): Position side (BOTH, LONG, or SHORT)
            - price (float): Execution price
            - qty (float): Executed quantity (contracts)
            - quoteQty (float): Quote asset quantity (USDT value)
            - realizedPnl (float): Realized profit/loss from this trade
            - commission (float): Trading fee amount
            - commissionAsset (string): Asset used for fee payment
            - buyer (boolean): True if you were the buyer
            - maker (boolean): True if you were maker (false = taker)
            - time (string): Execution timestamp

        Trade Types:
            - Maker trades: You provided liquidity (limit order filled), typically lower fees
            - Taker trades: You took liquidity (market order or aggressive limit), higher fees

        Realized P&L:
            - Shows profit/loss realized when closing/reducing positions
            - Positive = profit, Negative = loss
            - Only appears on trades that close positions
            - Opening trades show 0 realized P&L

        Commission:
            - Maker fee: Lower rate for providing liquidity (~0.02%)
            - Taker fee: Higher rate for taking liquidity (~0.04%)
            - Fees typically paid in USDT or BNB
            - Can be reduced with BNB fee payment and VIP levels

        Use Cases:
            - Review trading performance over time
            - Calculate total trading fees paid
            - Analyze realized P&L by trade
            - Verify order executions
            - Identify profitable vs unprofitable trades
            - Calculate average entry/exit prices
            - Tax reporting and record keeping
            - Trading strategy analysis

        Pagination:
            - Use limit parameter to control how many trades to fetch
            - Maximum 1000 trades per request
            - Use from_id to fetch older trades
            - from_id is the trade ID to start from (exclusive)

        Example usage:
            # Get last 100 trades for BTCUSDT
            binance_get_futures_trade_history(symbol="BTCUSDT")

            # Get last 500 trades
            binance_get_futures_trade_history(symbol="BTCUSDT", limit=500)

            # Paginate - get next 100 trades after trade ID 123456
            binance_get_futures_trade_history(symbol="BTCUSDT", from_id=123456)

        Analysis with py_eval:
            - Calculate total realized P&L: df['realizedPnl'].sum()
            - Calculate total fees paid: df['commission'].sum()
            - Calculate maker vs taker ratio: df['maker'].value_counts()
            - Find average execution price: df['price'].mean()
            - Analyze profitable vs losing trades
            - Calculate total trading volume
            - Group by date for daily performance
            - Calculate win rate on closing trades

        Performance Metrics:
            - Total Realized P&L: Sum of all realizedPnl
            - Net Profit: Total P&L minus total commission
            - Average Trade Size: Mean of qty column
            - Maker/Taker Ratio: Percentage of maker trades
            - Trading Volume: Sum of quoteQty

        Example Analysis:
            ```python
            df = pd.read_csv('data/mcp-binance/futures_trades_BTCUSDT_abc123.csv')

            # Total realized P&L
            total_pnl = df['realizedPnl'].sum()

            # Total fees paid
            total_fees = df['commission'].sum()

            # Net profit after fees
            net_profit = total_pnl - total_fees

            # Maker vs taker ratio
            maker_pct = (df['maker'].sum() / len(df)) * 100

            # Average execution price
            avg_price = df['price'].mean()
            ```

        Trade History vs Open Orders:
            - Trade history: Completed executions (filled orders)
            - Open orders: Pending orders not yet filled
            - Use binance_get_futures_open_orders for pending orders
            - Use this tool for historical completed trades

        Important Notes:
            - Only shows YOUR trades (not market trades)
            - Trades are sorted by most recent first (descending time)
            - One order can generate multiple trades (partial fills)
            - realizedPnl only shows when closing positions
            - Limit is capped at 1000 per request
            - Use pagination for retrieving older trades

        Related Tools:
            - View open orders: binance_get_futures_open_orders(symbol)
            - Check positions: binance_manage_futures_positions(symbol)
            - Account summary: binance_get_futures_balances()
            - Spot trades: binance_spot_trade_history(symbol)

        Tax and Reporting:
            - CSV provides complete trade record for tax reporting
            - Includes all necessary data: prices, quantities, fees, P&L
            - Timestamps in readable format for audit trails
            - Keep records for tax compliance

        Note:
            - Completely safe READ-ONLY operation
            - Run as frequently as needed
            - No API rate limit concerns for reasonable usage
            - CSV file saved for record keeping and analysis
            - Different from spot trade history (uses futures API)
        """
        logger.info(f"binance_get_futures_trade_history tool invoked for {symbol}")

        # Validate parameters
        if not symbol:
            return "Error: symbol is required (e.g., 'BTCUSDT')"

        if limit <= 0 or limit > 1000:
            return "Error: limit must be between 1 and 1000"

        try:
            # Fetch trade history
            df = fetch_futures_trade_history(
                binance_client=local_binance_client,
                symbol=symbol,
                limit=limit,
                from_id=from_id
            )

            # Generate filename
            filename = f"futures_trades_{symbol}_{str(uuid.uuid4())[:8]}.csv"
            filepath = csv_dir / filename

            # Save to CSV
            df.to_csv(filepath, index=False)
            logger.info(f"Saved futures trade history to {filename}")

            # Return formatted response
            result = format_csv_response(filepath, df)

            if df.empty:
                summary = f"""

═══════════════════════════════════════════════════════════════════════════════
NO TRADE HISTORY
═══════════════════════════════════════════════════════════════════════════════
No trades found for {symbol}.

This could mean:
- No trades have been executed for this symbol yet
- Using from_id beyond available history
- Symbol has no trade history in your account

═══════════════════════════════════════════════════════════════════════════════
"""
                return result + summary

            # Calculate summary statistics
            total_trades = len(df)
            total_pnl = df['realizedPnl'].sum()
            total_commission = df['commission'].sum()
            net_pnl = total_pnl - total_commission
            total_volume = df['quoteQty'].sum()
            maker_trades = df['maker'].sum()
            maker_pct = (maker_trades / total_trades) * 100 if total_trades > 0 else 0

            summary = f"""

═══════════════════════════════════════════════════════════════════════════════
FUTURES TRADE HISTORY SUMMARY
═══════════════════════════════════════════════════════════════════════════════
Symbol:              {symbol}
Total Trades:        {total_trades}
Total Volume:        {total_volume:,.2f} USDT

Financial Summary:
Realized P&L:        {total_pnl:+,.2f} USDT
Total Fees:          {total_commission:,.2f} USDT
Net P&L (after fees): {net_pnl:+,.2f} USDT

Trading Style:
Maker Trades:        {int(maker_trades)} ({maker_pct:.1f}%)
Taker Trades:        {total_trades - int(maker_trades)} ({100-maker_pct:.1f}%)

Date Range:
First Trade:         {df.iloc[-1]['time']}
Latest Trade:        {df.iloc[0]['time']}
═══════════════════════════════════════════════════════════════════════════════

Use py_eval to perform detailed analysis:
- Calculate daily/weekly P&L trends
- Analyze profitable vs unprofitable trades
- Calculate average trade size and execution quality
- Identify best performing trading sessions

To get more trades:
  binance_get_futures_trade_history(symbol="{symbol}", limit=500)

To get older trades (pagination):
  binance_get_futures_trade_history(symbol="{symbol}", from_id={df.iloc[-1]['id']})

═══════════════════════════════════════════════════════════════════════════════
"""

            return result + summary

        except Exception as e:
            logger.error(f"Error fetching futures trade history: {e}")
            return f"Error: {str(e)}\n\nCheck:\n- API credentials valid\n- Futures trading enabled\n- Symbol is correct\n- Network connectivity"
