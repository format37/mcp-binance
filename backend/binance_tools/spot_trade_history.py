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


@with_sentry_tracing("binance_spot_trade_history")
def fetch_trade_history(binance_client: Client, symbol: str, start_time: Optional[int] = None,
                        end_time: Optional[int] = None, limit: int = 500) -> pd.DataFrame:
    """
    Fetch trade history from Binance and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        start_time: Timestamp in milliseconds (optional) - trades after this time
        end_time: Timestamp in milliseconds (optional) - trades before this time
        limit: Number of trades to return (default: 500, max: 1000)

    Returns:
        DataFrame with trade history containing columns:
        - id: Trade ID
        - orderId: Order ID that this trade belongs to
        - symbol: Trading pair symbol
        - price: Trade execution price
        - qty: Quantity traded
        - quoteQty: Quote asset quantity (price * qty)
        - commission: Commission paid for this trade
        - commissionAsset: Asset in which commission was paid
        - time: Trade execution timestamp
        - isBuyer: True if this trade was a BUY, False if SELL
        - isMaker: True if trade was maker, False if taker
        - isBestMatch: True if trade was best price match

    Note:
        Returns your own trades only (not market trades). Useful for P&L analysis.
    """
    logger.info(f"Fetching trade history for {symbol} (limit: {limit})")

    try:
        # Prepare parameters
        params = {
            'symbol': symbol,
            'limit': min(limit, 1000)  # Binance API max is 1000
        }

        if start_time:
            params['startTime'] = start_time
            logger.info(f"Start time filter: {datetime.fromtimestamp(start_time / 1000).strftime('%Y-%m-%d %H:%M:%S')}")

        if end_time:
            params['endTime'] = end_time
            logger.info(f"End time filter: {datetime.fromtimestamp(end_time / 1000).strftime('%Y-%m-%d %H:%M:%S')}")

        # Fetch trades from Binance API
        trades = binance_client.get_my_trades(**params)
        logger.info(f"Fetched {len(trades)} trades for {symbol}")

        if not trades:
            logger.info("No trades found")
            # Return empty DataFrame with expected columns
            return pd.DataFrame(columns=[
                'id', 'orderId', 'symbol', 'price', 'qty', 'quoteQty',
                'commission', 'commissionAsset', 'time', 'isBuyer', 'isMaker', 'isBestMatch'
            ])

        # Process trades into records
        records = []
        for trade in trades:
            records.append({
                'id': trade['id'],
                'orderId': trade['orderId'],
                'symbol': trade['symbol'],
                'price': float(trade['price']),
                'qty': float(trade['qty']),
                'quoteQty': float(trade['quoteQty']),
                'commission': float(trade['commission']),
                'commissionAsset': trade['commissionAsset'],
                'time': datetime.fromtimestamp(trade['time'] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                'isBuyer': trade['isBuyer'],
                'isMaker': trade['isMaker'],
                'isBestMatch': trade['isBestMatch']
            })

        # Create DataFrame
        df = pd.DataFrame(records)

        # Sort by time (most recent first)
        df = df.sort_values('time', ascending=False).reset_index(drop=True)

        logger.info(f"Successfully processed {len(df)} trades")

        return df

    except Exception as e:
        logger.error(f"Error fetching trade history from Binance API: {e}")
        raise


def register_binance_spot_trade_history(local_mcp_instance, local_binance_client, csv_dir, requests_dir):
    """Register the binance_spot_trade_history tool"""
    @local_mcp_instance.tool()
    def binance_spot_trade_history(requester: str, symbol: str, start_time: int = None, end_time: int = None, limit: int = 500) -> str:
        """
        Fetch historical trade data for profit/loss analysis and save to CSV file.

        This tool retrieves your executed trades (not market trades) for a specific trading pair.
        Results include trade prices, quantities, commissions, and timestamps. Data is saved to
        CSV for detailed P&L analysis using the py_eval tool.

        Parameters:
            requester (str): Identifier of who is calling this tool (e.g., 'trading-agent', 'user-alex').
                Used for request logging and audit purposes.
            symbol (string, required): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
            start_time (integer, optional): Start timestamp in milliseconds. If provided, only trades
                after this time are returned. Example: 1640995200000 (Jan 1, 2022 00:00:00 UTC)
            end_time (integer, optional): End timestamp in milliseconds. If provided, only trades
                before this time are returned. Example: 1672531199000 (Dec 31, 2022 23:59:59 UTC)
            limit (integer, optional): Number of trades to return (default: 500, max: 1000)

        Returns:
            str: Formatted response with CSV file info, schema, sample data, and Python snippet to load the file.

        CSV Output Columns:
            - id (integer): Unique trade identifier
            - orderId (integer): Order ID that this trade belongs to
            - symbol (string): Trading pair symbol (e.g., 'BTCUSDT')
            - price (float): Trade execution price
            - qty (float): Quantity of base asset traded
            - quoteQty (float): Quantity of quote asset (price * qty)
            - commission (float): Commission/fee paid for this trade
            - commissionAsset (string): Asset in which commission was paid (e.g., 'BNB', 'USDT')
            - time (string): Trade execution timestamp (YYYY-MM-DD HH:MM:SS)
            - isBuyer (boolean): True if this was a BUY trade, False if SELL
            - isMaker (boolean): True if you were the maker (provided liquidity), False if taker
            - isBestMatch (boolean): True if trade was best price match

        Use Cases:
            - Calculate realized profit/loss from completed buy-sell cycles
            - Analyze trading performance over time periods
            - Track total commissions paid
            - Calculate average buy/sell prices
            - Identify winning vs losing trades
            - Tax reporting and record keeping
            - Evaluate trading strategy effectiveness
            - Monitor maker vs taker ratio

        Always use the py_eval tool to analyze the saved CSV file for insights such as:
            - Total P&L: Group by orderId, calculate (sell_price - buy_price) * quantity
            - Total commissions: Sum all commission values converted to USDT
            - Win rate: Percentage of profitable trades
            - Average trade size and value
            - Trading frequency over time
            - Commission savings from maker trades

        Example usage:
            # Get last 500 trades for BTCUSDT
            binance_spot_trade_history(symbol="BTCUSDT")

            # Get trades from specific date range
            binance_spot_trade_history(symbol="ETHUSDT", start_time=1640995200000, limit=1000)

            # Get recent trades with custom limit
            binance_spot_trade_history(symbol="BNBUSDT", limit=100)

        Timestamp Conversion (Python):
            ```python
            from datetime import datetime
            # Convert date to milliseconds timestamp
            start = int(datetime(2024, 1, 1).timestamp() * 1000)
            end = int(datetime(2024, 12, 31).timestamp() * 1000)
            ```

        Note:
            - This is a READ-ONLY operation
            - Returns YOUR executed trades only (not public market trades)
            - Maximum 1000 trades per request (Binance API limit)
            - For full history, make multiple requests with different time ranges
            - Results are sorted by time (most recent first)
            - Useful for tax reporting - consult with tax professional for official records
        """
        logger.info(f"binance_spot_trade_history tool invoked by {requester} for {symbol}")

        # Validate limit
        if limit < 1 or limit > 1000:
            return f"Error: limit must be between 1 and 1000. Provided: {limit}"

        # Call fetch_trade_history function
        df = fetch_trade_history(
            binance_client=local_binance_client,
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )

        if df.empty:
            time_range = ""
            if start_time:
                time_range += f" after {datetime.fromtimestamp(start_time / 1000).strftime('%Y-%m-%d')}"
            if end_time:
                time_range += f" before {datetime.fromtimestamp(end_time / 1000).strftime('%Y-%m-%d')}"
            return f"No trades found for {symbol}{time_range}."

        # Generate filename with unique identifier
        filename = f"trade_history_{symbol}_{str(uuid.uuid4())[:8]}.csv"
        filepath = csv_dir / filename

        # Save to CSV file
        df.to_csv(filepath, index=False)
        logger.info(f"Saved trade history to {filename} ({len(df)} trades)")

        # Return formatted response
        result = format_csv_response(filepath, df)

        # Log the request for audit trail
        log_request(
            requests_dir=requests_dir,
            requester=requester,
            tool_name="binance_spot_trade_history",
            input_params={"symbol": symbol, "start_time": start_time, "end_time": end_time, "limit": limit},
            output_result=result
        )

        return result
