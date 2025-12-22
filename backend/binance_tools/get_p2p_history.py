import logging
from datetime import datetime
from decimal import Decimal
import uuid
from mcp_service import format_csv_response
from request_logger import log_request
import pandas as pd
from binance.client import Client
from typing import Optional
from sentry_utils import with_sentry_tracing

logger = logging.getLogger(__name__)


@with_sentry_tracing("binance_get_p2p_history")
def fetch_p2p_history(
    binance_client: Client,
    trade_type: str,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    page: Optional[int] = 1,
    rows: Optional[int] = None
) -> pd.DataFrame:
    """
    Fetch Binance P2P (C2C) trading history and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        trade_type: "BUY" or "SELL" (required)
        start_time: Start time in milliseconds (cannot exceed 30 days from end_time)
        end_time: End time in milliseconds
        page: Page number for pagination (default: 1)
        rows: Number of records per page (default: 100, max: 100)

    Returns:
        DataFrame with P2P trading history containing columns:
        - orderNumber: Order ID
        - advNo: Advertisement number
        - tradeType: SELL or BUY
        - asset: Cryptocurrency symbol
        - fiat: Fiat currency code
        - fiatSymbol: Fiat currency symbol
        - amount: Crypto amount traded
        - totalPrice: Total fiat amount
        - unitPrice: Unit price (fiat per crypto)
        - orderStatus: Order status
        - createTime: Order creation timestamp (milliseconds)
        - createTime_readable: Human-readable timestamp
        - commission: Transaction fee
        - counterPartNickName: Other party's nickname (masked)
        - advertisementRole: TAKER or MAKER

    Note:
        - Time span between start_time and end_time cannot exceed 30 days
        - Only returns last 6 months of data
        - Historical data available from June 10, 2020 onwards
        - Defaults to last 7 days if no time parameters provided
    """
    logger.info(f"Fetching P2P history - trade_type: {trade_type}, page: {page}, rows: {rows}")

    # Validate trade_type
    if trade_type not in ['BUY', 'SELL']:
        raise ValueError("trade_type must be 'BUY' or 'SELL'")

    # Build parameters dict
    params = {'tradeType': trade_type}
    if start_time:
        params['startTime'] = start_time
    if end_time:
        params['endTime'] = end_time
    if page:
        params['page'] = page
    if rows:
        params['rows'] = rows

    try:
        # Fetch P2P/C2C history from Binance API
        response = binance_client.get_c2c_trade_history(**params)

        # Log response structure for debugging
        logger.info(f"P2P API response code: {response.get('code')}, message: {response.get('message')}")

        # Check if request was successful
        if response.get('code') != '000000' or not response.get('success'):
            error_msg = response.get('message', 'Unknown error')
            logger.error(f"P2P API error: {error_msg}")
            raise Exception(f"Binance P2P API error: {error_msg}")

        # Extract data array
        trades = response.get('data', [])
        total_records = response.get('total', 0)

        logger.info(f"Received {len(trades)} P2P trade records (total available: {total_records})")

        # Process each P2P trade record
        records = []
        for trade in trades:
            # Convert createTime to readable format
            create_time = trade.get('createTime')
            create_readable = datetime.fromtimestamp(create_time / 1000).strftime('%Y-%m-%d %H:%M:%S') if create_time else None

            records.append({
                'orderNumber': trade.get('orderNumber'),
                'advNo': trade.get('advNo'),
                'tradeType': trade.get('tradeType'),
                'asset': trade.get('asset'),
                'fiat': trade.get('fiat'),
                'fiatSymbol': trade.get('fiatSymbol', ''),
                'amount': float(trade.get('amount', 0)),
                'totalPrice': float(trade.get('totalPrice', 0)),
                'unitPrice': float(trade.get('unitPrice', 0)),
                'orderStatus': trade.get('orderStatus'),
                'createTime': create_time,
                'createTime_readable': create_readable,
                'commission': float(trade.get('commission', 0)),
                'counterPartNickName': trade.get('counterPartNickName', ''),
                'advertisementRole': trade.get('advertisementRole', '')
            })

        # Create DataFrame
        df = pd.DataFrame(records)

        # Sort by createTime descending (newest first)
        if not df.empty and 'createTime' in df.columns:
            df = df.sort_values('createTime', ascending=False).reset_index(drop=True)

        logger.info(f"Successfully processed {len(df)} P2P trade records")

        # Add metadata about pagination
        if not df.empty:
            logger.info(f"Page {page} of P2P trades. Total available: {total_records}")

        return df

    except Exception as e:
        logger.error(f"Error fetching P2P history from Binance API: {e}")
        raise


def register_binance_get_p2p_history(local_mcp_instance, local_binance_client, csv_dir, requests_dir):
    """Register the binance_get_p2p_history tool"""
    @local_mcp_instance.tool()
    def binance_get_p2p_history(
        requester: str,
        trade_type: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        page: Optional[int] = 1,
        rows: Optional[int] = 100
    ) -> str:
        """
        Fetch Binance P2P (Peer-to-Peer) trading history and save to CSV file for analysis.

        This tool retrieves your P2P/C2C (Customer-to-Customer) trading history where you
        bought or sold cryptocurrency directly with other users using fiat currency.
        Results are saved to CSV for detailed analysis of P2P trading activity.

        Parameters:
            requester (str): Identifier of who is calling this tool (e.g., 'trading-agent', 'user-alex').
                Used for request logging and audit purposes.

            trade_type (str, REQUIRED): Type of P2P trade to retrieve:
                                       - "BUY": When you bought crypto with fiat
                                       - "SELL": When you sold crypto for fiat
                                       This parameter is REQUIRED by the Binance API.

            start_time (int, optional): Start time in milliseconds (Unix timestamp).
                                        Cannot exceed 30 days from end_time.
                                        Example: 1609459200000 (Jan 1, 2021 00:00:00 UTC)

            end_time (int, optional): End time in milliseconds (Unix timestamp).
                                      Example: 1612137600000 (Feb 1, 2021 00:00:00 UTC)

            page (int, optional): Page number for pagination (default: 1).
                                  Use this to retrieve subsequent pages of results.

            rows (int, optional): Number of records per page.
                                  Default: 100, Maximum: 100

        Returns:
            str: Formatted response with CSV file info, schema, sample data, and Python snippet.

        CSV Output Columns:
            - orderNumber (string): P2P order ID
            - advNo (string): Advertisement number
            - tradeType (string): "BUY" or "SELL"
            - asset (string): Cryptocurrency symbol (e.g., 'USDT', 'BTC', 'BUSD')
            - fiat (string): Fiat currency code (e.g., 'USD', 'EUR', 'CNY')
            - fiatSymbol (string): Fiat currency symbol (e.g., '$', '€', '¥')
            - amount (float): Cryptocurrency amount traded
            - totalPrice (float): Total fiat amount paid/received
            - unitPrice (float): Price per unit of cryptocurrency
            - orderStatus (string): Order status - PENDING/TRADING/BUYER_PAID/DISTRIBUTING/
                                   COMPLETED/IN_APPEAL/CANCELLED/CANCELLED_BY_SYSTEM
            - createTime (int): Order creation timestamp (milliseconds)
            - createTime_readable (string): Human-readable timestamp
            - commission (float): Transaction fee charged
            - counterPartNickName (string): Other party's nickname (masked for privacy)
            - advertisementRole (string): "TAKER" or "MAKER"

        Important Notes:
            - Time span between start_time and end_time cannot exceed 30 days
            - If no time parameters provided, defaults to last 7 days
            - Only returns last 6 months of P2P data
            - Historical data available from June 10, 2020 onwards
            - Maximum 200 records per request (100 rows × 2 pages max before filtering)
            - Request weight: 1 (IP-based rate limit)
            - BUY and SELL trades must be fetched separately (two separate calls)

        Use Cases:
            - Track all P2P trading activity
            - Calculate total fiat deposited via P2P buys
            - Calculate total fiat withdrawn via P2P sells
            - Analyze P2P trading fees and costs
            - Identify cancelled or disputed orders
            - Calculate average P2P prices paid/received
            - Track P2P trading volume by currency pair
            - Audit P2P trading history for tax reporting
            - Compare P2P rates vs exchange rates

        Example usage:

            # Get all P2P BUY trades (last 7 days)
            binance_get_p2p_history(trade_type='BUY')

            # Get all P2P SELL trades (last 7 days)
            binance_get_p2p_history(trade_type='SELL')

            # Get P2P BUY trades within specific date range
            binance_get_p2p_history(
                trade_type='BUY',
                start_time=1609459200000,  # Jan 1, 2021
                end_time=1612137600000      # Feb 1, 2021 (within 30 days)
            )

            # Get second page of results
            binance_get_p2p_history(trade_type='BUY', page=2)

            # Get completed P2P sells only (requires filtering after retrieval)
            binance_get_p2p_history(trade_type='SELL')
            # Then filter in py_eval: df[df['orderStatus'] == 'COMPLETED']

        Analysis Examples (use py_eval tool):

            # Calculate total fiat deposited via P2P
            buys = pd.read_csv('p2p_history_BUY.csv')
            completed_buys = buys[buys['orderStatus'] == 'COMPLETED']
            total_deposited = completed_buys['totalPrice'].sum()

            # Calculate total fiat withdrawn via P2P
            sells = pd.read_csv('p2p_history_SELL.csv')
            completed_sells = sells[sells['orderStatus'] == 'COMPLETED']
            total_withdrawn = completed_sells['totalPrice'].sum()

            # Calculate total P2P fees paid
            total_fees = completed_buys['commission'].sum() + completed_sells['commission'].sum()

            # Calculate average P2P buy price
            avg_buy_price = completed_buys['unitPrice'].mean()

            # Net P2P flow (deposits - withdrawals)
            net_flow = total_deposited - total_withdrawn

            # Find disputed/appealed orders
            disputed = buys[buys['orderStatus'] == 'IN_APPEAL']

        Workflow for Complete P2P Analysis:

            # Step 1: Fetch BUY history
            binance_get_p2p_history(trade_type='BUY', start_time=..., end_time=...)

            # Step 2: Fetch SELL history
            binance_get_p2p_history(trade_type='SELL', start_time=..., end_time=...)

            # Step 3: Combine and analyze using py_eval
            buys = pd.read_csv('p2p_history_BUY_xxxx.csv')
            sells = pd.read_csv('p2p_history_SELL_xxxx.csv')
            all_p2p = pd.concat([buys, sells]).sort_values('createTime', ascending=False)

        Note:
            P2P trading is different from regular spot trading. P2P involves direct
            transactions with other users using fiat currency, while spot trading
            involves trading on the exchange order book with crypto pairs.
        """
        logger.info(f"binance_get_p2p_history tool invoked by {requester} - trade_type: {trade_type}, page: {page}")

        # Validate trade_type
        if trade_type not in ['BUY', 'SELL']:
            return "ERROR: trade_type must be 'BUY' or 'SELL'"

        # Call fetch function
        df = fetch_p2p_history(
            binance_client=local_binance_client,
            trade_type=trade_type,
            start_time=start_time,
            end_time=end_time,
            page=page,
            rows=rows
        )

        if df.empty:
            return f"No P2P {trade_type} history found for the specified parameters."

        # Generate filename with unique identifier
        filename = f"p2p_history_{trade_type}_{str(uuid.uuid4())[:8]}.csv"
        filepath = csv_dir / filename

        # Save to CSV file
        df.to_csv(filepath, index=False)
        logger.info(f"Saved P2P history to {filename} ({len(df)} records)")

        # Return formatted response
        result = format_csv_response(filepath, df)

        # Log the request for audit trail
        log_request(
            requests_dir=requests_dir,
            requester=requester,
            tool_name="binance_get_p2p_history",
            input_params={"trade_type": trade_type, "start_time": start_time, "end_time": end_time, "page": page, "rows": rows},
            output_result=result
        )

        return result
