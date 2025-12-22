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


@with_sentry_tracing("binance_get_deposit_history")
def fetch_deposit_history(
    binance_client: Client,
    coin: Optional[str] = None,
    status: Optional[int] = None,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    limit: Optional[int] = None
) -> pd.DataFrame:
    """
    Fetch Binance crypto deposit history and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        coin: Filter by specific cryptocurrency (e.g., 'BTC', 'USDT', 'ETH')
        status: Filter by status - 0=pending, 1=success, 6=credited but cannot withdraw,
                7=wrong deposit, 8=waiting user confirm, 2=rejected
        start_time: Start time in milliseconds (cannot exceed 90 days from end_time)
        end_time: End time in milliseconds
        limit: Number of records to return (default: 1000, max: 1000)

    Returns:
        DataFrame with deposit history containing columns:
        - id: Deposit record ID
        - amount: Deposit amount
        - coin: Cryptocurrency symbol
        - network: Network used for deposit (e.g., 'BTC', 'ETH', 'BEP20')
        - status: Deposit status code
        - status_text: Human-readable status
        - address: Deposit address
        - addressTag: Address tag/memo (if applicable)
        - txId: Transaction hash on blockchain
        - insertTime: When deposit initiated (milliseconds)
        - insertTime_readable: Human-readable timestamp
        - completeTime: When deposit completed (milliseconds)
        - completeTime_readable: Human-readable timestamp
        - transferType: Transfer type (0 = external deposit)
        - confirmTimes: Network confirmation times (e.g., "1/1")

    Note:
        - Only returns crypto deposits (not fiat)
        - Time span between start_time and end_time cannot exceed 90 days
        - Defaults to last 90 days if no time parameters provided
    """
    logger.info(f"Fetching deposit history - coin: {coin}, status: {status}, limit: {limit}")

    # Build parameters dict
    params = {}
    if coin:
        params['coin'] = coin
    if status is not None:
        params['status'] = status
    if start_time:
        params['startTime'] = start_time
    if end_time:
        params['endTime'] = end_time
    if limit:
        params['limit'] = limit

    try:
        # Fetch deposit history from Binance API
        deposits = binance_client.get_deposit_history(**params)
        logger.info(f"Received {len(deposits)} deposit records")

        # Status code mapping
        status_map = {
            0: "Pending",
            1: "Success",
            2: "Rejected",
            6: "Credited (Cannot Withdraw)",
            7: "Wrong Deposit",
            8: "Waiting User Confirm"
        }

        # Process each deposit record
        records = []
        for deposit in deposits:
            # Convert timestamps to readable format
            insert_time = deposit.get('insertTime')
            complete_time = deposit.get('completeTime')

            insert_readable = datetime.fromtimestamp(insert_time / 1000).strftime('%Y-%m-%d %H:%M:%S') if insert_time else None
            complete_readable = datetime.fromtimestamp(complete_time / 1000).strftime('%Y-%m-%d %H:%M:%S') if complete_time else None

            records.append({
                'id': deposit.get('id'),
                'amount': float(deposit.get('amount', 0)),
                'coin': deposit.get('coin'),
                'network': deposit.get('network'),
                'status': deposit.get('status'),
                'status_text': status_map.get(deposit.get('status'), f"Unknown ({deposit.get('status')})"),
                'address': deposit.get('address'),
                'addressTag': deposit.get('addressTag', ''),
                'txId': deposit.get('txId'),
                'insertTime': insert_time,
                'insertTime_readable': insert_readable,
                'completeTime': complete_time,
                'completeTime_readable': complete_readable,
                'transferType': deposit.get('transferType'),
                'confirmTimes': deposit.get('confirmTimes', ''),
                'unlockConfirm': deposit.get('unlockConfirm', 0),
                'walletType': deposit.get('walletType', 0)
            })

        # Create DataFrame
        df = pd.DataFrame(records)

        # Sort by insertTime descending (newest first)
        if not df.empty and 'insertTime' in df.columns:
            df = df.sort_values('insertTime', ascending=False).reset_index(drop=True)

        logger.info(f"Successfully processed {len(df)} deposit records")

        return df

    except Exception as e:
        logger.error(f"Error fetching deposit history from Binance API: {e}")
        raise


def register_binance_get_deposit_history(local_mcp_instance, local_binance_client, csv_dir, requests_dir):
    """Register the binance_get_deposit_history tool"""
    @local_mcp_instance.tool()
    def binance_get_deposit_history(
        requester: str,
        coin: Optional[str] = None,
        status: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = 1000
    ) -> str:
        """
        Fetch Binance crypto deposit history and save to CSV file for analysis.

        This tool retrieves your cryptocurrency deposit history including pending,
        completed, and failed deposits. Results are saved to CSV for detailed analysis.

        Parameters:
            requester (str): Identifier of who is calling this tool (e.g., 'trading-agent', 'user-alex').
                Used for request logging and audit purposes.

            coin (str, optional): Filter by specific cryptocurrency (e.g., 'BTC', 'USDT', 'ETH', 'BNB').
                                  If not provided, returns deposits for all coins.

            status (int, optional): Filter by deposit status:
                                   - 0: Pending
                                   - 1: Success (credited to account)
                                   - 2: Rejected
                                   - 6: Credited but cannot withdraw
                                   - 7: Wrong deposit
                                   - 8: Waiting user confirm
                                   If not provided, returns all statuses.

            start_time (int, optional): Start time in milliseconds (Unix timestamp).
                                        Cannot exceed 90 days from end_time.
                                        Example: 1609459200000 (Jan 1, 2021 00:00:00 UTC)

            end_time (int, optional): End time in milliseconds (Unix timestamp).
                                      Example: 1640995200000 (Jan 1, 2022 00:00:00 UTC)

            limit (int, optional): Maximum number of records to return.
                                   Default: 1000, Maximum: 1000

        Returns:
            str: Formatted response with CSV file info, schema, sample data, and Python snippet.

        CSV Output Columns:
            - id (string): Deposit record ID
            - amount (float): Deposit amount
            - coin (string): Cryptocurrency symbol
            - network (string): Network used (e.g., 'BTC', 'ETH', 'BEP20', 'TRC20')
            - status (int): Status code
            - status_text (string): Human-readable status
            - address (string): Deposit address
            - addressTag (string): Address tag/memo if applicable
            - txId (string): Transaction hash on blockchain
            - insertTime (int): Timestamp when deposit initiated (milliseconds)
            - insertTime_readable (string): Human-readable timestamp
            - completeTime (int): Timestamp when deposit completed (milliseconds)
            - completeTime_readable (string): Human-readable timestamp
            - transferType (int): Transfer type (0 = external deposit)
            - confirmTimes (string): Network confirmation times (e.g., "1/1")

        Important Notes:
            - This endpoint returns CRYPTO deposits only (not fiat/bank deposits)
            - Time span between start_time and end_time cannot exceed 90 days
            - If no time parameters provided, defaults to last 90 days
            - Request weight: 1 (IP-based rate limit)

        Use Cases:
            - Track all deposit activity
            - Identify pending or failed deposits
            - Calculate total deposits by coin
            - Verify blockchain transaction confirmations
            - Audit deposit history for specific time periods
            - Monitor deposit processing times
            - Identify deposits stuck in pending status

        Example usage:

            # Get all deposits (last 90 days)
            binance_get_deposit_history()

            # Get BTC deposits only
            binance_get_deposit_history(coin='BTC')

            # Get successful deposits only
            binance_get_deposit_history(status=1)

            # Get deposits within specific date range
            binance_get_deposit_history(
                coin='USDT',
                start_time=1609459200000,  # Jan 1, 2021
                end_time=1640995200000,     # Jan 1, 2022
                status=1                     # Success only
            )

            # Get recent deposits (last 100)
            binance_get_deposit_history(limit=100)

        Analysis Examples (use py_eval tool):

            # Calculate total deposits by coin
            df = pd.read_csv('deposits.csv')
            successful = df[df['status'] == 1]
            totals = successful.groupby('coin')['amount'].sum().sort_values(ascending=False)

            # Find pending deposits
            pending = df[df['status'] == 0]

            # Calculate average deposit processing time
            df['processing_time_seconds'] = (df['completeTime'] - df['insertTime']) / 1000
            avg_time = df[df['status'] == 1]['processing_time_seconds'].mean()

        Note:
            For fiat deposits (EUR, USD, etc.), use a separate fiat deposit history endpoint.
        """
        logger.info(f"binance_get_deposit_history tool invoked by {requester} - coin: {coin}, status: {status}")

        # Call fetch function
        df = fetch_deposit_history(
            binance_client=local_binance_client,
            coin=coin,
            status=status,
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )

        if df.empty:
            return f"No deposit history found for the specified parameters (coin: {coin}, status: {status})."

        # Generate filename with unique identifier
        coin_suffix = f"_{coin}" if coin else ""
        filename = f"deposit_history{coin_suffix}_{str(uuid.uuid4())[:8]}.csv"
        filepath = csv_dir / filename

        # Save to CSV file
        df.to_csv(filepath, index=False)
        logger.info(f"Saved deposit history to {filename} ({len(df)} records)")

        # Return formatted response
        result = format_csv_response(filepath, df)

        # Log the request for audit trail
        log_request(
            requests_dir=requests_dir,
            requester=requester,
            tool_name="binance_get_deposit_history",
            input_params={"coin": coin, "status": status, "start_time": start_time, "end_time": end_time, "limit": limit},
            output_result=result
        )

        return result
