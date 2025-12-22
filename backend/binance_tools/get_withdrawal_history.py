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


@with_sentry_tracing("binance_get_withdrawal_history")
def fetch_withdrawal_history(
    binance_client: Client,
    coin: Optional[str] = None,
    status: Optional[int] = None,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    limit: Optional[int] = None
) -> pd.DataFrame:
    """
    Fetch Binance crypto withdrawal history and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        coin: Filter by specific cryptocurrency (e.g., 'BTC', 'USDT', 'ETH')
        status: Filter by status - 0=email sent, 1=cancelled, 2=awaiting approval,
                3=rejected, 4=processing, 5=failure, 6=completed
        start_time: Start time in milliseconds (cannot exceed 90 days from end_time)
        end_time: End time in milliseconds
        limit: Number of records to return (default: 1000, max: 1000)

    Returns:
        DataFrame with withdrawal history containing columns:
        - id: Withdrawal ID
        - amount: Withdrawal amount
        - transactionFee: Network transaction fee
        - coin: Cryptocurrency symbol
        - status: Withdrawal status code
        - status_text: Human-readable status
        - address: Withdrawal destination address
        - txId: Transaction hash on blockchain
        - applyTime: Application timestamp
        - network: Network used for withdrawal
        - transferType: Transfer type code
        - withdrawOrderId: Client-defined withdrawal ID
        - info: Additional information (e.g., failure reason)
        - confirmNo: Number of network confirmations
        - walletType: Wallet type (0=spot, 1=funding)
        - completeTime: Completion timestamp (for completed withdrawals)

    Note:
        - Only returns crypto withdrawals (not fiat)
        - Time span between start_time and end_time cannot exceed 90 days
        - Defaults to last 90 days if no time parameters provided
    """
    logger.info(f"Fetching withdrawal history - coin: {coin}, status: {status}, limit: {limit}")

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
        # Fetch withdrawal history from Binance API
        withdrawals = binance_client.get_withdraw_history(**params)
        logger.info(f"Received {len(withdrawals)} withdrawal records")

        # Status code mapping
        status_map = {
            0: "Email Sent",
            1: "Cancelled",
            2: "Awaiting Approval",
            3: "Rejected",
            4: "Processing",
            5: "Failure",
            6: "Completed"
        }

        # Process each withdrawal record
        records = []
        for withdrawal in withdrawals:
            # Parse apply time (string format: "2019-10-12 11:12:02")
            apply_time_str = withdrawal.get('applyTime')
            apply_time_ms = None
            if apply_time_str:
                try:
                    dt = datetime.strptime(apply_time_str, '%Y-%m-%d %H:%M:%S')
                    apply_time_ms = int(dt.timestamp() * 1000)
                except ValueError:
                    logger.warning(f"Could not parse applyTime: {apply_time_str}")

            # Parse complete time (string format)
            complete_time_str = withdrawal.get('completeTime')
            complete_time_ms = None
            if complete_time_str:
                try:
                    dt = datetime.strptime(complete_time_str, '%Y-%m-%d %H:%M:%S')
                    complete_time_ms = int(dt.timestamp() * 1000)
                except ValueError:
                    logger.warning(f"Could not parse completeTime: {complete_time_str}")

            withdrawal_status = withdrawal.get('status')

            records.append({
                'id': withdrawal.get('id'),
                'amount': float(withdrawal.get('amount', 0)),
                'transactionFee': float(withdrawal.get('transactionFee', 0)),
                'coin': withdrawal.get('coin'),
                'status': withdrawal_status,
                'status_text': status_map.get(withdrawal_status, f"Unknown ({withdrawal_status})"),
                'address': withdrawal.get('address'),
                'txId': withdrawal.get('txId', ''),
                'applyTime': apply_time_str,
                'applyTime_ms': apply_time_ms,
                'network': withdrawal.get('network'),
                'transferType': withdrawal.get('transferType'),
                'withdrawOrderId': withdrawal.get('withdrawOrderId', ''),
                'info': withdrawal.get('info', ''),
                'confirmNo': withdrawal.get('confirmNo', 0),
                'walletType': withdrawal.get('walletType', 0),
                'txKey': withdrawal.get('txKey', ''),
                'completeTime': complete_time_str,
                'completeTime_ms': complete_time_ms
            })

        # Create DataFrame
        df = pd.DataFrame(records)

        # Sort by applyTime descending (newest first)
        if not df.empty and 'applyTime_ms' in df.columns:
            df = df.sort_values('applyTime_ms', ascending=False).reset_index(drop=True)

        logger.info(f"Successfully processed {len(df)} withdrawal records")

        return df

    except Exception as e:
        logger.error(f"Error fetching withdrawal history from Binance API: {e}")
        raise


def register_binance_get_withdrawal_history(local_mcp_instance, local_binance_client, csv_dir, requests_dir):
    """Register the binance_get_withdrawal_history tool"""
    @local_mcp_instance.tool()
    def binance_get_withdrawal_history(
        requester: str,
        coin: Optional[str] = None,
        status: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = 1000
    ) -> str:
        """
        Fetch Binance crypto withdrawal history and save to CSV file for analysis.

        This tool retrieves your cryptocurrency withdrawal history including pending,
        completed, cancelled, and failed withdrawals. Results are saved to CSV for
        detailed analysis of outflows from your account.

        Parameters:
            requester (str): Identifier of who is calling this tool (e.g., 'trading-agent', 'user-alex').
                Used for request logging and audit purposes.

            coin (str, optional): Filter by specific cryptocurrency (e.g., 'BTC', 'USDT', 'ETH', 'BNB').
                                  If not provided, returns withdrawals for all coins.

            status (int, optional): Filter by withdrawal status:
                                   - 0: Email Sent (awaiting email confirmation)
                                   - 1: Cancelled (funds remain in account)
                                   - 2: Awaiting Approval
                                   - 3: Rejected (funds remain in account)
                                   - 4: Processing (withdrawal in progress)
                                   - 5: Failure (funds remain in account)
                                   - 6: Completed (successful withdrawal)
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
            - id (string): Withdrawal ID
            - amount (float): Withdrawal amount
            - transactionFee (float): Network transaction fee deducted
            - coin (string): Cryptocurrency symbol
            - status (int): Status code
            - status_text (string): Human-readable status
            - address (string): Withdrawal destination address
            - txId (string): Transaction hash on blockchain (empty until confirmed)
            - applyTime (string): When withdrawal was initiated
            - applyTime_ms (int): Application time in milliseconds
            - network (string): Network used (e.g., 'BTC', 'ETH', 'BEP20', 'TRC20')
            - transferType (int): Transfer type code
            - withdrawOrderId (string): Client-defined withdrawal ID
            - info (string): Additional information (e.g., failure reason)
            - confirmNo (int): Number of network confirmations
            - walletType (int): Wallet type (0=spot, 1=funding)
            - completeTime (string): When withdrawal completed
            - completeTime_ms (int): Completion time in milliseconds

        Important Notes:
            - This endpoint returns CRYPTO withdrawals only (not fiat/bank withdrawals)
            - Time span between start_time and end_time cannot exceed 90 days
            - If no time parameters provided, defaults to last 90 days
            - Request weight: 1 (IP-based rate limit)
            - Rate limit: max 10 requests/second

        Use Cases:
            - Track all withdrawal activity
            - Calculate total withdrawals by coin
            - Calculate total network fees paid
            - Identify pending or failed withdrawals
            - Audit withdrawal history for specific time periods
            - Monitor withdrawal processing times
            - Calculate net outflow (withdrawals - fees)
            - Compare withdrawals vs deposits for trading efficiency

        Example usage:

            # Get all withdrawals (last 90 days)
            binance_get_withdrawal_history()

            # Get BTC withdrawals only
            binance_get_withdrawal_history(coin='BTC')

            # Get completed withdrawals only
            binance_get_withdrawal_history(status=6)

            # Get withdrawals within specific date range
            binance_get_withdrawal_history(
                coin='USDT',
                start_time=1609459200000,  # Jan 1, 2021
                end_time=1640995200000,     # Jan 1, 2022
                status=6                     # Completed only
            )

            # Get recent withdrawals (last 100)
            binance_get_withdrawal_history(limit=100)

        Analysis Examples (use py_eval tool):

            # Calculate total withdrawals by coin
            df = pd.read_csv('withdrawals.csv')
            completed = df[df['status'] == 6]
            totals = completed.groupby('coin')['amount'].sum().sort_values(ascending=False)

            # Calculate total fees paid
            total_fees = completed['transactionFee'].sum()

            # Find pending withdrawals
            pending = df[df['status'].isin([0, 2, 4])]

            # Calculate average withdrawal processing time
            df['processing_time_seconds'] = (df['completeTime_ms'] - df['applyTime_ms']) / 1000
            avg_time = df[df['status'] == 6]['processing_time_seconds'].mean()

            # Net withdrawal amount (after fees)
            df['net_amount'] = df['amount'] - df['transactionFee']

        Note:
            For fiat withdrawals (EUR, USD, etc.), use a separate fiat withdrawal history endpoint.
        """
        logger.info(f"binance_get_withdrawal_history tool invoked by {requester} - coin: {coin}, status: {status}")

        # Call fetch function
        df = fetch_withdrawal_history(
            binance_client=local_binance_client,
            coin=coin,
            status=status,
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )

        if df.empty:
            return f"No withdrawal history found for the specified parameters (coin: {coin}, status: {status})."

        # Generate filename with unique identifier
        coin_suffix = f"_{coin}" if coin else ""
        filename = f"withdrawal_history{coin_suffix}_{str(uuid.uuid4())[:8]}.csv"
        filepath = csv_dir / filename

        # Save to CSV file
        df.to_csv(filepath, index=False)
        logger.info(f"Saved withdrawal history to {filename} ({len(df)} records)")

        # Return formatted response
        result = format_csv_response(filepath, df)

        # Log the request for audit trail
        log_request(
            requests_dir=requests_dir,
            requester=requester,
            tool_name="binance_get_withdrawal_history",
            input_params={"coin": coin, "status": status, "start_time": start_time, "end_time": end_time, "limit": limit},
            output_result=result
        )

        return result
