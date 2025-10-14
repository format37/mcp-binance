import logging
from datetime import datetime
from decimal import Decimal
import uuid
from mcp_service import format_csv_response
import pandas as pd
from binance.client import Client
from typing import Optional

logger = logging.getLogger(__name__)


def fetch_account(binance_client: Client) -> pd.DataFrame:
    """
    Fetch Binance account information and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client

    Returns:
        DataFrame with account balances containing columns:
        - asset: Asset symbol (e.g., 'BTC', 'USDT', 'ETH')
        - free: Available balance for trading
        - locked: Balance locked in open orders
        - total: Total balance (free + locked)

    Note:
        Only returns assets with non-zero balances (where free + locked > 0).
        Results are sorted by total amount in descending order.
    """
    logger.info("Fetching Binance account information")

    records = []
    try:
        # Fetch account information from Binance API
        account_info = binance_client.get_account()

        # Log account status
        logger.info(
            f"Account status - Can Trade: {account_info.get('canTrade')}, "
            f"Can Withdraw: {account_info.get('canWithdraw')}, "
            f"Can Deposit: {account_info.get('canDeposit')}"
        )

        # Get all balances
        balances = account_info.get('balances', [])

        # Filter and process balances with non-zero amounts
        for balance in balances:
            free = Decimal(balance['free'])
            locked = Decimal(balance['locked'])
            total = free + locked

            # Only include assets with non-zero balance
            if total > 0:
                records.append({
                    'asset': balance['asset'],
                    'free': float(free),
                    'locked': float(locked),
                    'total': float(total)
                })

        logger.info(f"Found {len(records)} assets with non-zero balance")

    except Exception as e:
        logger.error(f"Error fetching account data from Binance API: {e}")
        raise

    # Create DataFrame
    df = pd.DataFrame(records)

    # Sort by total amount (descending)
    if not df.empty:
        df = df.sort_values('total', ascending=False).reset_index(drop=True)

    logger.info(f"Successfully fetched account data for {len(df)} assets")

    return df


def register_binance_get_account(local_mcp_instance, local_binance_client, csv_dir):
    """Register the binance_get_account tool"""
    @local_mcp_instance.tool()
    def binance_get_account() -> str:
        """
        Fetch Binance account portfolio information and save to CSV file for analysis.

        This tool retrieves your current Binance account balances including all assets
        with non-zero amounts (available + locked). The data is saved to a CSV file
        for detailed analysis using the py_eval tool.

        Returns:
            str: Formatted response with CSV file info, schema, sample data, and Python snippet to load the file.

        CSV Output Columns:
            - asset (string): Asset symbol (e.g., 'BTC', 'USDT', 'ETH', 'BNB')
            - free (float): Available balance that can be used for trading
            - locked (float): Balance currently locked in open orders
            - total (float): Total balance (free + locked)

        Account Information:
            This tool accesses READ-ONLY account information and does NOT perform any trades,
            withdrawals, or deposits. It requires API keys with account read permissions.

        Use Cases:
            - Portfolio composition analysis
            - Asset allocation review
            - Available balance checks before trading
            - Monitoring locked balances in open orders
            - Historical portfolio snapshots (by saving multiple CSV files over time)

        Always use the py_eval tool to analyze the saved CSV file for insights such as:
            - Total portfolio value calculation (when combined with price data)
            - Asset diversification metrics
            - Identifying assets with locked balances
            - Comparing portfolio changes over time

        Example usage:
            binance_get_account()

        Note:
            Results are sorted by total amount in descending order, showing your
            largest holdings first.
        """
        logger.info("binance_get_account tool invoked")

        # Call fetch_account function
        df = fetch_account(binance_client=local_binance_client)

        if df.empty:
            return "No assets found with non-zero balance in your Binance account."

        # Generate filename with unique identifier
        filename = f"account_{str(uuid.uuid4())[:8]}.csv"
        filepath = csv_dir / filename

        # Save to CSV file
        df.to_csv(filepath, index=False)
        logger.info(f"Saved account data to {filename} ({len(df)} assets)")

        # Return formatted response
        return format_csv_response(filepath, df)
