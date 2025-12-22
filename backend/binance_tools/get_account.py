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


@with_sentry_tracing("binance_get_account")
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
        - price_usdt: Current price in USDT (None if not available)
        - value_usdt: Total value in USDT (total * price_usdt, None if price not available)

    Note:
        Only returns assets with non-zero balances (where free + locked > 0).
        Results are sorted by USDT value in descending order, with assets
        without USDT prices at the end.
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

        # Fetch all ticker prices for valuation
        logger.info("Fetching current prices...")
        all_tickers = binance_client.get_all_tickers()

        # Build price lookup dictionary for USDT pairs
        price_map = {}
        for ticker in all_tickers:
            symbol = ticker['symbol']
            # Extract prices for USDT pairs (e.g., BTCUSDT -> BTC)
            if symbol.endswith('USDT'):
                asset = symbol[:-4]  # Remove 'USDT' suffix
                price_map[asset] = Decimal(ticker['price'])

        # USDT itself has a price of 1.0
        price_map['USDT'] = Decimal('1.0')

        logger.info(f"Built price map for {len(price_map)} assets")

        # Get all balances
        balances = account_info.get('balances', [])

        # Filter and process balances with non-zero amounts
        for balance in balances:
            free = Decimal(balance['free'])
            locked = Decimal(balance['locked'])
            total = free + locked

            # Only include assets with non-zero balance
            if total > 0:
                asset = balance['asset']
                # Get USDT price for this asset
                price_usdt = price_map.get(asset)

                # Calculate value in USDT
                if price_usdt is not None:
                    value_usdt = float(total * price_usdt)
                else:
                    value_usdt = None

                records.append({
                    'asset': asset,
                    'free': float(free),
                    'locked': float(locked),
                    'total': float(total),
                    'price_usdt': float(price_usdt) if price_usdt is not None else None,
                    'value_usdt': value_usdt
                })

        logger.info(f"Found {len(records)} assets with non-zero balance")

    except Exception as e:
        logger.error(f"Error fetching account data from Binance API: {e}")
        raise

    # Create DataFrame
    df = pd.DataFrame(records)

    # Sort by USDT value (descending), then by total amount
    # Put assets without USDT price at the end
    if not df.empty:
        df['_sort_key'] = df['value_usdt'].fillna(-1)
        df = df.sort_values('_sort_key', ascending=False).reset_index(drop=True)
        df = df.drop(columns=['_sort_key'])

    logger.info(f"Successfully fetched account data for {len(df)} assets")

    return df


def register_binance_get_account(local_mcp_instance, local_binance_client, csv_dir, requests_dir):
    """Register the binance_get_account tool"""
    @local_mcp_instance.tool()
    def binance_get_account(requester: str) -> str:
        """
        Fetch Binance account portfolio information with current prices and save to CSV file for analysis.

        This tool retrieves your current Binance account balances including all assets
        with non-zero amounts (available + locked), along with current USDT prices and
        total values. The data is saved to a CSV file for detailed analysis using the py_eval tool.

        Parameters:
            requester (str): Identifier of who is calling this tool (e.g., 'trading-agent', 'user-alex').
                Used for request logging and audit purposes.

        Returns:
            str: Formatted response with CSV file info, schema, sample data, and Python snippet to load the file.

        CSV Output Columns:
            - asset (string): Asset symbol (e.g., 'BTC', 'USDT', 'ETH', 'BNB')
            - free (float): Available balance that can be used for trading
            - locked (float): Balance currently locked in open orders
            - total (float): Total balance (free + locked)
            - price_usdt (float): Current price of the asset in USDT (None if not available)
            - value_usdt (float): Total value in USDT (total * price_usdt, None if price not available)

        Account Information:
            This tool accesses READ-ONLY account information and does NOT perform any trades,
            withdrawals, or deposits. It requires API keys with account read permissions.

        Use Cases:
            - Portfolio valuation in USDT
            - Portfolio composition analysis
            - Asset allocation review by value
            - Available balance checks before trading
            - Monitoring locked balances in open orders
            - Historical portfolio snapshots (by saving multiple CSV files over time)
            - Comparing portfolio value over time

        Always use the py_eval tool to analyze the saved CSV file for insights such as:
            - Total portfolio value calculation (sum of value_usdt column)
            - Asset diversification metrics by value
            - Identifying largest holdings by USDT value
            - Assets without USDT pricing (value_usdt is None)
            - Comparing portfolio changes over time

        Example usage:
            binance_get_account()

        Note:
            Results are sorted by USDT value in descending order, showing your
            most valuable holdings first. Assets without USDT pricing appear at the end.
        """
        logger.info(f"binance_get_account tool invoked by {requester}")

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
        result = format_csv_response(filepath, df)

        # Log the request for audit trail
        log_request(
            requests_dir=requests_dir,
            requester=requester,
            tool_name="binance_get_account",
            input_params={},
            output_result=result
        )

        return result
