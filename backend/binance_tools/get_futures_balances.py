import logging
from datetime import datetime
from decimal import Decimal
import uuid
from mcp_service import format_csv_response
import pandas as pd
from binance.client import Client
from sentry_utils import with_sentry_tracing

logger = logging.getLogger(__name__)


@with_sentry_tracing("binance_get_futures_balances")
def fetch_futures_balances(binance_client: Client) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetch Binance futures account information and return as DataFrames.

    Args:
        binance_client: Initialized Binance Client

    Returns:
        Tuple of (account_df, positions_df):
        - account_df: Account summary with wallet balance, margin, P&L
        - positions_df: Open positions with entry price, liquidation price, P&L

    Note:
        Futures trading involves liquidation risk. Monitor positions carefully.
    """
    logger.info("Fetching Binance futures account information")

    try:
        # Fetch account information from Binance API
        account = binance_client.futures_account()

        logger.info(
            f"Account status - Can Trade: {account.get('canTrade')}, "
            f"Can Withdraw: {account.get('canWithdraw')}, "
            f"Can Deposit: {account.get('canDeposit')}"
        )

        # Account summary record
        account_record = {
            'timestamp': datetime.fromtimestamp(account['updateTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
            'canTrade': account.get('canTrade', False),
            'canDeposit': account.get('canDeposit', False),
            'canWithdraw': account.get('canWithdraw', False),
            'featureSe

t': account.get('featureSet', 'N/A'),
            'totalWalletBalance': float(account['totalWalletBalance']),
            'totalUnrealizedProfit': float(account['totalUnrealizedProfit']),
            'totalMarginBalance': float(account['totalMarginBalance']),
            'totalInitialMargin': float(account['totalInitialMargin']),
            'totalMaintMargin': float(account['totalMaintMargin']),
            'availableBalance': float(account['availableBalance']),
            'maxWithdrawAmount': float(account['maxWithdrawAmount'])
        }

        # Calculate risk metrics
        total_margin_balance = Decimal(account['totalMarginBalance'])
        total_maint_margin = Decimal(account['totalMaintMargin'])
        if total_margin_balance > 0 and total_maint_margin > 0:
            margin_ratio = float((total_maint_margin / total_margin_balance) * 100)
            account_record['marginRatio'] = margin_ratio
        else:
            account_record['marginRatio'] = 0.0

        # Calculate effective leverage
        total_initial_margin = Decimal(account['totalInitialMargin'])
        total_wallet_balance = Decimal(account['totalWalletBalance'])
        if total_initial_margin > 0 and total_wallet_balance > 0:
            effective_leverage = float(total_initial_margin / total_wallet_balance)
            account_record['effectiveLeverage'] = effective_leverage
        else:
            account_record['effectiveLeverage'] = 0.0

        account_df = pd.DataFrame([account_record])

        # Process open positions
        position_records = []
        for position in account['positions']:
            pos_amt = Decimal(position['positionAmt'])
            if pos_amt != 0:
                entry_price = Decimal(position['entryPrice'])
                mark_price = Decimal(position['markPrice'])
                liquidation_price = Decimal(position['liquidationPrice']) if position['liquidationPrice'] != '0' else None

                # Calculate liquidation distance
                if liquidation_price and mark_price > 0:
                    liq_distance = float(abs((liquidation_price - mark_price) / mark_price * 100))
                else:
                    liq_distance = None

                position_records.append({
                    'symbol': position['symbol'],
                    'positionAmt': float(pos_amt),
                    'positionSide': position['positionSide'],
                    'entryPrice': float(entry_price),
                    'markPrice': float(mark_price),
                    'liquidationPrice': float(liquidation_price) if liquidation_price else None,
                    'liqDistancePct': liq_distance,
                    'unRealizedProfit': float(position['unRealizedProfit']),
                    'leverage': int(position['leverage']),
                    'initialMargin': float(position['initialMargin']),
                    'maintMargin': float(position['maintMargin']),
                    'isolated': position['isolated'],
                    'updateTime': datetime.fromtimestamp(int(position['updateTime']) / 1000).strftime('%Y-%m-%d %H:%M:%S')
                })

        positions_df = pd.DataFrame(position_records) if position_records else pd.DataFrame(columns=[
            'symbol', 'positionAmt', 'positionSide', 'entryPrice', 'markPrice',
            'liquidationPrice', 'liqDistancePct', 'unRealizedProfit', 'leverage',
            'initialMargin', 'maintMargin', 'isolated', 'updateTime'
        ])

        logger.info(f"Successfully fetched futures account data with {len(position_records)} open positions")

        return account_df, positions_df

    except Exception as e:
        logger.error(f"Error fetching futures account data from Binance API: {e}")
        raise


def register_binance_get_futures_balances(local_mcp_instance, local_binance_client, csv_dir):
    """Register the binance_get_futures_balances tool"""
    @local_mcp_instance.tool()
    def binance_get_futures_balances() -> str:
        """
        Fetch Binance futures account balance, margin, and open positions to CSV files.

        This tool retrieves comprehensive futures account information including wallet balance,
        margin usage, unrealized P&L, and all open leveraged positions. Results are saved to
        two CSV files for detailed analysis using the py_eval tool.

        ⚠️  FUTURES TRADING WARNING ⚠️
        Futures trading involves leverage and liquidation risk. Positions can result in losses
        exceeding your initial investment. Always monitor liquidation prices and margin ratios.

        Returns:
            str: Formatted response with two CSV files (account summary and positions), schema,
                sample data, and Python snippet to load the files.

        CSV 1 - Account Summary Columns:
            - timestamp (string): Account update timestamp
            - canTrade (boolean): Trading permission status
            - canDeposit (boolean): Deposit permission status
            - canWithdraw (boolean): Withdrawal permission status
            - featureSet (string): Account feature set
            - totalWalletBalance (float): Total wallet balance in USDT
            - totalUnrealizedProfit (float): Total unrealized P&L across all positions
            - totalMarginBalance (float): Total margin balance (wallet + unrealized P&L)
            - totalInitialMargin (float): Total initial margin used by positions
            - totalMaintMargin (float): Total maintenance margin required
            - availableBalance (float): Available balance for new positions
            - maxWithdrawAmount (float): Maximum amount that can be withdrawn
            - marginRatio (float): Margin ratio percentage (risk indicator)
            - effectiveLeverage (float): Effective leverage across all positions

        CSV 2 - Open Positions Columns:
            - symbol (string): Trading pair (e.g., 'BTCUSDT')
            - positionAmt (float): Position size (positive=LONG, negative=SHORT)
            - positionSide (string): Position side (LONG, SHORT, BOTH)
            - entryPrice (float): Average entry price
            - markPrice (float): Current mark price
            - liquidationPrice (float): Liquidation price (None if no liquidation risk)
            - liqDistancePct (float): Distance to liquidation in percentage
            - unRealizedProfit (float): Unrealized P&L in USDT
            - leverage (integer): Leverage multiplier (1x-125x)
            - initialMargin (float): Initial margin for this position
            - maintMargin (float): Maintenance margin required
            - isolated (boolean): True if isolated margin mode
            - updateTime (string): Last update timestamp

        Risk Indicators:
            - Margin Ratio: Higher = More risk. >80% = Critical, >60% = Warning, <40% = Healthy
            - Liquidation Distance: Lower = More risk. <5% = Critical, <10% = High risk, >20% = Safer
            - Effective Leverage: Higher = More risk. >20x = Very aggressive, <5x = Conservative

        Use Cases:
            - Check futures account balance and available margin
            - Monitor all open leveraged positions
            - Assess liquidation risk across portfolio
            - Calculate total unrealized P&L
            - Track margin usage and availability
            - Verify account permissions before trading
            - Portfolio risk assessment

        Always use the py_eval tool to analyze the saved CSV files for insights such as:
            - Total portfolio value and P&L
            - Positions closest to liquidation
            - Margin utilization percentage
            - Leverage distribution across positions
            - Risk-weighted portfolio analysis

        Example usage:
            binance_get_futures_balances()

        Note:
            - This is a READ-ONLY operation
            - Requires API key with futures account read permissions
            - Returns empty positions if no open positions
            - Liquidation can occur if margin ratio becomes too high
            - Always monitor positions in volatile markets
        """
        logger.info("binance_get_futures_balances tool invoked")

        try:
            # Call fetch_futures_balances function
            account_df, positions_df = fetch_futures_balances(binance_client=local_binance_client)

            # Generate filenames with unique identifier
            uid = str(uuid.uuid4())[:8]
            account_filename = f"futures_account_{uid}.csv"
            positions_filename = f"futures_positions_{uid}.csv"
            account_filepath = csv_dir / account_filename
            positions_filepath = csv_dir / positions_filename

            # Save to CSV files
            account_df.to_csv(account_filepath, index=False)
            positions_df.to_csv(positions_filepath, index=False)

            logger.info(f"Saved futures account data to {account_filename}")
            logger.info(f"Saved {len(positions_df)} positions to {positions_filename}")

            # Build combined response
            account_response = format_csv_response(account_filepath, account_df)
            positions_response = format_csv_response(positions_filepath, positions_df)

            result = f"""═══════════════════════════════════════════════════════════════════════════════
FUTURES ACCOUNT DATA SAVED
═══════════════════════════════════════════════════════════════════════════════

File 1: ACCOUNT SUMMARY
{account_response}

File 2: OPEN POSITIONS ({len(positions_df)} positions)
{positions_response}

═══════════════════════════════════════════════════════════════════════════════
RISK ASSESSMENT
═══════════════════════════════════════════════════════════════════════════════
"""

            # Add risk warnings
            if not account_df.empty:
                margin_ratio = account_df.iloc[0]['marginRatio']
                if margin_ratio >= 80:
                    result += "🚨 CRITICAL: Margin ratio very high - liquidation risk!\n"
                elif margin_ratio >= 60:
                    result += "⚠️  WARNING: High margin ratio - consider reducing positions\n"
                elif margin_ratio >= 40:
                    result += "⚡ CAUTION: Moderate margin usage\n"
                else:
                    result += "✓  Healthy margin ratio\n"

            if not positions_df.empty:
                critical_positions = len(positions_df[positions_df['liqDistancePct'] < 10])
                if critical_positions > 0:
                    result += f"🚨 WARNING: {critical_positions} position(s) within 10% of liquidation!\n"

            result += "═══════════════════════════════════════════════════════════════════════════════\n"

            return result

        except Exception as e:
            logger.error(f"Error in binance_get_futures_balances: {e}")
            return f"Error fetching futures account data: {str(e)}\n\nPlease check:\n- API credentials are valid\n- API key has futures account read permissions\n- Futures account is activated on Binance\n- Network connectivity"
