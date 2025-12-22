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


@with_sentry_tracing("binance_set_futures_leverage")
def set_leverage_operation(binance_client: Client, symbol: str, leverage: int) -> pd.DataFrame:
    """
    Set leverage for a futures symbol and return as DataFrame.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        leverage: Leverage multiplier (1-125)

    Returns:
        DataFrame with leverage setting confirmation

    Note:
        Higher leverage = Higher liquidation risk!
    """
    logger.info(f"Setting leverage for {symbol} to {leverage}x")

    if leverage < 1 or leverage > 125:
        raise ValueError("Leverage must be between 1 and 125")

    try:
        # Set leverage
        logger.warning(f"⚠️  SETTING LEVERAGE: {symbol} to {leverage}x")
        result = binance_client.futures_change_leverage(symbol=symbol, leverage=leverage)

        # Calculate risk level
        if leverage >= 75:
            risk_level = "EXTREME"
            risk_warning = "75x+ leverage can liquidate very quickly (~1% move)"
        elif leverage >= 20:
            risk_level = "HIGH"
            risk_warning = "20x+ leverage is very risky (~5% move)"
        elif leverage >= 10:
            risk_level = "MODERATE"
            risk_warning = "10x+ leverage requires careful monitoring (~10% move)"
        elif leverage >= 5:
            risk_level = "REASONABLE"
            risk_warning = "Moderate leverage - reasonable risk management possible"
        else:
            risk_level = "CONSERVATIVE"
            risk_warning = "Conservative leverage - lower risk, more margin safety"

        record = {
            'symbol': result['symbol'],
            'leverage': int(result['leverage']),
            'maxNotionalValue': float(result.get('maxNotionalValue', 0)),
            'riskLevel': risk_level,
            'riskWarning': risk_warning,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        df = pd.DataFrame([record])
        logger.info(f"Leverage set successfully for {symbol}: {leverage}x")

        return df

    except Exception as e:
        logger.error(f"Error setting leverage: {e}")
        raise


@with_sentry_tracing("binance_set_futures_margin_type")
def set_margin_type_operation(binance_client: Client, symbol: str, margin_type: str) -> pd.DataFrame:
    """
    Set margin type (CROSSED or ISOLATED) for a futures symbol.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        margin_type: 'CROSSED' or 'ISOLATED'

    Returns:
        DataFrame with margin type setting confirmation

    Note:
        Can only change margin type when no open positions exist.
    """
    logger.info(f"Setting margin type for {symbol} to {margin_type}")

    margin_type = margin_type.upper()
    if margin_type not in ['CROSSED', 'ISOLATED']:
        raise ValueError("margin_type must be 'CROSSED' or 'ISOLATED'")

    try:
        # Set margin type
        logger.warning(f"⚠️  SETTING MARGIN TYPE: {symbol} to {margin_type}")
        result = binance_client.futures_change_margin_type(symbol=symbol, marginType=margin_type)

        if margin_type == 'ISOLATED':
            description = "Each position has dedicated margin. Liquidation only affects specific position."
        else:
            description = "Margin shared across all positions. More efficient but liquidation affects entire account."

        record = {
            'symbol': symbol,
            'marginType': margin_type,
            'description': description,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        df = pd.DataFrame([record])
        logger.info(f"Margin type set successfully for {symbol}: {margin_type}")

        return df

    except Exception as e:
        error_msg = str(e)
        if "-4046" in error_msg or "No need to change margin type" in error_msg:
            # Margin type already set or position exists
            logger.info(f"Margin type already {margin_type} for {symbol}")
            record = {
                'symbol': symbol,
                'marginType': margin_type,
                'description': f"Already set to {margin_type} or position exists. Close positions to change.",
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            return pd.DataFrame([record])
        else:
            logger.error(f"Error setting margin type: {e}")
            raise


@with_sentry_tracing("binance_get_futures_leverage_info")
def get_leverage_info(binance_client: Client, symbol: str) -> pd.DataFrame:
    """
    Get current leverage and margin mode for a symbol.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT')

    Returns:
        DataFrame with current leverage settings
    """
    logger.info(f"Getting leverage info for {symbol}")

    try:
        account = binance_client.futures_account()

        records = []
        for position in account['positions']:
            if position['symbol'] == symbol:
                leverage = int(position['leverage'])
                isolated = position['isolated']
                margin_type = "ISOLATED" if isolated else "CROSSED"

                # Get leverage brackets
                try:
                    brackets = binance_client.futures_leverage_bracket(symbol=symbol)
                    max_leverage = brackets[0]['brackets'][0]['initialLeverage'] if brackets else 125
                except:
                    max_leverage = 125

                records.append({
                    'symbol': symbol,
                    'currentLeverage': leverage,
                    'marginType': margin_type,
                    'maxLeverageAvailable': max_leverage,
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })

        if not records:
            # Symbol not found in positions, get default info
            try:
                brackets = binance_client.futures_leverage_bracket(symbol=symbol)
                max_leverage = brackets[0]['brackets'][0]['initialLeverage'] if brackets else 125
            except:
                max_leverage = 125

            records.append({
                'symbol': symbol,
                'currentLeverage': None,
                'marginType': None,
                'maxLeverageAvailable': max_leverage,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })

        df = pd.DataFrame(records)
        logger.info(f"Retrieved leverage info for {symbol}")

        return df

    except Exception as e:
        logger.error(f"Error getting leverage info: {e}")
        raise


def register_binance_set_futures_leverage(local_mcp_instance, local_binance_client, csv_dir, requests_dir):
    """Register the binance_set_futures_leverage tool"""
    @local_mcp_instance.tool()
    def binance_set_futures_leverage(requester: str, symbol: str, leverage: Optional[int] = None,
                                     margin_type: Optional[str] = None,
                                     get_info: bool = False) -> str:
        """
        Manage leverage and margin settings for futures trading symbols and save to CSV.

        ⚠️  LEVERAGE WARNING ⚠️
        Higher leverage = Higher risk of liquidation. Losses can EXCEED your investment.
        Start with LOW leverage (2x-5x) until experienced. 75x+ leverage is EXTREMELY risky.

        Parameters:
            requester (string, required): Name of the requester making this call (for request logging)
            symbol (string, required): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT')
            leverage (integer, optional): Leverage multiplier to set (1-125)
            margin_type (string, optional): Margin type - 'CROSSED' or 'ISOLATED'
            get_info (boolean, optional): If True, returns current settings without changes (default: False)

        Returns:
            str: Formatted response with CSV file containing leverage/margin settings.

        CSV Output Columns (for set leverage):
            - symbol (string): Trading pair symbol
            - leverage (integer): Leverage multiplier (1x-125x)
            - maxNotionalValue (float): Maximum notional value with this leverage
            - riskLevel (string): Risk assessment (CONSERVATIVE, REASONABLE, MODERATE, HIGH, EXTREME)
            - riskWarning (string): Risk warning message
            - timestamp (string): Setting update timestamp

        CSV Output Columns (for set margin type):
            - symbol (string): Trading pair symbol
            - marginType (string): CROSSED or ISOLATED
            - description (string): Margin mode explanation
            - timestamp (string): Setting update timestamp

        CSV Output Columns (for get info):
            - symbol (string): Trading pair symbol
            - currentLeverage (integer): Current leverage setting
            - marginType (string): Current margin type (CROSSED or ISOLATED)
            - maxLeverageAvailable (integer): Maximum leverage available for symbol
            - timestamp (string): Query timestamp

        Leverage Levels:
            - Conservative (1x-5x): Lower risk, more margin required, safer for beginners
            - Reasonable (5x-10x): Moderate risk/reward, suitable for experienced traders
            - Moderate (10x-20x): Higher risk, requires active monitoring
            - High (20x-75x): Very high risk, can liquidate quickly with small moves
            - Extreme (75x-125x): Extremely risky, NOT recommended, ~1% move = liquidation

        Margin Modes:
            - CROSSED: Shares margin across all positions (default, more efficient)
                * Pros: Better margin utilization, lower liquidation risk per position
                * Cons: One position liquidation can affect entire account
            - ISOLATED: Each position has dedicated margin (limits risk per position)
                * Pros: Liquidation only affects specific position
                * Cons: Less efficient margin usage, requires more capital

        Parameter Rules:
            - Use get_info=True to check current settings without making changes
            - Use leverage parameter to set leverage (1-125)
            - Use margin_type parameter to set margin mode ('CROSSED' or 'ISOLATED')
            - Can set leverage and margin_type in same call
            - Cannot change margin_type while open positions exist

        Use Cases:
            - Check current leverage settings before trading
            - Set appropriate leverage for trading strategy
            - Switch margin modes based on risk tolerance
            - Reduce leverage to lower liquidation risk
            - Increase leverage for more capital efficiency (higher risk)

        Risk Guidelines:
            - Beginners: Start with 2x-5x leverage maximum
            - Experienced: 5x-20x leverage with proper risk management
            - Advanced: >20x leverage ONLY with strict stop-loss discipline
            - NEVER use maximum leverage without extensive experience

        Example usage:
            # Check current settings
            binance_set_futures_leverage(symbol="BTCUSDT", get_info=True)

            # Set leverage to 10x
            binance_set_futures_leverage(symbol="BTCUSDT", leverage=10)

            # Set margin type to isolated
            binance_set_futures_leverage(symbol="ETHUSDT", margin_type="ISOLATED")

            # Set both leverage and margin type
            binance_set_futures_leverage(symbol="BTCUSDT", leverage=5, margin_type="CROSSED")

        CRITICAL Safety Rules:
            - ALWAYS start with low leverage (2x-5x) when learning
            - Higher leverage does NOT increase profit potential long-term
            - Set leverage BEFORE opening positions
            - Monitor liquidation prices constantly with high leverage
            - Use stop-loss orders to protect against liquidation
            - Understand that 10x leverage = 10% adverse move = liquidation

        Common Errors:
            - "No need to change margin type": Already set or position exists
            - "Leverage exceeds maximum": Symbol doesn't support that leverage level
            - Close all positions before changing margin type

        Note:
            - Leverage setting persists until you change it
            - Each symbol has independent leverage settings
            - Check max available leverage per symbol (varies by symbol)
            - Lower leverage = More margin safety = Lower liquidation risk
            - This tool modifies REAL account settings
        """
        logger.info(f"binance_set_futures_leverage tool invoked for {symbol} by {requester}")

        if not symbol:
            return "Error: symbol is required (e.g., 'BTCUSDT')"

        # Validate parameters
        if not get_info and not leverage and not margin_type:
            return "Error: Must specify leverage, margin_type, or get_info=True"

        try:
            results = []

            # Get info mode
            if get_info:
                df = get_leverage_info(
                    binance_client=local_binance_client,
                    symbol=symbol
                )
                filename = f"leverage_info_{symbol}_{str(uuid.uuid4())[:8]}.csv"
                filepath = csv_dir / filename
                df.to_csv(filepath, index=False)
                logger.info(f"Saved leverage info to {filename}")

                result = format_csv_response(filepath, df)

                # Log request
                log_request(
                    requests_dir=requests_dir,
                    requester=requester,
                    tool_name="binance_set_futures_leverage",
                    input_params={
                        "symbol": symbol,
                        "leverage": leverage,
                        "margin_type": margin_type,
                        "get_info": get_info
                    },
                    output_result=result
                )

                info_data = df.iloc[0]
                summary = f"""

═══════════════════════════════════════════════════════════════════════════════
LEVERAGE SETTINGS FOR {symbol}
═══════════════════════════════════════════════════════════════════════════════
"""
                if info_data['currentLeverage']:
                    summary += f"""Current Leverage:    {int(info_data['currentLeverage'])}x
Margin Type:         {info_data['marginType']}
Max Available:       {int(info_data['maxLeverageAvailable'])}x
"""
                else:
                    summary += f"""No position exists yet for {symbol}
Max Available:       {int(info_data['maxLeverageAvailable'])}x
═══════════════════════════════════════════════════════════════════════════════
"""

                return result + summary

            # Set leverage
            if leverage:
                df = set_leverage_operation(
                    binance_client=local_binance_client,
                    symbol=symbol,
                    leverage=leverage
                )
                filename = f"set_leverage_{symbol}_{leverage}x_{str(uuid.uuid4())[:8]}.csv"
                filepath = csv_dir / filename
                df.to_csv(filepath, index=False)
                logger.info(f"Saved leverage setting to {filename}")

                results.append(format_csv_response(filepath, df))

                lev_data = df.iloc[0]
                results.append(f"""

═══════════════════════════════════════════════════════════════════════════════
LEVERAGE UPDATED
═══════════════════════════════════════════════════════════════════════════════
Symbol:              {lev_data['symbol']}
New Leverage:        {int(lev_data['leverage'])}x
Risk Level:          {lev_data['riskLevel']}
Max Notional:        ${lev_data['maxNotionalValue']:,.0f}

⚠️  {lev_data['riskWarning']}
═══════════════════════════════════════════════════════════════════════════════
""")

            # Set margin type
            if margin_type:
                df = set_margin_type_operation(
                    binance_client=local_binance_client,
                    symbol=symbol,
                    margin_type=margin_type
                )
                filename = f"set_margin_{symbol}_{margin_type.lower()}_{str(uuid.uuid4())[:8]}.csv"
                filepath = csv_dir / filename
                df.to_csv(filepath, index=False)
                logger.info(f"Saved margin type setting to {filename}")

                margin_result = format_csv_response(filepath, df)
                results.append(margin_result)

                # Log request
                log_request(
                    requests_dir=requests_dir,
                    requester=requester,
                    tool_name="binance_set_futures_leverage",
                    input_params={
                        "symbol": symbol,
                        "leverage": leverage,
                        "margin_type": margin_type,
                        "get_info": get_info
                    },
                    output_result=margin_result
                )

                margin_data = df.iloc[0]
                results.append(f"""

═══════════════════════════════════════════════════════════════════════════════
MARGIN TYPE UPDATED
═══════════════════════════════════════════════════════════════════════════════
Symbol:              {margin_data['symbol']}
Margin Type:         {margin_data['marginType']}
Description:         {margin_data['description']}
═══════════════════════════════════════════════════════════════════════════════
""")

            return "\n".join(results)

        except ValueError as e:
            return f"Error: {str(e)}"
        except Exception as e:
            logger.error(f"Error managing leverage: {e}")
            error_msg = str(e)

            if "-4046" in error_msg:
                return f"Error: Cannot change margin type while positions exist.\n\nClose all {symbol} positions first, then try again."
            elif "exceeds maximum" in error_msg.lower():
                return f"Error: Leverage exceeds maximum allowed for {symbol}.\n\nUse get_info=True to check maximum available leverage."
            else:
                return f"Error: {error_msg}\n\nCheck:\n- API credentials valid\n- Futures trading enabled\n- Symbol is correct\n- Leverage within limits"
