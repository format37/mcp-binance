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


@with_sentry_tracing("binance_calculate_liquidation_risk")
def calculate_liquidation_risk(binance_client: Client, symbol: Optional[str] = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calculate liquidation risk for futures positions and return as DataFrames.

    Args:
        binance_client: Initialized Binance Client
        symbol: Optional specific symbol to analyze

    Returns:
        Tuple of (risk_analysis_df, summary_df):
        - risk_analysis_df: Detailed risk metrics per position
        - summary_df: Portfolio-wide risk summary

    Note:
        This is a READ-ONLY operation that calculates risk metrics.
    """
    logger.info(f"Calculating liquidation risk" + (f" for {symbol}" if symbol else " for all positions"))

    try:
        # Get positions
        if symbol:
            positions = binance_client.futures_position_information(symbol=symbol)
        else:
            positions = binance_client.futures_position_information()

        risk_records = []
        for position in positions:
            pos_amt = Decimal(position['positionAmt'])
            if pos_amt == 0:
                continue  # Skip zero positions

            symbol_name = position['symbol']
            entry_price = Decimal(position['entryPrice'])
            mark_price = Decimal(position['markPrice'])
            liquidation_price = Decimal(position['liquidationPrice']) if position['liquidationPrice'] != '0' else None
            unrealized_pnl = Decimal(position['unRealizedProfit'])
            leverage = int(position['leverage'])
            initial_margin = Decimal(position['initialMargin'])
            maint_margin = Decimal(position['maintMargin'])

            # Determine direction
            direction = "LONG" if pos_amt > 0 else "SHORT"

            # Calculate risk metrics
            if liquidation_price and mark_price > 0:
                # Distance to liquidation (percentage)
                liq_distance_pct = float(abs((liquidation_price - mark_price) / mark_price * 100))

                # Distance to liquidation (USDT)
                liq_distance_usdt = float(abs(liquidation_price - mark_price))

                # Margin ratio
                if initial_margin > 0:
                    margin_ratio = float((maint_margin / initial_margin) * 100)
                else:
                    margin_ratio = 0.0

                # Risk level
                if liq_distance_pct < 5:
                    risk_level = "CRITICAL"
                    risk_emoji = "🚨"
                    recommendation = "IMMEDIATE ACTION REQUIRED - Add margin or close position now!"
                elif liq_distance_pct < 10:
                    risk_level = "HIGH"
                    risk_emoji = "⚠️"
                    recommendation = "Caution advised - Consider adding margin or reducing position size"
                elif liq_distance_pct < 20:
                    risk_level = "MEDIUM"
                    risk_emoji = "⚡"
                    recommendation = "Monitor regularly - Have stop-loss strategy in place"
                else:
                    risk_level = "LOW"
                    risk_emoji = "✓"
                    recommendation = "Position relatively safe - Continue normal monitoring"

                # Calculate additional margin for safety
                notional = abs(pos_amt * mark_price)
                current_margin = notional / leverage if leverage > 0 else notional
                safer_leverage = max(1, leverage / 2)
                safer_margin_needed = float((notional / safer_leverage) - current_margin)

                # Calculate max adverse price movement
                if direction == "LONG":
                    max_adverse_pct = float(((mark_price - liquidation_price) / mark_price) * 100)
                else:
                    max_adverse_pct = float(((liquidation_price - mark_price) / mark_price) * 100)

                risk_records.append({
                    'symbol': symbol_name,
                    'direction': direction,
                    'positionAmt': float(pos_amt),
                    'leverage': leverage,
                    'entryPrice': float(entry_price),
                    'markPrice': float(mark_price),
                    'liquidationPrice': float(liquidation_price),
                    'liqDistancePct': liq_distance_pct,
                    'liqDistanceUsdt': liq_distance_usdt,
                    'maxAdverseMovePct': max_adverse_pct,
                    'riskLevel': risk_level,
                    'riskEmoji': risk_emoji,
                    'unRealizedProfit': float(unrealized_pnl),
                    'notionalValue': float(notional),
                    'initialMargin': float(initial_margin),
                    'maintMargin': float(maint_margin),
                    'marginRatio': margin_ratio,
                    'saferMarginNeeded': safer_margin_needed,
                    'recommendation': recommendation,
                    'updateTime': datetime.fromtimestamp(int(position['updateTime']) / 1000).strftime('%Y-%m-%d %H:%M:%S')
                })

        # Create risk analysis DataFrame
        risk_df = pd.DataFrame(risk_records) if risk_records else pd.DataFrame(columns=[
            'symbol', 'direction', 'positionAmt', 'leverage', 'entryPrice', 'markPrice',
            'liquidationPrice', 'liqDistancePct', 'liqDistanceUsdt', 'maxAdverseMovePct',
            'riskLevel', 'riskEmoji', 'unRealizedProfit', 'notionalValue', 'initialMargin',
            'maintMargin', 'marginRatio', 'saferMarginNeeded', 'recommendation', 'updateTime'
        ])

        # Sort by risk level (most risky first)
        if not risk_df.empty:
            risk_order = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}
            risk_df['_risk_order'] = risk_df['riskLevel'].map(risk_order)
            risk_df = risk_df.sort_values(['_risk_order', 'liqDistancePct']).drop('_risk_order', axis=1)

        # Create summary DataFrame
        if not risk_df.empty:
            risk_counts = risk_df['riskLevel'].value_counts().to_dict()
            total_unrealized_pnl = risk_df['unRealizedProfit'].sum()
            total_notional = risk_df['notionalValue'].sum()
            total_margin = risk_df['initialMargin'].sum()
            avg_leverage = risk_df['leverage'].mean()
            avg_liq_distance = risk_df['liqDistancePct'].mean()

            summary_record = {
                'totalPositions': len(risk_df),
                'criticalRisk': risk_counts.get('CRITICAL', 0),
                'highRisk': risk_counts.get('HIGH', 0),
                'mediumRisk': risk_counts.get('MEDIUM', 0),
                'lowRisk': risk_counts.get('LOW', 0),
                'totalUnrealizedPnl': total_unrealized_pnl,
                'totalNotionalValue': total_notional,
                'totalMarginUsed': total_margin,
                'avgLeverage': avg_leverage,
                'avgLiqDistance': avg_liq_distance,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        else:
            summary_record = {
                'totalPositions': 0,
                'criticalRisk': 0,
                'highRisk': 0,
                'mediumRisk': 0,
                'lowRisk': 0,
                'totalUnrealizedPnl': 0.0,
                'totalNotionalValue': 0.0,
                'totalMarginUsed': 0.0,
                'avgLeverage': 0.0,
                'avgLiqDistance': 0.0,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

        summary_df = pd.DataFrame([summary_record])

        logger.info(f"Calculated liquidation risk for {len(risk_records)} positions")

        return risk_df, summary_df

    except Exception as e:
        logger.error(f"Error calculating liquidation risk: {e}")
        raise


def register_binance_calculate_liquidation_risk(local_mcp_instance, local_binance_client, csv_dir):
    """Register the binance_calculate_liquidation_risk tool"""
    @local_mcp_instance.tool()
    def binance_calculate_liquidation_risk(symbol: Optional[str] = None) -> str:
        """
        Calculate liquidation risk for futures positions and save detailed analysis to CSV.

        This tool provides comprehensive risk analysis for leveraged positions, including
        liquidation distance, risk levels, margin ratios, and actionable recommendations
        to protect your capital from liquidation.

        ✓ READ-ONLY OPERATION - Safe to run anytime

        Parameters:
            symbol (string, optional): Trading pair symbol (e.g., 'BTCUSDT')
                - If provided: Analyzes only this symbol
                - If omitted: Analyzes all open positions (recommended)

        Returns:
            str: Formatted response with two CSV files containing risk analysis and portfolio summary.

        CSV 1 - Risk Analysis Columns:
            - symbol (string): Trading pair
            - direction (string): LONG or SHORT
            - positionAmt (float): Position size
            - leverage (integer): Leverage multiplier
            - entryPrice (float): Entry price
            - markPrice (float): Current mark price
            - liquidationPrice (float): Liquidation price
            - liqDistancePct (float): Distance to liquidation in % (CRITICAL METRIC)
            - liqDistanceUsdt (float): Distance to liquidation in USDT
            - maxAdverseMovePct (float): Maximum adverse price movement before liquidation
            - riskLevel (string): Risk assessment (LOW, MEDIUM, HIGH, CRITICAL)
            - riskEmoji (string): Visual risk indicator
            - unRealizedProfit (float): Current unrealized P&L
            - notionalValue (float): Position notional value
            - initialMargin (float): Initial margin used
            - maintMargin (float): Maintenance margin required
            - marginRatio (float): Margin ratio percentage
            - saferMarginNeeded (float): Additional margin to halve leverage (improve safety)
            - recommendation (string): Actionable risk recommendation
            - updateTime (string): Position update timestamp

        CSV 2 - Portfolio Summary Columns:
            - totalPositions (integer): Total number of positions
            - criticalRisk (integer): Count of CRITICAL risk positions
            - highRisk (integer): Count of HIGH risk positions
            - mediumRisk (integer): Count of MEDIUM risk positions
            - lowRisk (integer): Count of LOW risk positions
            - totalUnrealizedPnl (float): Total unrealized P&L across all positions
            - totalNotionalValue (float): Total notional value of all positions
            - totalMarginUsed (float): Total margin used
            - avgLeverage (float): Average leverage across positions
            - avgLiqDistance (float): Average liquidation distance %
            - timestamp (string): Analysis timestamp

        Risk Level Definitions:
            - CRITICAL (<5% to liquidation): Extremely dangerous - Immediate action required!
                * Price can move very little before liquidation
                * Add margin NOW or close position
                * Do NOT wait - liquidation can happen any second

            - HIGH (5-10% to liquidation): Very risky - Caution strongly advised
                * Small price movement can cause liquidation
                * Consider adding margin or reducing position size
                * Set tight stop-loss immediately

            - MEDIUM (10-20% to liquidation): Moderate risk - Active monitoring needed
                * Reasonable buffer but still at risk in volatile markets
                * Have stop-loss strategy in place
                * Check position daily minimum

            - LOW (>20% to liquidation): Relatively safe - Normal monitoring
                * Good margin buffer
                * Continue normal position monitoring
                * Still be aware during major market events

        Key Metrics Explained:
            - Liquidation Distance %: Most important metric - lower = more dangerous
            - Max Adverse Move %: How much price can move against you before liquidation
            - Margin Ratio: Higher = More risk (>80% critical, >60% warning, <40% healthy)
            - Safer Margin Needed: Amount to add to halve leverage (improve safety 2x)

        Use Cases:
            - Daily risk assessment of futures portfolio
            - Pre-market volatility risk check
            - Identify which positions need attention
            - Calculate how much margin to add for safety
            - Portfolio risk monitoring and alerts
            - Prevent liquidations through early warning
            - Optimize margin allocation across positions

        Risk Management Actions:
            For CRITICAL Risk:
                1. Add margin immediately (use saferMarginNeeded as guide)
                2. Close part of position to reduce size
                3. Set stop-loss ASAP if not already set
                4. Monitor position constantly

            For HIGH Risk:
                1. Consider adding margin (saferMarginNeeded amount)
                2. Reduce leverage by closing partial position
                3. Ensure stop-loss is in place
                4. Monitor position every few hours

            For MEDIUM Risk:
                1. Have stop-loss strategy ready
                2. Check position daily
                3. Be prepared to add margin if needed
                4. Know liquidation price by heart

            For LOW Risk:
                1. Continue normal monitoring
                2. Still check during major market events
                3. Don't get complacent - markets can move fast

        Example usage:
            # Analyze all open positions (recommended)
            binance_calculate_liquidation_risk()

            # Analyze specific symbol
            binance_calculate_liquidation_risk(symbol="BTCUSDT")

        Analysis Tips with py_eval:
            - Sort by liqDistancePct to find most dangerous positions
            - Filter riskLevel == 'CRITICAL' for emergency positions
            - Sum saferMarginNeeded to know total margin needed for safety
            - Calculate weighted average risk by notionalValue
            - Track risk changes over time

        When to Run This Tool:
            - Every morning before market opens (daily routine)
            - Before major news events or economic data releases
            - When market volatility increases significantly
            - After opening new leveraged positions
            - When feeling uncertain about portfolio risk
            - Before going to sleep (for overnight positions)

        Portfolio Risk Warnings:
            - If ANY position shows CRITICAL: Take immediate action
            - If >50% positions are HIGH/CRITICAL: Portfolio at extreme risk
            - If avgLiqDistance <15%: Overall portfolio risk too high
            - If criticalRisk + highRisk >0: Prioritize those positions

        CRITICAL Safety Notes:
            - This tool does NOT modify positions - it only analyzes
            - Run frequently to stay aware of changing risk levels
            - Markets can move quickly - risk levels can change fast
            - Liquidation events happen in seconds during volatile periods
            - Better to be over-cautious than under-prepared
            - Prevention is 1000x better than dealing with liquidation

        Common Scenarios:
            - "All positions show LOW risk": Good! But still monitor regularly
            - "One CRITICAL position": Focus all attention on that position first
            - "Multiple HIGH risk": Consider closing some positions or adding margin
            - "No positions found": No open positions to analyze

        Note:
            - Completely safe READ-ONLY operation
            - Run as frequently as needed
            - Use with binance_manage_futures_positions to view full details
            - Use with binance_get_futures_balances to check available margin
            - Combine with price alerts for comprehensive risk management
        """
        logger.info(f"binance_calculate_liquidation_risk tool invoked")

        try:
            # Calculate risk
            risk_df, summary_df = calculate_liquidation_risk(
                binance_client=local_binance_client,
                symbol=symbol
            )

            # Generate filenames
            uid = str(uuid.uuid4())[:8]
            risk_filename = f"liquidation_risk_{uid}.csv"
            summary_filename = f"risk_summary_{uid}.csv"
            risk_filepath = csv_dir / risk_filename
            summary_filepath = csv_dir / summary_filename

            # Save to CSV files
            risk_df.to_csv(risk_filepath, index=False)
            summary_df.to_csv(summary_filepath, index=False)

            logger.info(f"Saved liquidation risk analysis to {risk_filename}")
            logger.info(f"Saved risk summary to {summary_filename}")

            # Build response
            risk_response = format_csv_response(risk_filepath, risk_df)
            summary_response = format_csv_response(summary_filepath, summary_df)

            if risk_df.empty:
                result = f"""═══════════════════════════════════════════════════════════════════════════════
LIQUIDATION RISK ANALYSIS
═══════════════════════════════════════════════════════════════════════════════

No open positions found.

Risk Summary:
{summary_response}
═══════════════════════════════════════════════════════════════════════════════
"""
                return result

            # Get summary data
            summary = summary_df.iloc[0]

            result = f"""═══════════════════════════════════════════════════════════════════════════════
LIQUIDATION RISK ANALYSIS
═══════════════════════════════════════════════════════════════════════════════

PORTFOLIO RISK SUMMARY:
{summary_response}

DETAILED RISK ANALYSIS (Sorted by Risk Level):
{risk_response}

═══════════════════════════════════════════════════════════════════════════════
PORTFOLIO RISK ASSESSMENT
═══════════════════════════════════════════════════════════════════════════════
Total Positions:         {int(summary['totalPositions'])}
Total Unrealized P&L:    {summary['totalUnrealizedPnl']:+,.2f} USDT
Total Margin Used:       {summary['totalMarginUsed']:,.2f} USDT
Average Leverage:        {summary['avgLeverage']:.1f}x
Avg Liq Distance:        {summary['avgLiqDistance']:.2f}%

Risk Breakdown:
"""

            if summary['criticalRisk'] > 0:
                result += f"  🚨 CRITICAL Risk:      {int(summary['criticalRisk'])} position(s)\n"
            if summary['highRisk'] > 0:
                result += f"  ⚠️  HIGH Risk:         {int(summary['highRisk'])} position(s)\n"
            if summary['mediumRisk'] > 0:
                result += f"  ⚡ MEDIUM Risk:        {int(summary['mediumRisk'])} position(s)\n"
            if summary['lowRisk'] > 0:
                result += f"  ✓  LOW Risk:          {int(summary['lowRisk'])} position(s)\n"

            result += "\n"

            # Add risk warnings
            if summary['criticalRisk'] > 0:
                result += "🚨 CRITICAL ALERT: You have position(s) at CRITICAL risk of liquidation!\n"
                result += "   IMMEDIATE ACTION REQUIRED:\n"
                result += "   1. Add margin to critical positions NOW\n"
                result += "   2. Or close/reduce critical positions immediately\n"
                result += "   3. Do NOT wait - liquidation can happen any second\n\n"

            if summary['highRisk'] > 0:
                result += "⚠️  HIGH RISK WARNING: You have position(s) at HIGH risk!\n"
                result += "   Recommended actions:\n"
                result += "   1. Consider adding margin to improve safety\n"
                result += "   2. Reduce position sizes if unable to add margin\n"
                result += "   3. Set tight stop-loss orders\n"
                result += "   4. Monitor these positions very closely\n\n"

            if summary['avgLiqDistance'] < 15 and summary['totalPositions'] > 0:
                result += "⚠️  PORTFOLIO WARNING: Average liquidation distance below 15%\n"
                result += "   Overall portfolio risk is elevated.\n"
                result += "   Consider reducing leverage or adding margin.\n\n"

            if summary['criticalRisk'] == 0 and summary['highRisk'] == 0 and summary['totalPositions'] > 0:
                result += "✓  Portfolio risk appears manageable.\n"
                result += "   Continue monitoring positions regularly.\n\n"

            result += "═══════════════════════════════════════════════════════════════════════════════\n"

            return result

        except Exception as e:
            logger.error(f"Error calculating liquidation risk: {e}")
            return f"Error: {str(e)}\n\nCheck:\n- API credentials valid\n- Futures account enabled\n- Network connectivity"
