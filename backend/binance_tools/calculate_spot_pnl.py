import logging
from datetime import datetime, timedelta
from decimal import Decimal
from collections import defaultdict
import uuid
from mcp_service import format_csv_response
import pandas as pd
from binance.client import Client
from typing import Optional
from sentry_utils import with_sentry_tracing

logger = logging.getLogger(__name__)


def get_asset_price_in_usdt(binance_client: Client, asset: str) -> Optional[Decimal]:
    """
    Get current price of asset in USDT.

    Args:
        binance_client: Initialized Binance Client
        asset: Asset symbol (e.g., 'BTC', 'ETH')

    Returns:
        Price in USDT or None if not available
    """
    stablecoins = ['USDT', 'BUSD', 'USDC', 'TUSD', 'USDP', 'FDUSD']

    if asset in stablecoins:
        return Decimal('1.0')

    symbol = f"{asset}USDT"
    try:
        ticker = binance_client.get_symbol_ticker(symbol=symbol)
        return Decimal(ticker['price'])
    except:
        return None


@with_sentry_tracing("binance_calculate_spot_pnl")
def calculate_spot_pnl(binance_client: Client, symbol: Optional[str] = None,
                       days: Optional[int] = None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Calculate P&L for spot trading by analyzing trade history.

    Args:
        binance_client: Initialized Binance Client
        symbol: Optional specific trading pair to analyze
        days: Optional number of days to look back (None = all history)

    Returns:
        Tuple of (pnl_by_symbol_df, fee_summary_df, overall_summary_df):
        - pnl_by_symbol_df: P&L breakdown by symbol
        - fee_summary_df: Trading fees by asset
        - overall_summary_df: Overall P&L summary

    Note:
        This is a READ-ONLY operation analyzing historical trades.
    """
    logger.info(f"Calculating spot P&L" + (f" for {symbol}" if symbol else " for all symbols"))

    try:
        # Calculate time range
        if days:
            start_time = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
        else:
            start_time = None

        # Get all trades
        all_trades = []
        total_commission = defaultdict(Decimal)

        if symbol:
            symbols = [symbol]
        else:
            # Get all symbols user has traded
            account = binance_client.get_account()
            symbols = []
            for balance in account['balances']:
                if Decimal(balance['free']) > 0 or Decimal(balance['locked']) > 0:
                    asset = balance['asset']
                    # Try common quote currencies
                    for quote in ['USDT', 'BUSD', 'USDC', 'BTC', 'ETH', 'BNB']:
                        if asset != quote:
                            symbols.append(f"{asset}{quote}")

        # Collect trade data
        for sym in symbols:
            try:
                params = {'symbol': sym, 'limit': 1000}
                if start_time:
                    params['startTime'] = start_time

                trades = binance_client.get_my_trades(**params)

                if not trades:
                    continue

                for trade in trades:
                    all_trades.append({
                        'symbol': sym,
                        'time': datetime.fromtimestamp(trade['time'] / 1000),
                        'price': Decimal(trade['price']),
                        'qty': Decimal(trade['qty']),
                        'quoteQty': Decimal(trade['quoteQty']),
                        'commission': Decimal(trade['commission']),
                        'commissionAsset': trade['commissionAsset'],
                        'isBuyer': trade['isBuyer'],
                        'isMaker': trade['isMaker']
                    })

                    # Track commissions
                    total_commission[trade['commissionAsset']] += Decimal(trade['commission'])

            except Exception as e:
                # Symbol might not exist or no trades
                logger.debug(f"Skipping {sym}: {e}")
                continue

        if not all_trades:
            # Return empty DataFrames
            pnl_df = pd.DataFrame(columns=[
                'symbol', 'buyCount', 'sellCount', 'avgBuyPrice', 'avgSellPrice',
                'totalBought', 'totalSold', 'realizedPnl', 'pnlPercent'
            ])
            fee_df = pd.DataFrame(columns=['asset', 'amount', 'valueUsdt'])
            summary_df = pd.DataFrame([{
                'totalTrades': 0,
                'symbolsTraded': 0,
                'realizedPnl': 0.0,
                'totalFeesUsdt': 0.0,
                'netPnl': 0.0,
                'timeRange': 'No trades found',
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }])
            return pnl_df, fee_df, summary_df

        # Sort trades by time
        all_trades.sort(key=lambda x: x['time'])

        # Calculate P&L by symbol
        symbol_stats = defaultdict(lambda: {
            'total_bought': Decimal('0'),
            'total_spent': Decimal('0'),
            'total_sold': Decimal('0'),
            'total_received': Decimal('0'),
            'trade_count': 0,
            'buy_count': 0,
            'sell_count': 0
        })

        for trade in all_trades:
            sym = trade['symbol']
            stats = symbol_stats[sym]
            stats['trade_count'] += 1

            if trade['isBuyer']:
                stats['buy_count'] += 1
                stats['total_bought'] += trade['qty']
                stats['total_spent'] += trade['quoteQty']
            else:
                stats['sell_count'] += 1
                stats['total_sold'] += trade['qty']
                stats['total_received'] += trade['quoteQty']

        # Build P&L records
        pnl_records = []
        total_realized_pnl = Decimal('0')

        for sym, stats in sorted(symbol_stats.items()):
            avg_buy_price = stats['total_spent'] / stats['total_bought'] if stats['total_bought'] > 0 else Decimal('0')
            avg_sell_price = stats['total_received'] / stats['total_sold'] if stats['total_sold'] > 0 else Decimal('0')

            # Calculate realized P&L (from completed buy-sell cycles)
            min_qty = min(stats['total_bought'], stats['total_sold'])
            if min_qty > 0:
                realized_pnl = (avg_sell_price - avg_buy_price) * min_qty
                pnl_percent = ((avg_sell_price - avg_buy_price) / avg_buy_price * 100) if avg_buy_price > 0 else Decimal('0')
            else:
                realized_pnl = Decimal('0')
                pnl_percent = Decimal('0')

            total_realized_pnl += realized_pnl

            pnl_records.append({
                'symbol': sym,
                'buyCount': stats['buy_count'],
                'sellCount': stats['sell_count'],
                'avgBuyPrice': float(avg_buy_price),
                'avgSellPrice': float(avg_sell_price) if avg_sell_price > 0 else None,
                'totalBought': float(stats['total_bought']),
                'totalSold': float(stats['total_sold']),
                'realizedPnl': float(realized_pnl),
                'pnlPercent': float(pnl_percent)
            })

        pnl_df = pd.DataFrame(pnl_records)

        # Build fee records
        fee_records = []
        total_fees_usdt = Decimal('0')

        for asset, amount in total_commission.items():
            if amount > 0:
                # Convert to USDT
                price_usdt = get_asset_price_in_usdt(binance_client, asset)
                if price_usdt:
                    fee_usdt = amount * price_usdt
                    total_fees_usdt += fee_usdt
                    fee_records.append({
                        'asset': asset,
                        'amount': float(amount),
                        'valueUsdt': float(fee_usdt)
                    })
                else:
                    fee_records.append({
                        'asset': asset,
                        'amount': float(amount),
                        'valueUsdt': None
                    })

        fee_df = pd.DataFrame(fee_records) if fee_records else pd.DataFrame(columns=['asset', 'amount', 'valueUsdt'])

        # Build overall summary
        net_pnl = total_realized_pnl - total_fees_usdt

        if days:
            time_range = f"Last {days} days"
        else:
            time_range = f"{all_trades[0]['time'].strftime('%Y-%m-%d')} to {all_trades[-1]['time'].strftime('%Y-%m-%d')}"

        summary_record = {
            'totalTrades': len(all_trades),
            'symbolsTraded': len(symbol_stats),
            'realizedPnl': float(total_realized_pnl),
            'totalFeesUsdt': float(total_fees_usdt),
            'netPnl': float(net_pnl),
            'timeRange': time_range,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        summary_df = pd.DataFrame([summary_record])

        logger.info(f"Calculated P&L for {len(pnl_records)} symbols with {len(all_trades)} total trades")

        return pnl_df, fee_df, summary_df

    except Exception as e:
        logger.error(f"Error calculating spot P&L: {e}")
        raise


def register_binance_calculate_spot_pnl(local_mcp_instance, local_binance_client, csv_dir):
    """Register the binance_calculate_spot_pnl tool"""
    @local_mcp_instance.tool()
    def binance_calculate_spot_pnl(symbol: Optional[str] = None, days: Optional[int] = None) -> str:
        """
        Calculate profit/loss for spot trading by analyzing trade history and save to CSV.

        This tool provides comprehensive P&L analysis by examining your completed trades,
        calculating realized gains/losses, tracking fees, and computing net profitability
        across your spot trading portfolio.

        ✓ READ-ONLY OPERATION - Completely safe to run anytime

        Parameters:
            symbol (string, optional): Trading pair to analyze (e.g., 'BTCUSDT')
                - If provided: Analyzes only this trading pair
                - If omitted: Analyzes all traded pairs (recommended)
            days (integer, optional): Number of days to look back
                - If provided: Analyzes trades from last N days
                - If omitted: Analyzes entire trade history

        Returns:
            str: Formatted response with three CSV files containing P&L analysis, fees, and summary.

        CSV 1 - P&L by Symbol Columns:
            - symbol (string): Trading pair (e.g., 'BTCUSDT')
            - buyCount (integer): Number of buy trades
            - sellCount (integer): Number of sell trades
            - avgBuyPrice (float): Average buy price
            - avgSellPrice (float): Average sell price (None if no sells)
            - totalBought (float): Total quantity bought
            - totalSold (float): Total quantity sold
            - realizedPnl (float): Realized profit/loss in quote currency
            - pnlPercent (float): P&L percentage

        CSV 2 - Trading Fees Columns:
            - asset (string): Fee asset (e.g., 'BNB', 'USDT')
            - amount (float): Fee amount in that asset
            - valueUsdt (float): Fee value in USDT (None if conversion unavailable)

        CSV 3 - Overall Summary Columns:
            - totalTrades (integer): Total number of trades
            - symbolsTraded (integer): Number of different symbols traded
            - realizedPnl (float): Total realized P&L in USDT
            - totalFeesUsdt (float): Total fees paid in USDT
            - netPnl (float): Net P&L after fees (realizedPnl - fees)
            - timeRange (string): Time period analyzed
            - timestamp (string): Analysis timestamp

        P&L Calculation Method:
            - Realized P&L: Calculated from completed buy-sell cycles only
            - Averages weighted by quantity traded
            - Unrealized P&L NOT included (current holdings not yet sold)
            - Fees converted to USDT when possible for accurate net calculation

        Understanding the Results:
            - Positive realizedPnl = Profitable trading on that symbol
            - Negative realizedPnl = Loss on that symbol
            - pnlPercent shows return percentage relative to buy price
            - netPnl is the final profit/loss after subtracting all fees

        Use Cases:
            - Track trading performance over time
            - Analyze profitability by symbol
            - Calculate total trading fees paid
            - Evaluate trading strategy effectiveness
            - Tax reporting preparation (consult tax professional)
            - Identify most/least profitable trades
            - Portfolio performance review
            - Compare different trading pairs

        Analysis Tips:
            - Run monthly to track performance trends
            - Use days parameter for recent performance (e.g., days=30)
            - Compare P&L across different time periods
            - Identify symbols consistently profitable or unprofitable
            - Calculate win rate by counting profitable vs unprofitable symbols
            - Use py_eval for advanced analysis and visualization

        Example usage:
            # Calculate P&L for all trades, all time
            binance_calculate_spot_pnl()

            # Calculate P&L for specific symbol
            binance_calculate_spot_pnl(symbol="BTCUSDT")

            # Calculate P&L for last 30 days
            binance_calculate_spot_pnl(days=30)

            # Calculate P&L for specific symbol in last 7 days
            binance_calculate_spot_pnl(symbol="ETHUSDT", days=7)

        Advanced Analysis with py_eval:
            - Sort by realizedPnl to find best/worst performers
            - Calculate total P&L by quote currency (USDT, BTC, etc.)
            - Compute win rate (profitable symbols / total symbols)
            - Analyze fee efficiency (fees / realizedPnl ratio)
            - Track P&L trends over time with multiple runs
            - Compare performance across different market conditions

        Limitations and Notes:
            - Only includes REALIZED P&L (completed buy-sell cycles)
            - Current holdings (unrealized) NOT included
            - Averages may not reflect exact FIFO/LIFO accounting
            - For precise tax calculations, consult a tax professional
            - API limit: 1000 most recent trades per symbol
            - Very active traders may not see complete history
            - Fee conversions depend on current market prices

        Tax Considerations:
            - This tool provides estimates only
            - Different jurisdictions have different rules
            - FIFO, LIFO, or specific identification may apply
            - Consult qualified tax professional for tax reporting
            - Keep trade records for compliance
            - Consider tax implications before year-end trading

        When to Run This Tool:
            - End of month for monthly performance review
            - Before tax season for preliminary calculations
            - After significant trading period to assess strategy
            - When evaluating different trading pairs
            - To decide which symbols to continue trading
            - For portfolio performance reports

        Interpreting Results:
            High netPnl = Successful trading strategy
            Low/negative netPnl = Strategy needs improvement
            High fees relative to P&L = Consider fee optimization
            Consistent losses on symbol = Avoid or change approach
            Mixed results = Refine symbol selection

        Performance Optimization:
            - Focus on symbols with consistent positive P&L
            - Reduce trading on consistently unprofitable symbols
            - Consider fee structure (maker/taker, BNB discounts)
            - Evaluate if fees are eating into profits
            - Track performance metrics over time for improvement

        Note:
            - Completely safe READ-ONLY analysis
            - Does not modify any positions or execute trades
            - Can be run as frequently as needed
            - CSV files saved for further analysis and record-keeping
            - Combine with portfolio tracking for comprehensive view
        """
        logger.info(f"binance_calculate_spot_pnl tool invoked")

        try:
            # Calculate P&L
            pnl_df, fee_df, summary_df = calculate_spot_pnl(
                binance_client=local_binance_client,
                symbol=symbol,
                days=days
            )

            # Generate filenames
            uid = str(uuid.uuid4())[:8]
            pnl_filename = f"spot_pnl_{uid}.csv"
            fee_filename = f"spot_fees_{uid}.csv"
            summary_filename = f"pnl_summary_{uid}.csv"

            pnl_filepath = csv_dir / pnl_filename
            fee_filepath = csv_dir / fee_filename
            summary_filepath = csv_dir / summary_filename

            # Save to CSV files
            pnl_df.to_csv(pnl_filepath, index=False)
            fee_df.to_csv(fee_filepath, index=False)
            summary_df.to_csv(summary_filepath, index=False)

            logger.info(f"Saved spot P&L analysis to {pnl_filename}, {fee_filename}, {summary_filename}")

            # Build response
            pnl_response = format_csv_response(pnl_filepath, pnl_df)
            fee_response = format_csv_response(fee_filepath, fee_df)
            summary_response = format_csv_response(summary_filepath, summary_df)

            summary = summary_df.iloc[0]

            if summary['totalTrades'] == 0:
                result = f"""═══════════════════════════════════════════════════════════════════════════════
SPOT TRADING P&L ANALYSIS
═══════════════════════════════════════════════════════════════════════════════

No trades found in the specified time period.

Summary:
{summary_response}
═══════════════════════════════════════════════════════════════════════════════
"""
                return result

            result = f"""═══════════════════════════════════════════════════════════════════════════════
SPOT TRADING P&L ANALYSIS
═══════════════════════════════════════════════════════════════════════════════

OVERALL SUMMARY:
{summary_response}

P&L BY SYMBOL:
{pnl_response}

TRADING FEES:
{fee_response}

═══════════════════════════════════════════════════════════════════════════════
PERFORMANCE SUMMARY
═══════════════════════════════════════════════════════════════════════════════
Time Period:         {summary['timeRange']}
Total Trades:        {int(summary['totalTrades'])}
Symbols Traded:      {int(summary['symbolsTraded'])}

Realized P&L:        ${summary['realizedPnl']:,.2f}
Trading Fees:        ${summary['totalFeesUsdt']:,.2f}
═══════════════════════════════════════════════════════════════════════════════
NET P&L:             ${summary['netPnl']:+,.2f}
═══════════════════════════════════════════════════════════════════════════════

"""

            # Add performance assessment
            net_pnl = summary['netPnl']
            if net_pnl > 0:
                result += f"✓  Net Profit: ${net_pnl:,.2f}\n"
                result += "   Trading strategy showing positive returns.\n"
            elif net_pnl < 0:
                result += f"✗  Net Loss: ${abs(net_pnl):,.2f}\n"
                result += "   Consider reviewing trading strategy and risk management.\n"
            else:
                result += "=  Break Even: $0.00\n"
                result += "   Trading at break-even point.\n"

            result += "\n"

            # Fee analysis
            if summary['realizedPnl'] != 0:
                fee_pct = (summary['totalFeesUsdt'] / abs(summary['realizedPnl'])) * 100
                result += f"Fee Impact:          {fee_pct:.2f}% of realized P&L\n"

                if fee_pct > 50:
                    result += "⚠️  Fees are consuming significant portion of profits!\n"
                    result += "   Consider optimizing fee structure (maker orders, BNB discounts).\n"

            result += "\n"
            result += "═══════════════════════════════════════════════════════════════════════════════\n"
            result += "NOTES:\n"
            result += "- Realized P&L: Profit/loss from completed buy-sell cycles\n"
            result += "- Unrealized P&L: Not included (current holdings not sold)\n"
            result += "- This is an estimate based on trade history\n"
            result += "- For tax purposes, consult with a qualified tax professional\n"
            result += "═══════════════════════════════════════════════════════════════════════════════\n"

            return result

        except Exception as e:
            logger.error(f"Error calculating spot P&L: {e}")
            return f"Error: {str(e)}\n\nCheck:\n- API credentials valid\n- Trade history available\n- Network connectivity"
