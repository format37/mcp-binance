Example test request (⚠️ DANGEROUS - REAL TRADING):
"I want to execute a futures market order. Before executing:
1. Check my futures account balance to confirm available margin
2. Calculate the position size based on 2% risk of my available balance
3. Execute a BUY market order for BTCUSDT with quantity 0.001 BTC, position side LONG

After execution:
- Show me the order execution details (order ID, avg price, executed quantity)
- Display my new position details (entry price, liquidation price, unrealized P&L)
- Calculate my distance to liquidation in percentage
- Tell me my current leverage on this position

Then use py_eval to:
- Verify the order was filled completely
- Calculate the notional value of my position
- Estimate the margin used for this trade"

⚠️ EXTREME RISK WARNING ⚠️
This executes REAL TRADES with leverage! Losses can EXCEED your investment!
Only use this with explicit user confirmation and proper risk management!

SAFER ALTERNATIVE (for testing CSV reading capability):
"Please show me the structure of a futures market order by reading the trade_futures_market.py
file and explaining what parameters it accepts and what CSV columns it returns."
