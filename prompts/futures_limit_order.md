Example test request (⚠️ DANGEROUS - REAL TRADING):
"I want to place a futures limit order. Before executing:
1. Check my futures account balance to confirm available margin
2. Check current BTCUSDT price to set appropriate limit price
3. Place a BUY limit order for BTCUSDT:
   - Quantity: 0.001 BTC
   - Limit Price: [5% below current market price]
   - Position Side: LONG
   - Time in Force: GTC

After execution:
- Show me the order ID and status
- Verify if the order is active (status should be NEW if not filled immediately)
- Explain when this order will execute

Then use py_eval to:
- Read the CSV and display the order details
- Calculate the notional value (quantity * price)
- Show how to monitor this order"

⚠️ EXTREME RISK WARNING ⚠️
This executes REAL TRADES with leverage! Only use with explicit confirmation!

SAFER ALTERNATIVE (for testing CSV reading capability):
"Please show me the structure of a futures limit order by reading the futures_limit_order.py
file and explaining:
1. What parameters does it accept?
2. What's the difference between GTC, IOC, FOK, and GTX time_in_force?
3. What CSV columns does it return?
4. When would I use reduce_only=True?"
