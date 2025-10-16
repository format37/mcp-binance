Example test request (READ-ONLY - Safe):
"Please fetch all my open futures orders and tell me:
1. How many open futures orders do I currently have?
2. Are there any partially filled orders?
3. Which order types do I have open (LIMIT, STOP, etc.)?
4. Which symbols have open orders?
5. What's the total number of orders by position side (LONG vs SHORT)?

Then use py_eval to analyze the CSV and:
- Show the oldest order (earliest time)
- List all orders with their limit prices
- Calculate how much margin is locked in these orders
- Identify any orders that might need cancellation (stale orders)"

NOTE: This is a READ-ONLY operation that safely checks your open futures orders without making any changes.
