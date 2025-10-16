Example test request (READ-ONLY - Safe):
"Please fetch my Binance futures account balance and positions, and tell me:
1. What is my total wallet balance in USDT?
2. How much unrealized P&L do I have across all positions?
3. What is my available balance for new positions?
4. What is my margin ratio and is it healthy?
5. How many open positions do I currently have?
6. What is my effective leverage across all positions?

Then use py_eval to analyze the CSVs and:
- Calculate the percentage of margin being used
- Identify my largest position by notional value
- Find any positions within 15% of liquidation price
- Show the distribution of my positions (long vs short)"

NOTE: This is a READ-ONLY operation that safely checks your futures account status without making any trades or modifications.
