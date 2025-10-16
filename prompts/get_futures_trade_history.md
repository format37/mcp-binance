Example test request (READ-ONLY - Safe):
"Please fetch my futures trade history for BTCUSDT and tell me:
1. How many trades have I executed?
2. What's my total realized P&L from these trades?
3. What's my total trading fees paid?
4. What's my net P&L (realized P&L minus fees)?
5. What percentage of my trades were maker vs taker?
6. What's my total trading volume in USDT?

Then use py_eval to analyze the CSV and:
- Calculate my average trade size
- Find my most profitable single trade
- Find my worst losing trade
- Show the date range of my trading activity
- Calculate daily P&L if possible"

NOTE: This is a READ-ONLY operation that safely retrieves your historical trade data for analysis.
