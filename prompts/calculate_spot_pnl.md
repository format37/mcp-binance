✓ READ-ONLY operation - Completely safe to run anytime

Example test request:
"Please calculate my spot trading P&L for the last 30 days and tell me:
1. What is my total realized P&L?
2. How much did I pay in trading fees?
3. What is my net P&L after fees?
4. Which trading pair was most profitable?
5. Which trading pair had the worst performance?
6. How many total trades did I execute?
7. What percentage of my P&L was consumed by fees?

Then use py_eval to analyze the CSV and:
- Calculate win rate (how many symbols had positive P&L vs total)
- Show top 5 most profitable symbols sorted by realizedPnl
- Show symbols where fees exceeded 20% of realized P&L"
