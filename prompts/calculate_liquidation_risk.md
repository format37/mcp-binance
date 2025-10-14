✓ READ-ONLY operation - Completely safe to run anytime

Example test request:
"Please analyze liquidation risk for all my futures positions and tell me:
1. How many positions are at CRITICAL risk?
2. How many positions are at HIGH risk?
3. What is the average liquidation distance across all positions?
4. Which position is closest to liquidation?
5. For any CRITICAL or HIGH risk positions, how much additional margin is recommended?
6. What is the total unrealized P&L across all positions?

Then use py_eval to:
- Filter positions with liqDistancePct < 10%
- Calculate total saferMarginNeeded for all positions
- Show the top 3 riskiest positions sorted by liqDistancePct ascending"
