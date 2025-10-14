Example test request (READ-ONLY - Safe):
"Please list all my open futures positions and tell me:
1. How many positions are currently open?
2. Which position has the highest unrealized P&L?
3. Which position has the lowest unrealized P&L?
4. Are there any positions at HIGH or CRITICAL risk level?
5. What is the total unrealized P&L across all positions?
6. What is the average leverage across all positions?

Then use py_eval to analyze the CSV and:
- Sort positions by risk level to identify most dangerous positions
- Calculate total margin used
- Identify positions with ROI > 10% or < -10%"

NOTE: To actually CLOSE a position, use close_position=True. Only do this if explicitly requested and confirmed.
