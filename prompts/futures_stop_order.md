# Futures Stop Order Tool - Test Prompts

## Example 1: STOP_MARKET (Stop-Loss) Test

"I have an open LONG position on BTCUSDT. Please help me set up a stop-loss:
1. First check my current futures positions to see my entry price
2. Set a STOP_MARKET order at 5% below my entry price to protect against loss
3. Use close_position=True to close the entire position if triggered

After placing the order:
- Show me the order ID and status
- Explain when this stop-loss will trigger
- Tell me how to check or cancel this order later"


## Example 2: TAKE_PROFIT_MARKET Test

"I want to set a take-profit order for my LONG ETHUSDT position:
1. Check my current position entry price
2. Place a TAKE_PROFIT_MARKET order at 10% above entry to lock in profits
3. Use close_position=True

Show me how to verify the order is active and waiting."


## Example 3: TRAILING_STOP_MARKET Test

"Set up a trailing stop for my BTCUSDT LONG position:
1. Use callback_rate=2.0 (2% trailing distance)
2. Set activation_price at current market price (so it activates immediately)
3. Use close_position=True

Explain:
- How the trailing stop follows the price
- When it will trigger
- The difference between activation_price and callback_rate"


## Example 4: Complete Risk Management Setup

"I just opened a LONG position on BTCUSDT. Help me set up complete risk management:
1. Check my entry price first
2. Place a STOP_MARKET at 5% below entry (stop-loss)
3. Place a TAKE_PROFIT_MARKET at 15% above entry (take-profit)

Use py_eval to:
- Read both order CSVs
- Display a summary table of my active protection orders
- Calculate the risk/reward ratio"


## SAFE Testing (No Real Orders)

"Please explain the binance_futures_stop_order tool by reading the source code:
1. What are the three order types supported?
2. Explain the difference between STOP_MARKET and TAKE_PROFIT_MARKET trigger logic
3. How does TRAILING_STOP_MARKET work with callback_rate and activation_price?
4. What does working_type MARK_PRICE vs CONTRACT_PRICE mean?
5. Why is reduceOnly set to True by default?
6. Show me example parameter combinations for:
   - Protecting a LONG position
   - Protecting a SHORT position
   - Setting a trailing stop that activates at a specific price"


## CSV Reading Verification

"After placing a stop order, use py_eval to:
1. Read the CSV file path returned
2. Display all columns and their values
3. Explain what each field means
4. Show how to filter open stop orders by symbol"

---

RISK WARNING: Stop-loss orders help limit losses but are NOT guaranteed.
In fast-moving markets, slippage may occur and execution price may differ from stop price.
Always monitor your positions and understand that futures trading involves liquidation risk.
