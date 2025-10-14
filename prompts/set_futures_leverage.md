NOTE: This tool modifies REAL futures leverage settings. Start with LOW leverage (2x-5x).

Example test request:
"Please check the current leverage settings for BTCUSDT futures and tell me:
1. What is the current leverage?
2. What is the margin type (CROSSED or ISOLATED)?
3. What is the maximum leverage available for BTCUSDT?

Then, if the current leverage is above 10x, please set it to 5x for safety. If it's already below 10x, just confirm the settings."

Advanced analysis with CSV:
"After checking the leverage settings, please use py_eval to read the CSV and calculate:
- How much safer would the position be with 5x vs current leverage
- What would be the new max notional value"
