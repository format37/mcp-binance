# Portfolio Performance Test

Please generate a portfolio performance report for the last 30 days.

## Expected Result
- An inline chart image showing actual vs hypothetical portfolio performance
- CSV files with equity curves, events, and metrics
- A clickable download URL for the full-resolution PNG chart
- Performance metrics comparing actual trading to buy-and-hold strategy

## Verification Steps
1. Call the `binance_portfolio_performance` tool with `days=30`
2. Verify the response includes an inline image preview
3. Verify the response includes CSV file information (equity, events, metrics)
4. Verify the response includes a download URL (if MCP_PUBLIC_BASE_URL is configured)
5. Test the download URL in a browser to confirm the PNG is accessible
