Example test request (⚠️ DANGEROUS - CANCELS REAL ORDERS):
"I want to cancel a futures order. First:
1. Show me all my open futures orders for BTCUSDT
2. Identify which order I want to cancel by its order ID

Then cancel the specific order:
- Use the order ID from step 1
- Cancel that single order

After cancellation:
- Confirm the order was cancelled
- Check open orders again to verify it's gone
- Show my freed margin in futures account balance"

⚠️ WARNING: This CANCELS REAL ORDERS!
Only use after verifying the correct order_id.

SAFER ALTERNATIVE (for understanding):
"Please explain the futures order cancellation process by reading cancel_futures_order.py:
1. What's the difference between cancelling a single order and cancel_all?
2. What happens to margin when I cancel an order?
3. Can I cancel partially filled orders?
4. What are the safety considerations?"
