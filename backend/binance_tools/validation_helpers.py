"""
Helper functions for validating and adjusting trading parameters according to Binance symbol filters.

This module provides utilities to:
- Fetch symbol trading rules from Binance
- Validate and adjust quantities to meet LOT_SIZE requirements
- Provide helpful error messages when validation fails
"""

import logging
from decimal import Decimal, ROUND_DOWN
from binance.client import Client
from typing import Dict, Any, Tuple, Optional

logger = logging.getLogger(__name__)


def get_symbol_info(binance_client: Client, symbol: str) -> Dict[str, Any]:
    """
    Fetch symbol trading rules from Binance.

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT')

    Returns:
        Dictionary containing symbol information including filters

    Raises:
        Exception: If symbol info cannot be fetched
    """
    try:
        symbol_info = binance_client.get_symbol_info(symbol)
        if not symbol_info:
            raise ValueError(f"Symbol '{symbol}' not found on Binance")
        return symbol_info
    except Exception as e:
        logger.error(f"Failed to fetch symbol info for {symbol}: {e}")
        raise


def get_lot_size_filter(symbol_info: Dict[str, Any]) -> Dict[str, str]:
    """
    Extract LOT_SIZE filter from symbol info.

    Args:
        symbol_info: Symbol information dictionary from Binance

    Returns:
        Dictionary with 'minQty', 'maxQty', and 'stepSize' as strings

    Raises:
        ValueError: If LOT_SIZE filter not found
    """
    filters = symbol_info.get('filters', [])
    for f in filters:
        if f.get('filterType') == 'LOT_SIZE':
            return {
                'minQty': f['minQty'],
                'maxQty': f['maxQty'],
                'stepSize': f['stepSize']
            }
    raise ValueError(f"LOT_SIZE filter not found for symbol {symbol_info.get('symbol', 'unknown')}")


def round_step_size(quantity: float, step_size: str) -> float:
    """
    Round quantity to match the step size requirement.

    Args:
        quantity: Original quantity to round
        step_size: Step size from LOT_SIZE filter (as string)

    Returns:
        Rounded quantity as float

    Example:
        >>> round_step_size(0.001763, '0.00001')
        0.00176
        >>> round_step_size(0.03225, '0.0001')
        0.0322
    """
    quantity_decimal = Decimal(str(quantity))
    step_size_decimal = Decimal(step_size)

    # Calculate precision from step size
    # e.g., '0.00001' has precision 5, '0.0001' has precision 4
    precision = abs(step_size_decimal.as_tuple().exponent)

    # Round down to step size
    # Formula: floor(quantity / step_size) * step_size
    steps = (quantity_decimal / step_size_decimal).quantize(Decimal('1'), rounding=ROUND_DOWN)
    rounded = (steps * step_size_decimal).quantize(step_size_decimal)

    return float(rounded)


def validate_and_adjust_quantity(
    binance_client: Client,
    symbol: str,
    quantity: float
) -> Tuple[float, Optional[str]]:
    """
    Validate and adjust quantity according to symbol's LOT_SIZE filter.

    This function:
    1. Fetches symbol trading rules
    2. Rounds quantity to valid step size
    3. Validates against min/max quantity limits
    4. Returns adjusted quantity or error message

    Args:
        binance_client: Initialized Binance Client
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        quantity: Desired quantity

    Returns:
        Tuple of (adjusted_quantity, error_message)
        - If successful: (adjusted_quantity, None)
        - If failed: (original_quantity, error_message)

    Example:
        >>> adjusted_qty, error = validate_and_adjust_quantity(client, 'BTCUSDT', 0.001763)
        >>> if error:
        >>>     print(error)
        >>> else:
        >>>     print(f"Use quantity: {adjusted_qty}")  # 0.00176
    """
    try:
        # Fetch symbol info
        symbol_info = get_symbol_info(binance_client, symbol)
        lot_size = get_lot_size_filter(symbol_info)

        min_qty = Decimal(lot_size['minQty'])
        max_qty = Decimal(lot_size['maxQty'])
        step_size = lot_size['stepSize']

        # Round to valid step size
        adjusted_qty = round_step_size(quantity, step_size)
        adjusted_qty_decimal = Decimal(str(adjusted_qty))

        logger.info(f"Quantity validation for {symbol}: {quantity} -> {adjusted_qty} (step: {step_size})")

        # Validate against min quantity
        if adjusted_qty_decimal < min_qty:
            error_msg = f"""LOT_SIZE validation failed for {symbol}:
Your quantity: {quantity}
Adjusted quantity: {adjusted_qty}
Minimum quantity: {min_qty}

Your quantity is too small. Minimum is {min_qty}.
"""
            return quantity, error_msg

        # Validate against max quantity
        if adjusted_qty_decimal > max_qty:
            error_msg = f"""LOT_SIZE validation failed for {symbol}:
Your quantity: {quantity}
Maximum quantity: {max_qty}

Your quantity is too large. Maximum is {max_qty}.
"""
            return quantity, error_msg

        # Check if adjustment was significant
        if abs(quantity - adjusted_qty) / quantity > 0.01:  # More than 1% change
            logger.warning(
                f"Quantity adjusted by more than 1% for {symbol}: {quantity} -> {adjusted_qty}"
            )

        return adjusted_qty, None

    except Exception as e:
        error_msg = f"Failed to validate quantity for {symbol}: {str(e)}"
        logger.error(error_msg)
        return quantity, error_msg


def create_lot_size_error_message(
    symbol: str,
    original_qty: float,
    exception_message: str
) -> str:
    """
    Create a helpful error message for LOT_SIZE failures.

    Args:
        symbol: Trading pair symbol
        original_qty: The quantity that failed
        exception_message: The exception message from Binance

    Returns:
        Formatted error message with troubleshooting steps
    """
    return f"""LOT_SIZE filter error for {symbol}:

Your quantity: {original_qty}
Error: {exception_message}

Troubleshooting steps:
1. The quantity precision may be incorrect for this symbol
2. Try reducing the number of decimal places
3. Check minimum order size requirements
4. Verify the quantity meets the symbol's step size

The system attempted to auto-adjust the quantity, but the order still failed.
Please verify the symbol's trading rules using binance_get_ticker(symbol="{symbol}").
"""


def format_decimal(value: float) -> str:
    """
    Format a float value as a decimal string without scientific notation.

    This is critical for Binance API which requires numbers in decimal format
    matching regex: ^([0-9]{1,20})(\.[0-9]{1,20})?$

    Without this, small numbers like 0.00009 get converted to scientific
    notation (9e-05) which Binance API rejects with error -1100.

    Args:
        value: Float value to format

    Returns:
        String representation in decimal format (never scientific notation)

    Examples:
        >>> format_decimal(0.00009)
        '0.00009'
        >>> format_decimal(0.001)
        '0.001'
        >>> format_decimal(100.5)
        '100.5'
        >>> format_decimal(0.0000001)
        '0.0000001'
    """
    # Convert to Decimal to avoid scientific notation
    decimal_value = Decimal(str(value))

    # Format as string, stripping trailing zeros and unnecessary decimal point
    # normalize() converts to the shortest equivalent representation
    formatted = str(decimal_value.normalize())

    # If normalize() still produced scientific notation (very small/large numbers),
    # use a different approach
    if 'E' in formatted or 'e' in formatted:
        # Use fixed-point notation with sufficient decimal places
        formatted = format(decimal_value, 'f')

    return formatted
