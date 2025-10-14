"""
Sentry utilities for performance monitoring and error tracking in MCP tools.

This module provides decorators and utilities to automatically instrument MCP tools
with Sentry transaction tracing and profiling.
"""

import functools
import logging
import sentry_sdk
from typing import Callable, Any

logger = logging.getLogger(__name__)


def with_sentry_tracing(operation_name: str):
    """
    Decorator to wrap MCP tool functions with Sentry transaction tracing.

    This decorator automatically:
    - Creates a Sentry transaction for the tool execution
    - Captures execution time and performance metrics
    - Adds custom tags for filtering and analysis
    - Captures exceptions with full context
    - Completes transaction with success/failure status

    Args:
        operation_name: Name of the operation (e.g., "binance_get_account", "binance_get_ticker")

    Usage:
        @with_sentry_tracing("binance_get_account")
        def fetch_account(binance_client: Client) -> pd.DataFrame:
            # Your code here
            pass

    Note:
        The decorator will extract common parameters (like 'symbol') from function
        arguments and add them as tags for better filtering in Sentry.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Start Sentry transaction
            with sentry_sdk.start_transaction(op="tool.execution", name=operation_name) as transaction:
                # Add operation tag
                sentry_sdk.set_tag("tool_name", operation_name)

                # Extract and tag common parameters
                if 'symbol' in kwargs:
                    sentry_sdk.set_tag("symbol", kwargs['symbol'])
                if 'limit' in kwargs:
                    sentry_sdk.set_tag("limit", kwargs['limit'])

                # Add breadcrumb for tool invocation
                sentry_sdk.add_breadcrumb(
                    category="tool.invocation",
                    message=f"Executing {operation_name}",
                    level="info",
                    data={"kwargs": str(kwargs) if kwargs else "{}"}
                )

                try:
                    # Execute the wrapped function
                    result = func(*args, **kwargs)

                    # Mark transaction as successful
                    transaction.set_status("ok")

                    # Add success breadcrumb
                    sentry_sdk.add_breadcrumb(
                        category="tool.completion",
                        message=f"{operation_name} completed successfully",
                        level="info"
                    )

                    return result

                except Exception as e:
                    # Mark transaction as failed
                    transaction.set_status("internal_error")

                    # Add error context
                    sentry_sdk.set_context("error_details", {
                        "tool_name": operation_name,
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    })

                    # Capture exception in Sentry
                    sentry_sdk.capture_exception(e)

                    # Add failure breadcrumb
                    sentry_sdk.add_breadcrumb(
                        category="tool.error",
                        message=f"{operation_name} failed: {str(e)}",
                        level="error"
                    )

                    # Log the error
                    logger.error(f"Error in {operation_name}: {e}", exc_info=True)

                    # Re-raise the exception to maintain original behavior
                    raise

        return wrapper
    return decorator
