import logging
from datetime import datetime
import pathlib
from request_logger import log_request
from sentry_utils import with_sentry_tracing

logger = logging.getLogger(__name__)


def register_trading_notes(local_mcp_instance, csv_dir, requests_dir):
    """Register trading notes tools for long-term strategy memory"""

    # Create trading_notes directory
    notes_dir = csv_dir / "trading_notes"
    notes_dir.mkdir(parents=True, exist_ok=True)

    # Main trading notes file
    trading_notes_file = notes_dir / "strategy.md"

    @local_mcp_instance.tool()
    @with_sentry_tracing("read_trading_notes")
    def read_trading_notes(requester: str) -> str:
        """
        Read current trading strategy and session notes.

        This tool retrieves the complete trading strategy document including:
        - Long-term trading plans and goals
        - Current positions and rationale
        - Risk management rules
        - Market analysis and outlook
        - Lessons learned from previous sessions
        - Important context for decision-making

        Use this tool at the start of each trading session to understand:
        - What positions were taken and why
        - What the overall strategy is
        - What market conditions are being monitored
        - What decisions are pending
        - What mistakes to avoid based on past experience

        Parameters:
            requester (string): The entity requesting this operation (e.g., 'user', 'claude', 'system')

        Returns:
            str: Full markdown content of trading notes, or a message if no notes exist

        Use Cases:
            - Session initialization: Read notes before making any trading decisions
            - Context restoration: Understand the portfolio state and strategy
            - Continuity: Connect current session with previous trading logic
            - Learning: Review past decisions and their outcomes
            - Risk management: Understand current risk exposure and limits

        Example usage:
            read_trading_notes()

        Note:
            - Returns the complete strategy document with all historical updates
            - Each update includes a timestamp showing when it was added
            - If no notes exist, returns instructions to create the first note
            - This is the first tool to call when starting a new trading session
        """
        logger.info(f"read_trading_notes tool invoked by {requester}")

        try:
            # Check if notes file exists
            if not trading_notes_file.exists():
                return """No trading notes found yet.

This is a fresh start. Use update_trading_notes() to create your first strategy document.

Consider including:
- Overall trading strategy and goals
- Risk management rules
- Current market outlook
- Portfolio allocation targets
- Monitoring criteria for positions"""

            # Read and return the content
            with open(trading_notes_file, 'r', encoding='utf-8') as f:
                content = f.read()

            logger.info(f"Read {len(content)} characters from trading notes")

            # Log request
            log_request(
                requests_dir=requests_dir,
                requester=requester,
                tool_name="read_trading_notes",
                input_params={},
                output_result=content
            )

            return content

        except Exception as e:
            logger.error(f"Error reading trading notes: {e}")
            return f"✗ Error reading trading notes: {str(e)}"

    @local_mcp_instance.tool()
    @with_sentry_tracing("update_trading_notes")
    def update_trading_notes(requester: str, markdown_content: str, append: bool = False) -> str:
        """
        Update trading strategy and session notes by REWRITING the entire document.

        ⚠️ IMPORTANT: This tool REPLACES the entire strategy document by default!

        Any previous content NOT included in markdown_content will be FORGOTTEN.
        Before calling this tool:
        1. Call read_trading_notes() to review current notes
        2. Identify what significant points to retain
        3. Include ALL important information in your new markdown_content:
           - Active positions and their rationale
           - Current trading strategy and goals
           - Risk management rules that are still relevant
           - Important lessons learned
           - Pending action items

        Only mention significant points worth remembering. Minor details from previous
        sessions can be omitted to keep the document focused and manageable.

        Parameters:
            requester (string): The entity requesting this operation (e.g., 'user', 'claude', 'system')
            markdown_content (str): Complete markdown strategy document. Must include:
                - ALL active positions with rationale and exit criteria
                - Current trading strategy and market outlook
                - Risk management rules and limits
                - Significant lessons learned worth remembering
                - Action items for next session
                - Any other important context for decision-making

            append (bool): If False (default), REPLACES entire document (prevents file bloat).
                          If True, appends with timestamp (use only for historical tracking).

        Returns:
            str: Confirmation message with timestamp and file location

        Use Cases:
            - Session updates: Rewrite with current positions, strategy, and key learnings
            - Strategy changes: Document new approach while preserving relevant context
            - Position management: Update positions list, removing closed trades
            - Regular cleanup: Keep document focused by dropping obsolete information

        Best Practices:
            - ALWAYS call read_trading_notes() first to see what exists
            - Include only SIGNIFICANT points from previous notes
            - Drop outdated information (closed positions, obsolete analysis)
            - Keep the document focused and actionable
            - Use append=True ONLY if you need to maintain complete history
            - Include specific details: prices, quantities, timeframes
            - Explain the "why" behind decisions, not just "what"

        Example usage:
            # First, read existing notes
            existing_notes = read_trading_notes()

            # Then rewrite with updated content, keeping significant points
            update_trading_notes(
                markdown_content=\"\"\"
                # Current Trading Strategy

                ## Active Positions

                ### BTC Long Position
                - **Opened:** 2025-01-15 at $42,500
                - **Size:** 0.1 BTC ($4,250 USD)
                - **Rationale:** Strong support at $42k, bullish RSI divergence on 4h
                - **Exit Plan:**
                  - Take profit: $45,000 (+5.9%)
                  - Stop loss: $41,000 (-3.5%)
                - **Risk:** 1.5% of portfolio

                ## Risk Management Rules
                - Maximum 2% risk per trade
                - No more than 3 concurrent positions
                - Daily loss limit: 5% of portfolio

                ## Key Lessons
                - Don't chase pumps - wait for confirmations
                - Always set stop losses before entering

                ## Next Session
                - Monitor BTC for take profit level
                - Review ETH setup if it breaks $2,300
                \"\"\",
                append=False  # Default: replaces entire document
            )

        Note:
            - This REWRITES the file to prevent unbounded growth
            - Previous content is DELETED unless you include it in markdown_content
            - Read existing notes first to decide what to keep
            - Only retain significant, actionable information
            - Use markdown formatting for readability
        """
        logger.info(f"update_trading_notes tool invoked by {requester} (append={append})")

        try:
            # Ensure directory exists (defensive check)
            notes_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if append and trading_notes_file.exists():
                # Append new content with timestamp separator
                entry = f"\n\n---\n**Updated:** {timestamp}\n\n{markdown_content}\n"

                with open(trading_notes_file, 'a', encoding='utf-8') as f:
                    f.write(entry)

                logger.info(f"Appended {len(markdown_content)} characters to trading notes")

                result = f"""✓ Trading notes updated successfully

Mode: Appended to existing notes
Timestamp: {timestamp}
File: trading_notes/strategy.md
Content size: {len(markdown_content)} characters

Use read_trading_notes() to view the complete strategy document."""

                # Log request
                log_request(
                    requests_dir=requests_dir,
                    requester=requester,
                    tool_name="update_trading_notes",
                    input_params={"append": append, "content_length": len(markdown_content)},
                    output_result=result
                )

                return result

            else:
                # Create or replace entire file
                header = f"# Trading Strategy & Session Notes\n\n**Created:** {timestamp}\n\n"
                full_content = header + markdown_content + "\n"

                with open(trading_notes_file, 'w', encoding='utf-8') as f:
                    f.write(full_content)

                mode = "Created new" if not trading_notes_file.exists() else "Replaced"
                logger.info(f"{mode} trading notes file with {len(full_content)} characters")

                result = f"""✓ Trading notes {mode.lower()} successfully

Mode: {mode} strategy document
Timestamp: {timestamp}
File: trading_notes/strategy.md
Content size: {len(markdown_content)} characters

Use read_trading_notes() to view the strategy document."""

                # Log request
                log_request(
                    requests_dir=requests_dir,
                    requester=requester,
                    tool_name="update_trading_notes",
                    input_params={"append": append, "content_length": len(markdown_content)},
                    output_result=result
                )

                return result

        except Exception as e:
            logger.error(f"Error updating trading notes: {e}")
            return f"✗ Error updating trading notes: {str(e)}"
