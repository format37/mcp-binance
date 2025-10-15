import logging
from datetime import datetime
import pathlib
from sentry_utils import with_sentry_tracing

logger = logging.getLogger(__name__)


def register_trading_notes(local_mcp_instance, csv_dir):
    """Register trading notes tools for long-term strategy memory"""

    # Create trading_notes directory
    notes_dir = csv_dir / "trading_notes"
    notes_dir.mkdir(parents=True, exist_ok=True)

    # Main trading notes file
    trading_notes_file = notes_dir / "strategy.md"

    @local_mcp_instance.tool()
    @with_sentry_tracing("read_trading_notes")
    def read_trading_notes() -> str:
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
        logger.info("read_trading_notes tool invoked")

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

            return content

        except Exception as e:
            logger.error(f"Error reading trading notes: {e}")
            return f"✗ Error reading trading notes: {str(e)}"

    @local_mcp_instance.tool()
    @with_sentry_tracing("update_trading_notes")
    def update_trading_notes(markdown_content: str, append: bool = True) -> str:
        """
        Update trading strategy and session notes.

        This tool saves or appends trading strategy information that persists across sessions.
        Use it to maintain long-term context about your trading approach, current positions,
        market outlook, and lessons learned.

        Parameters:
            markdown_content (str): Markdown-formatted trading notes. Should include:
                - Strategic decisions made and rationale
                - Position updates (opened/closed positions with reasoning)
                - Market analysis and outlook changes
                - Risk management updates
                - Lessons learned or important observations
                - Action items for next session

            append (bool): If True (default), appends content with timestamp.
                          If False, replaces entire notes file with new content.

        Returns:
            str: Confirmation message with timestamp and file location

        Use Cases:
            - After opening a position: Document the trade rationale and exit criteria
            - After closing a position: Record the outcome and lessons learned
            - After market analysis: Update outlook and adjust strategy if needed
            - At end of session: Summarize actions taken and next steps
            - When changing strategy: Document why the approach is being modified
            - Risk management: Update exposure limits or risk parameters

        Best Practices:
            - Use append=True for most updates to maintain history
            - Use append=False only for complete strategy rewrites
            - Include specific details: prices, quantities, timeframes
            - Explain the "why" behind decisions, not just "what"
            - Note what you're monitoring for position management
            - Flag important learnings or mistakes to avoid repeating

        Example usage:
            update_trading_notes(
                markdown_content=\"\"\"
                # Position Update: BTC Long

                **Action:** Opened long BTC position at $42,500
                **Size:** 0.1 BTC ($4,250 USD)
                **Rationale:** Strong support at $42k, bullish RSI divergence on 4h chart
                **Exit Plan:**
                - Take profit: $45,000 (+5.9%)
                - Stop loss: $41,000 (-3.5%)

                **Risk:** 1.5% of portfolio
                \"\"\",
                append=True
            )

        Note:
            - Each appended update includes a timestamp automatically
            - Updates are saved immediately to persist across sessions
            - Use markdown formatting for better readability
            - Keep notes focused on actionable information and context
        """
        logger.info(f"update_trading_notes tool invoked (append={append})")

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

                return f"""✓ Trading notes updated successfully

Mode: Appended to existing notes
Timestamp: {timestamp}
File: trading_notes/strategy.md
Content size: {len(markdown_content)} characters

Use read_trading_notes() to view the complete strategy document."""

            else:
                # Create or replace entire file
                header = f"# Trading Strategy & Session Notes\n\n**Created:** {timestamp}\n\n"
                full_content = header + markdown_content + "\n"

                with open(trading_notes_file, 'w', encoding='utf-8') as f:
                    f.write(full_content)

                mode = "Created new" if not trading_notes_file.exists() else "Replaced"
                logger.info(f"{mode} trading notes file with {len(full_content)} characters")

                return f"""✓ Trading notes {mode.lower()} successfully

Mode: {mode} strategy document
Timestamp: {timestamp}
File: trading_notes/strategy.md
Content size: {len(markdown_content)} characters

Use read_trading_notes() to view the strategy document."""

        except Exception as e:
            logger.error(f"Error updating trading notes: {e}")
            return f"✗ Error updating trading notes: {str(e)}"
