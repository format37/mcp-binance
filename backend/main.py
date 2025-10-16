import contextlib
import contextvars
import logging
import os
import re
from typing import Any, Dict
import sentry_sdk
import uvicorn
import pathlib
from dotenv import load_dotenv
from starlette.applications import Starlette
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.routing import Mount
from mcp.server.fastmcp import FastMCP
from binance.client import Client
from mcp_service import register_py_eval, register_tool_notes
from mcp_resources import register_mcp_resources
from binance_tools.get_account import register_binance_get_account
from binance_tools.get_ticker import register_binance_get_ticker
from binance_tools.get_orderbook import register_binance_get_orderbook
from binance_tools.get_recent_trades import register_binance_get_recent_trades
from binance_tools.get_price import register_binance_get_price
from binance_tools.get_book_ticker import register_binance_get_book_ticker
from binance_tools.get_avg_price import register_binance_get_avg_price
from binance_tools.get_open_orders import register_binance_get_open_orders
from binance_tools.spot_trade_history import register_binance_spot_trade_history
from binance_tools.spot_market_order import register_binance_spot_market_order
from binance_tools.spot_limit_order import register_binance_spot_limit_order
from binance_tools.spot_oco_order import register_binance_spot_oco_order
from binance_tools.cancel_order import register_binance_cancel_order
from binance_tools.get_futures_balances import register_binance_get_futures_balances
from binance_tools.trade_futures_market import register_binance_trade_futures_market
from binance_tools.futures_limit_order import register_binance_futures_limit_order
from binance_tools.get_futures_open_orders import register_binance_get_futures_open_orders
from binance_tools.cancel_futures_order import register_binance_cancel_futures_order
from binance_tools.get_futures_trade_history import register_binance_get_futures_trade_history
from binance_tools.set_futures_leverage import register_binance_set_futures_leverage
from binance_tools.manage_futures_positions import register_binance_manage_futures_positions
from binance_tools.calculate_liquidation_risk import register_binance_calculate_liquidation_risk
from binance_tools.calculate_spot_pnl import register_binance_calculate_spot_pnl
from binance_tools.trading_notes import register_trading_notes

load_dotenv(".env")

logger = logging.getLogger(__name__)

# Initialize Sentry if DSN is provided
sentry_dsn = os.getenv("SENTRY_DSN")
if sentry_dsn:
    from sentry_sdk.integrations.logging import LoggingIntegration

    logger.info(f"Initializing Sentry with DSN: {sentry_dsn[:20]}... (truncated for security)")

    # Filter function to exclude noisy logs from Sentry
    def before_send(event, hint):
        """Filter out noisy health checks and expected stream disconnection errors from Sentry"""
        # Filter out health check requests
        if event.get('logger') == 'uvicorn.access':
            message = event.get('message', '')
            if '/health' in message:
                return None  # Don't send to Sentry

        # Filter out expected stream disconnection errors (ClosedResourceError)
        # These occur when clients disconnect and are part of normal operation
        if 'ClosedResourceError' in str(event.get('message', '')):
            return None  # Don't send to Sentry

        return event

    # Get container name from environment for better tracking
    container_name = os.getenv("CONTAINER_NAME", "mcp-binance-local")

    # Configure logging integration
    logging_integration = LoggingIntegration(
        level=logging.INFO,        # Capture info and above as breadcrumbs
        event_level=logging.WARNING  # Only send warnings and above as events (issues)
    )

    sentry_sdk.init(
        dsn=sentry_dsn,
        environment=container_name,
        send_default_pii=False,  # Don't send PII by default for security
        # Enable profiling for performance monitoring
        profiles_sample_rate=1.0,  # Profile 100% of transactions
        # Enable transaction tracing for performance monitoring
        traces_sample_rate=1.0,  # Trace 100% of transactions
        integrations=[logging_integration],
        # Enable logs to be sent to Sentry
        _experiments={
            "enable_logs": True,
        },
        before_send=before_send
    )

    # Set service tag for easier filtering in Sentry
    sentry_sdk.set_tag("service", "mcp-binance")

    logger.info(f"Sentry initialized successfully with profiling and tracing (environment: {container_name})")
else:
    logger.info("Sentry DSN not provided, running without Sentry")

MCP_TOKEN_CTX = contextvars.ContextVar("mcp_token", default=None)

# Initialize FastMCP using MCP_NAME (env) for tool name and base path
# Ensure the streamable path ends with '/'
MCP_NAME = os.getenv("MCP_NAME", "polygon")
_safe_name = re.sub(r"[^a-z0-9_-]", "-", MCP_NAME.lower()).strip("-") or "service"
BASE_PATH = f"/{_safe_name}"
STREAM_PATH = f"{BASE_PATH}/"
ENV_PREFIX = re.sub(r"[^A-Z0-9_]", "_", _safe_name.upper())
logger.info(f"Safe service name: {_safe_name}")
logger.info(f"Stream path: {STREAM_PATH}")


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning("Invalid %s=%r; using %s", name, value, default)
        return default


def _sanitize_filename(name: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9_.-]", "-", name).strip("-.")
    return sanitized or "script"


# Binance API configuration
BINANCE_API_KEY = os.getenv("BINANCE-API-KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE-API-SECRET", "")

# Initialize Binance client if API credentials are available
binance_client = None
if BINANCE_API_KEY and BINANCE_API_SECRET:
    try:
        logger.info(f"BINANCE_API_KEY: {BINANCE_API_KEY[:5]}... (truncated for security)")
        binance_client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
        logger.info("Binance Client initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Binance Client: {e}")
        binance_client = None
else:
    logger.warning("BINANCE_API_KEY or BINANCE_API_SECRET not provided")

mcp = FastMCP(_safe_name, streamable_http_path=STREAM_PATH, json_response=True)

# CSV storage directory (hardcoded for simplicity)
CSV_DIR = pathlib.Path("data/mcp-binance")
CSV_DIR.mkdir(parents=True, exist_ok=True)

# Register MCP resources (documentation, etc.)
register_mcp_resources(mcp, _safe_name)

# Binance MCP tools
register_binance_get_account(mcp, binance_client, CSV_DIR)
register_binance_get_ticker(mcp, binance_client, CSV_DIR)
register_binance_get_orderbook(mcp, binance_client, CSV_DIR)
register_binance_get_recent_trades(mcp, binance_client, CSV_DIR)
register_binance_get_price(mcp, binance_client, CSV_DIR)
register_binance_get_book_ticker(mcp, binance_client, CSV_DIR)
register_binance_get_avg_price(mcp, binance_client, CSV_DIR)
register_binance_get_open_orders(mcp, binance_client, CSV_DIR)
register_binance_spot_trade_history(mcp, binance_client, CSV_DIR)
register_binance_spot_market_order(mcp, binance_client, CSV_DIR)
register_binance_spot_limit_order(mcp, binance_client, CSV_DIR)
register_binance_spot_oco_order(mcp, binance_client, CSV_DIR)
register_binance_cancel_order(mcp, binance_client, CSV_DIR)
register_binance_get_futures_balances(mcp, binance_client, CSV_DIR)
register_binance_trade_futures_market(mcp, binance_client, CSV_DIR)
register_binance_futures_limit_order(mcp, binance_client, CSV_DIR)
register_binance_get_futures_open_orders(mcp, binance_client, CSV_DIR)
register_binance_cancel_futures_order(mcp, binance_client, CSV_DIR)
register_binance_get_futures_trade_history(mcp, binance_client, CSV_DIR)
register_binance_set_futures_leverage(mcp, binance_client, CSV_DIR)
register_binance_manage_futures_positions(mcp, binance_client, CSV_DIR)
register_binance_calculate_liquidation_risk(mcp, binance_client, CSV_DIR)
register_binance_calculate_spot_pnl(mcp, binance_client, CSV_DIR)
register_trading_notes(mcp, CSV_DIR)
register_py_eval(mcp, CSV_DIR)
register_tool_notes(mcp, CSV_DIR)

# Add custom error handling for stream disconnections
original_logger = logging.getLogger("mcp.server.streamable_http")

class StreamErrorFilter(logging.Filter):
    def filter(self, record):
        # Suppress ClosedResourceError logs as they're expected when clients disconnect
        if "ClosedResourceError" in str(record.getMessage()):
            return False
        return True

original_logger.addFilter(StreamErrorFilter())

# Build the main ASGI app with Streamable HTTP mounted
mcp_asgi = mcp.streamable_http_app()

@contextlib.asynccontextmanager
async def lifespan(_: Starlette):
    # Ensure FastMCP session manager is running, as required by Streamable HTTP
    async with mcp.session_manager.run():
        yield

async def health_check(request):
    return JSONResponse({"status": "healthy", "service": "polygon-mcp"})

app = Starlette(
    routes=[
        # Mount at root; internal app handles service path routing
        Mount("/", app=mcp_asgi),
    ],
    lifespan=lifespan,
)

# Add health endpoint before auth middleware
from starlette.routing import Route
app.routes.insert(0, Route("/health", health_check, methods=["GET"]))


class TokenAuthMiddleware(BaseHTTPMiddleware):
    """Simple token gate for all service requests under BASE_PATH.

    Accepts tokens via Authorization header: "Bearer <token>" (default and recommended).
    If env {ENV_PREFIX}_ALLOW_URL_TOKENS=true, also accepts:
      - Query parameter: ?token=<token>
      - URL path form: {BASE_PATH}/<token>/... (token is stripped before forwarding)

    Configure allowed tokens via env var {ENV_PREFIX}_TOKENS (comma-separated). If unset or empty,
    authentication is disabled (allows all) but logs a warning.
    """

    def __init__(self, app):
        super().__init__(app)
        # Prefer envs derived from MCP_NAME; fall back to legacy CBONDS_* names for backward compatibility
        raw = os.getenv(f"MCP_TOKENS", "")
        self.allowed_tokens = {t.strip() for t in raw.split(",") if t.strip()}
        self.allow_url_tokens = (
            os.getenv(f"MCP_ALLOW_URL_TOKENS", "").lower()
            in ("1", "true", "yes")
        )
        self.require_auth = (
            os.getenv(f"MCP_REQUIRE_AUTH", "").lower()
            in ("1", "true", "yes")
        )
        if not self.allowed_tokens:
            if self.require_auth:
                logger.warning(
                    "%s is not set; %s=true -> all %s requests will be rejected (401)",
                    f"MCP_TOKENS",
                    f"MCP_REQUIRE_AUTH",
                    BASE_PATH,
                )
            else:
                logger.warning(
                    "%s is not set; token auth is DISABLED for %s endpoints",
                    f"MCP_TOKENS",
                    BASE_PATH,
                )

    async def dispatch(self, request, call_next):
        # Only protect BASE_PATH path space
        path = request.url.path or "/"
        if not path.startswith(BASE_PATH):
            return await call_next(request)

        def accept(token_value: str, source: str):
            request.state.mcp_token = token_value  # stash for downstream use
            logger.info(
                "Authenticated %s %s via %s token %s",
                request.method,
                path,
                source,
                token_value,
            )
            return MCP_TOKEN_CTX.set(token_value)

        async def proceed(token_value: str, source: str):
            token_scope = accept(token_value, source)
            try:
                return await call_next(request)
            finally:
                MCP_TOKEN_CTX.reset(token_scope)

        # If auth is not required, always allow
        if not self.require_auth:
            logger.info("Auth disabled, allowing request to %s", path)
            return await call_next(request)

        # If no tokens configured but auth is required
        if not self.allowed_tokens:
            return JSONResponse({"detail": "Unauthorized"}, status_code=401, headers={"WWW-Authenticate": "Bearer"})

        # Authorization: Bearer <token>
        token = None
        auth = request.headers.get("authorization") or request.headers.get("Authorization")
        if auth and auth.lower().startswith("bearer "):
            token = auth.split(" ", 1)[1].strip()

        # Header token valid -> allow
        if token and token in self.allowed_tokens:
            return await proceed(token, "header")

        # If URL tokens are allowed, check query and path variants
        if self.allow_url_tokens:
            # 1) Query parameter ?token=...
            url_token = request.query_params.get("token")
            if url_token and url_token in self.allowed_tokens:
                return await proceed(url_token, "query")

            # 2) Path segment /<service>/<token>/...
            segs = [s for s in path.split("/") if s != ""]
            if len(segs) >= 2 and segs[0] == _safe_name:
                candidate = segs[1]
                if candidate in self.allowed_tokens:
                    # Rebuild path without the token segment
                    remainder = "/".join([_safe_name] + segs[2:])
                    new_path = "/" + (remainder + "/" if path.endswith("/") and not remainder.endswith("/") else remainder)
                    if new_path == BASE_PATH:
                        new_path = STREAM_PATH
                    request.scope["path"] = new_path
                    if "raw_path" in request.scope:
                        request.scope["raw_path"] = new_path.encode("utf-8")
                    return await proceed(candidate, "path")

        # If we reached here, reject unauthorized
        if self.allow_url_tokens:
            detail = "Unauthorized"
        else:
            detail = "Use Authorization: Bearer <token>; URL/query tokens are not allowed"
        return JSONResponse({"detail": detail}, status_code=401, headers={"WWW-Authenticate": "Bearer"})


# Install auth middleware last to wrap the full app
app.add_middleware(TokenAuthMiddleware)

def main():
    """
    Run the uvicorn server without SSL (TLS handled by reverse proxy).
    """
    PORT = int(os.getenv("PORT", "8010"))

    logger.info(f"Starting {MCP_NAME} MCP server (HTTP) on port {PORT} at {STREAM_PATH}")

    uvicorn.run(
        app=app,
        host=os.getenv("HOST", "0.0.0.0"),
        port=PORT,
        log_level=os.getenv("LOG_LEVEL", "info"),
        access_log=True,
        # Behind Caddy: respect X-Forwarded-* and use https in redirects
        proxy_headers=True,
        forwarded_allow_ips="*",
        timeout_keep_alive=120,
    )

if __name__ == "__main__":
    main()
