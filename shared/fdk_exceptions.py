"""Custom exception types."""


class AuthenticationError(Exception):
    """Fyers authentication failed."""


class TokenExpiredError(Exception):
    """Fyers access token has expired."""


class WebSocketError(Exception):
    """Fyers WebSocket connection error."""


class SupabaseError(Exception):
    """Supabase database operation error."""
