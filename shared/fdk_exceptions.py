"""Custom exception types."""


class AuthenticationError(Exception):
    """Shoonya authentication failed."""


class TokenExpiredError(Exception):
    """Shoonya access token has expired."""


class WebSocketError(Exception):
    """Shoonya WebSocket connection error."""


class SupabaseError(Exception):
    """Supabase database operation error."""
