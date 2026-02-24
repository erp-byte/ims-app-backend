API_PREFIX = "/api"

# Obfuscated code → real route name
# Frontend sends /api/1.1, middleware rewrites to /api/login
ROUTE_MAP = {
    "1.1": "login",
    "1.2": "register",
    "1.3": "refresh",
    "1.4": "punch-out",
    "1.5": "promoter-update",
    "1.6": "promoter-delete",
    "1.7": "change-password",
    "1.8": "send-otp",
    "1.9": "verify-otp",
    "2.0": "reset-password",
    "2.1": "punch-in",
    "2.2": "session-status",
}
