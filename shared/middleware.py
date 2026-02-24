from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from shared.constants import API_PREFIX, ROUTE_MAP


class RouteObfuscationMiddleware(BaseHTTPMiddleware):
    """Rewrites obfuscated URLs to real route names.

    /api/1.1 → /api/login
    /api/1.2 → /api/register
    """

    async def dispatch(self, request: Request, call_next):
        path = request.scope["path"]

        if path.startswith(API_PREFIX + "/"):
            code = path[len(API_PREFIX) + 1:]
            real_name = ROUTE_MAP.get(code)
            if real_name:
                request.scope["path"] = f"{API_PREFIX}/{real_name}"

        return await call_next(request)
