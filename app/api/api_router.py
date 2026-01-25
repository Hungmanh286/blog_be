from fastapi import APIRouter

from app.api import (
    api_user, api_login, api_register, api_healthcheck, api_post, 
    api_market_data, api_market_indicators
)

router = APIRouter()

router.include_router(
    api_healthcheck.router, tags=["health-check"], prefix="/healthcheck"
)
router.include_router(api_login.router, tags=["login"], prefix="/login")
router.include_router(api_register.router, tags=["register"], prefix="/register")
router.include_router(api_user.router, tags=["user"], prefix="/users")
router.include_router(api_post.router, tags=["posts"], prefix="/posts")
router.include_router(api_market_data.router, tags=["market-data"], prefix="/market-data")
router.include_router(api_market_indicators.router, tags=["market-indicators"], prefix="/market-indicators")
