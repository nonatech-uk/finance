"""Auth endpoints."""

from fastapi import APIRouter, Depends

from src.api.deps import CurrentUser, get_current_user
from src.api.models import UserInfo

router = APIRouter()


@router.get("/auth/me", response_model=UserInfo)
def get_me(user: CurrentUser = Depends(get_current_user)):
    """Return the authenticated user's info."""
    return UserInfo(
        email=user.email,
        display_name=user.display_name,
        allowed_scopes=user.allowed_scopes,
        role=user.role,
    )
