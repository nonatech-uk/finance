"""Finance API — FastAPI application."""

import sys
from contextlib import asynccontextmanager
from pathlib import Path

# Ensure project root is on sys.path so `config.*` and `src.*` imports work
# when running via uvicorn from any directory.
_project_root = str(Path(__file__).resolve().parent.parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.deps import close_pool, init_pool
from src.api.routers import accounts, categories, imports, merchants, stats, transactions


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage connection pool lifecycle."""
    init_pool()
    yield
    close_pool()


app = FastAPI(
    title="Finance API",
    version="0.1.0",
    description="Personal finance system API",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount routers
app.include_router(transactions.router, prefix="/api/v1", tags=["transactions"])
app.include_router(accounts.router, prefix="/api/v1", tags=["accounts"])
app.include_router(categories.router, prefix="/api/v1", tags=["categories"])
app.include_router(merchants.router, prefix="/api/v1", tags=["merchants"])
app.include_router(stats.router, prefix="/api/v1", tags=["stats"])
app.include_router(imports.router, prefix="/api/v1", tags=["imports"])


@app.get("/health")
def health():
    return {"status": "ok"}
