import json
from typing import Annotated

import asyncpg
import uvicorn
from fastapi import APIRouter, FastAPI, Depends, Request


def get_config_db() -> dict:
    with open('app_settings.json', 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data['db_settings']


async def get_pg_connection(request: Request) -> asyncpg.Connection:
    async with request.app.state.db_pool.acquire() as conn:
        yield conn


async def get_db_version(
    conn: Annotated[asyncpg.Connection, Depends(get_pg_connection)]
):
    return await conn.fetchval("SELECT version()")


def register_routes(app: FastAPI):
    router = APIRouter(prefix="/api")
    router.add_api_route(path="/db_version", endpoint=get_db_version)
    app.include_router(router)


def create_app() -> FastAPI:
    app = FastAPI(title="e-Comet")

    @app.on_event("startup")
    async def startup():
        config = get_config_db()
        app.state.db_pool = await asyncpg.create_pool(**config)

    @app.on_event("shutdown")
    async def shutdown():
        await app.state.db_pool.close()

    register_routes(app)
    return app


if __name__ == "__main__":
    uvicorn.run("main:create_app", factory=True)
