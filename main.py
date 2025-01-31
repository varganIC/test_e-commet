import json
from typing import Annotated

import asyncpg
import uvicorn
from fastapi import APIRouter, FastAPI, Depends

# Я так понял, что необходимо вообще не хранить состояние
# подключения к БД , сделал так, чтобы при каждом запросе к роуту
# создавалось и корректно закрывалось соединение


def get_config_db() -> dict:
    with open('app_settings.json', 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data


async def get_db_pool(
    config: Annotated[dict, Depends(get_config_db)]
) -> asyncpg.Pool:
    async with asyncpg.create_pool(**config) as pool:
        yield pool


async def get_pg_connection(
    pool: Annotated[asyncpg.Pool, Depends(get_db_pool)]
) -> asyncpg.Connection:
    async with pool.acquire() as conn:
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
    register_routes(app)
    return app


if __name__ == "__main__":
    uvicorn.run("main:create_app", factory=True)
