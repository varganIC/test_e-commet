import asyncio

import aiohttp
from aiochclient import ChClient

from config import db_settings, git_url
from database import ClickHouseDB
from github_scraper import GithubReposScrapper


async def main():
    client = GithubReposScrapper(
        'API_KEY',
        git_url
    )
    data = await client.get_repositories()
    await client.close()

    async with aiohttp.ClientSession() as session:
        async with ChClient(session, **db_settings) as conn:
            async with ClickHouseDB(conn) as db:
                await db.insert_data(data)


asyncio.run(main())

