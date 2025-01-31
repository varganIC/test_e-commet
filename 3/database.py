import datetime as dt
from aiochclient import ChClient
from github_scraper import Repository


class ClickHouseDB:
    def __init__(self, db: ChClient):
        self.db = db

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.db.close()

    async def _insert_repositories(self, items):
        await self.db.execute(
            "INSERT INTO repositories VALUES", *items
        )

    async def _insert_repositories_authors_commits(self, items):
        await self.db.execute(
            "INSERT INTO repositories_authors_commits VALUES", *items
        )

    async def _insert_repositories_positions(self, items):
        await self.db.execute(
            "INSERT INTO repositories_positions VALUES", *items
        )

    async def insert_data(self, items: list[Repository]):
        date = dt.datetime.now()
        repos = []
        repositories_positions = []
        repositories_authors_commits = []

        for item in items:
            repos.append(
                (
                    item.name,
                    item.owner,
                    item.stars,
                    item.watchers,
                    item.forks,
                    item.language,
                    date.strftime("%Y-%m-%d %H:%M:%S"),
                )
            )
            repositories_positions.append(
                (
                    date.date(),
                    item.name,
                    item.position,
                )
            )
            repositories_authors_commits.extend(
                [
                    (
                        date.date(),
                        item.name,
                        commit.author,
                        commit.commits_num,
                    )
                    for commit in item.authors_commits_num_today
                ]
            )

        await self._insert_repositories(repos)
        await self._insert_repositories_authors_commits(
            repositories_authors_commits
        )
        await self._insert_repositories_positions(
            repositories_positions
        )



