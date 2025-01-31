import asyncio
import datetime as dt

from aiolimiter import AsyncLimiter
from dataclasses import dataclass
from typing import Any, Union

from aiohttp import ClientSession


@dataclass
class RepositoryAuthorCommitsNum:
    author: str
    commits_num: int


@dataclass
class Repository:
    name: str
    owner: str
    position: int
    stars: int
    watchers: int
    forks: int
    language: str
    authors_commits_num_today: list[RepositoryAuthorCommitsNum]


class GithubReposScrapper:
    def __init__(
        self,
        access_token: str,
        github_url_base: str,
        max_req: int = 5,
    ):
        self._session = ClientSession(
            headers={
                "Accept": "application/vnd.github.v3+json",
                "Authorization": f"Bearer {access_token}"
            }
        )
        self.github_url_base = github_url_base
        self.mcr = asyncio.Semaphore(max_req)
        self.rps = AsyncLimiter(max_req, 1)

    async def _make_request(
        self,
        endpoint: str,
        method: str = "GET",
        params: Union[dict[str, Any], None] = None
    ) -> Any:
        async with self._session.request(
            method,
            f"{self.github_url_base}/{endpoint}",
            params=params
        ) as response:
            return await response.json()

    async def _get_top_repositories(self, limit: int = 10) -> list[dict[str, Any]]:
        """GitHub REST API: https://docs.github.com/en/rest/search/search?apiVersion=2022-11-28#search-repositories"""
        data = await self._make_request(
            endpoint="search/repositories",
            params={"q": "stars:>1", "sort": "stars", "order": "desc", "per_page": limit},
        )
        return data["items"]

    async def _get_repository_commits(self, owner: str, repo: str) -> list[dict[str, Any]]:
        """GitHub REST API: https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28#list-commits"""
        async with self.mcr:
            async with self.rps:
                data = await self._make_request(
                    endpoint=f'repos/{owner}/{repo}/commits'
                )
                print(dt.datetime.utcnow())
                return data

    async def get_repositories(self) -> list[Repository]:
        list_repo = []
        yesterday = dt.datetime.utcnow() - dt.timedelta(days=1)
        repos = await self._get_top_repositories()
        if not repos:
            return list_repo

        committers_tasks = [
            self._get_repository_commits(
                repo['owner']['login'],
                repo['name']
            )
            for repo in repos
        ]

        committers_res = await asyncio.gather(*committers_tasks)
        for index, (repo, committers) in enumerate(
            zip(repos, committers_res), start=1
        ):
            authors = {}
            for committer in committers:
                if committer.get('commit') is None:
                    continue

                date_commit = dt.datetime.strptime(
                    committer['commit']['author']['date'],
                    "%Y-%m-%dT%H:%M:%SZ"
                )
                if date_commit > yesterday:
                    author = committer['author']
                    if author and author.get('login'):
                        authors[author['login']] = (
                                authors.get(author['login'], 0) + 1
                        )

            list_repo.append(
                Repository(
                    name=repo['name'],
                    owner=repo['owner']['login'],
                    position=index,
                    stars=repo['stargazers_count'],
                    watchers=repo['watchers'],
                    forks=repo['forks'],
                    language=repo['language'],
                    authors_commits_num_today=[
                        RepositoryAuthorCommitsNum(
                            author=key,
                            commits_num=value,
                        )
                        for key, value in authors.items()
                    ]
                )
            )

        return list_repo

    async def close(self):
        await self._session.close()
