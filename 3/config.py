import json
from dataclasses import dataclass


@dataclass
class DBSettingsModel:
    url: str
    user: str
    password: str
    database: str


@dataclass
class AppSettings:
    db_settings: DBSettingsModel
    github_api_base_url: str


class SettingsLoader:
    def __init__(self):
        self.file_name = 'app_settings.json'
        self.app_settings = None

        self.load_config()

    @staticmethod
    def load_json(path):
        with open(path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        return data

    def load_config(self):
        data = self.load_json(self.file_name)

        self.app_settings = AppSettings(**data)


loader = SettingsLoader()
db_settings = loader.app_settings.db_settings
git_url = loader.app_settings.github_api_base_url
