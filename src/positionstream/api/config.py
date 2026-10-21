from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    postgres_db: str = "positionstream"
    postgres_user: str = "positionstream"
    postgres_password: str = "change-me-for-local-development"
    postgres_host: str = "localhost"
    postgres_port: int = 5432

    redis_url: str = "redis://localhost:6379/0"

    api_host: str = "127.0.0.1"
    api_port: int = 8000
    log_level: str = "INFO"

    @property
    def database_url(self) -> str:
        from urllib.parse import quote, urlencode

        user = quote(self.postgres_user, safe="")
        password = quote(self.postgres_password, safe="")
        credentials = f"{user}:{password}"
        database = quote(self.postgres_db, safe="")
        query = urlencode({"host": self.postgres_host, "port": self.postgres_port})
        return f"postgresql://{credentials}@/{database}?{query}"


@lru_cache
def get_settings() -> Settings:
    return Settings()
