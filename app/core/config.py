import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
load_dotenv(os.path.join(BASE_DIR, ".env"))


class Settings(BaseSettings):
    PROJECT_NAME: str = "FASTAPI BASE"
    SECRET_KEY: str = "secret123"
    API_PREFIX: str = ""
    BACKEND_CORS_ORIGINS: List[str] = ["*"]
    DATABASE_URL: str = "postgresql://db_user:secret123@localhost:5433/fastapi_base"
    ACCESS_TOKEN_EXPIRE_SECONDS: int = 60 * 60 * 24 * 7
    SECURITY_ALGORITHM: str = "HS256"
    LOGGING_CONFIG_FILE: str = os.path.join(BASE_DIR, "logging.ini")

    model_config = SettingsConfigDict(env_file=os.path.join(BASE_DIR, ".env"))


settings = Settings()
