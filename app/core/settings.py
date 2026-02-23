from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    env: str = "development"
    app_name: str = "task_assignee"
    log_level: str = "INFO"

    host: str = "0.0.0.0"
    port: int = 8000

    mongodb_uri: str = "mongodb://localhost:27017"  # Default, will be overridden by .env
    mongodb_db: str = "task_assignee"

    redis_url: str = "redis://localhost:6379/0"

    uploads_dir: str = "uploads"

    # RBAC bypass for testing (set to true to disable auth enforcement)
    disable_rbac: bool = False

    jwt_secret: str = "change_me"
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 60
    refresh_token_expire_days: int = 7

    openai_api_key: str | None = None
    gemini_api_key: str | None = None
    
    # Vertex AI settings
    use_vertex_ai: bool = False
    vertex_ai_project_id: str | None = None
    vertex_ai_region: str = "us-central1"
    vertex_ai_model_name: str = "gemini-2.0-flash"
    vertex_ai_embedding_model_name: str = "text-embedding-004"
    
    # Additional Vertex AI fields from .env
    google_application_credentials: str | None = None
    gemini_embedding_model: str | None = None
    gemini_embedding_dimension: int | None = None
    project_id: str | None = None
    location: str | None = None
    
    # MySQL Configuration
    mysql_host: str | None = None
    mysql_port: int = 3306
    mysql_database: str | None = None
    mysql_user: str | None = None
    mysql_password: str | None = None
    
    # MySQL SSH Configuration
    mysql_ssh_host: str | None = None
    mysql_ssh_port: int = 22
    mysql_ssh_user: str | None = None
    mysql_ssh_key_path: str = "prod-key.pem"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", case_sensitive=False)


settings = Settings()
