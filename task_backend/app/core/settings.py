
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
import os

class Settings(BaseSettings):

    app_name: str = Field(default="Task Assignment System", alias="APP_NAME")
    uploads_dir: str = Field(default="uploads", alias="UPLOADS_DIR")
    
    mongodb_uri: str = Field(default="mongodb://mongodb:27017", alias="MONGODB_URL")
    mongodb_db: str = Field(default="task_assignee", alias="MONGODB_DB")

    mysql_host: str = Field(default="mysql", alias="MYSQL_HOST")
    mysql_port: int = Field(default=3306, alias="MYSQL_PORT")
    mysql_database: str = Field(default="permits_db", alias="MYSQL_DATABASE")
    mysql_user: str = Field(default="task_user", alias="MYSQL_USER")
    mysql_password: str = Field(default="task_password", alias="MYSQL_PASSWORD")
    mysql_ssh_host: str | None = Field(default=None, alias="MYSQL_SSH_HOST")
    mysql_ssh_port: int = Field(default=22, alias="MYSQL_SSH_PORT")
    mysql_ssh_user: str = Field(default="root", alias="MYSQL_SSH_USER")
    mysql_ssh_key_path: str = Field(default="/home/user/smart_task_assignee/task_recommend/prod-key.pem", alias="MYSQL_SSH_KEY_PATH")

    clickhouse_host: str = Field(default="clickhouse", alias="CLICKHOUSE_HOST")
    clickhouse_port: int = Field(default=9000, alias="CLICKHOUSE_PORT")
    clickhouse_database: str = Field(default="task_analytics", alias="CLICKHOUSE_DATABASE")

settings = Settings()