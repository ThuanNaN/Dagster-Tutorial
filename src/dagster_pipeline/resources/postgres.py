"""PostgreSQL resource for analytical query layer."""
import psycopg2
from sqlalchemy import create_engine
from dagster import ConfigurableResource


class PostgresResource(ConfigurableResource):
    host: str = "localhost"
    port: int = 5432
    database: str = "mydb"
    user: str = "postgres"
    password: str = "postgres"

    def get_connection_url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def get_connection(self):
        """Return a psycopg2 connection."""
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )

    def get_engine(self):
        """Return a SQLAlchemy engine for pandas to_sql()."""
        return create_engine(f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}")
