from pathlib import Path

from mees_shared.settings import BaseAppSettings


class Settings(BaseAppSettings):
    db_host: str = "192.168.128.9"
    db_name: str = "finance"
    db_user: str = "finance"
    db_sslmode: str = "require"

    cors_origins: list[str] = [
        "https://finance.mees.st",
        "http://localhost:5173",
    ]

    # Monzo
    monzo_client_id: str = ""
    monzo_client_secret: str = ""
    monzo_redirect_uri: str = "http://localhost:9876/oauth/callback"
    monzo_auth_url: str = "https://auth.monzo.com/"
    monzo_token_url: str = "https://api.monzo.com/oauth2/token"
    monzo_api_base: str = "https://api.monzo.com"

    # Cross-DB (stuff — for Amazon order lookups)
    stuff_db_name: str = "stuff"
    stuff_db_user: str = "stuff"
    stuff_db_password: str = ""

    # Wise
    wise_api_token: str = ""
    wise_api_base: str = "https://api.wise.com"

    # Splitwise
    splitwise_api_key: str = ""
    splitwise_api_base: str = "https://secure.splitwise.com/api/v3.0"
    splitwise_default_group_id: int = 0

    # Xero
    xero_client_id: str = ""
    xero_client_secret: str = ""
    xero_redirect_uri: str = "http://localhost:9877/oauth/callback"
    xero_token_file: str = "xero_tokens.json"
    xero_tenant_id: str = ""
    xero_bank_account_id: str = ""
    xero_default_account_code: str = "400"

    # PayPal
    paypal_client_id: str = ""
    paypal_client_secret: str = ""
    paypal_environment: str = "live"

    # Healthcheck UUIDs
    hc_paypal_sync: str = ""

    # Anthropic (for LLM categorisation)
    anthropic_api_key: str = ""

    # Receipts
    receipt_storage_path: str = "./receipts"

    model_config = {
        "env_file": str(Path(__file__).resolve().parent / ".env"),
        "env_file_encoding": "utf-8",
    }

    @property
    def stuff_dsn(self) -> str:
        return self.cross_dsn(
            self.stuff_db_name, self.stuff_db_user, self.stuff_db_password,
        )


settings = Settings()
