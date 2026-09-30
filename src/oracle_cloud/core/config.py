from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    APP_NAME: str = Field(default="Lion API")
    APP_ENV: str = Field(default="development")
    DEBUG: bool = Field(default=True)
    LOG_LEVEL: str = Field(default="INFO")

    @field_validator("DEBUG", mode="before")
    @classmethod
    def parse_debug(cls, v: str | bool) -> bool:
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            v_lower = v.lower().strip()
            if v_lower in ("true", "1", "yes", "on"):
                return True
            if v_lower in ("false", "0", "no", "off", ""):
                return False
            return False
        return bool(v)

    HOST: str = Field(default="0.0.0.0")
    PORT: int = Field(default=8001)

    CORS_ORIGINS: list[str] = Field(
        default=["*"],
        # ["http://lion.dev.agnes.com.br", "https://lion.dev.agnes.com.br"],
    )

    # Database - individual fields
    DB_HOST: str = Field(default="localhost")
    DB_PORT: int = Field(default=5432)
    DB_USER: str = Field(default="postgres")
    DB_PASSWORD: str = Field(default="postgres")
    DB_NAME: str = Field(default="lion_db")
    DATABASE_POOL_SIZE: int = Field(default=10)
    DATABASE_MAX_OVERFLOW: int = Field(default=20)
    DATABASE_ECHO: bool = Field(default=False)

    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    # Cloudflare Turnstile (CAPTCHA do portal público).
    # Vazio = verificação desabilitada (degradação graciosa; o front também
    # esconde o widget quando a site key não está configurada).
    TURNSTILE_SECRET_KEY: str = Field(default="")

    # OCI Object Storage
    OCI_ACCESS_KEY_ID: str = Field(default="")
    OCI_SECRET_ACCESS_KEY: str = Field(default="")
    OCI_REGION: str = Field(default="sa-saopaulo-1")
    OCI_BUCKET_NAME: str = Field(default="lion-bucket")
    OCI_ENDPOINT_URL: str = Field(default="")

    # OCI Queue
    OCI_QUEUE_ID: str = Field(default="")
    OCI_QUEUE_SERVICE_ENDPOINT: str = Field(default="")
    OCI_TENANCY_ID: str = Field(default="")
    OCI_USER_ID: str = Field(default="")
    OCI_FINGERPRINT: str = Field(default="")
    OCI_KEY_FILE: str = Field(default="")
    SPARK_JOBS_QUEUE: str = Field(default="spark-jobs")
    # Origin enviado ao ag-platform-launcher, que resolve namespace/nodepool/pvc
    # do Spark. Default "lion"; o ambiente SP seta LION_ORIGIN=lion-sp.
    LION_ORIGIN: str = Field(default="lion")

    # ── Agnes Notifications Service integration (PILOT, feature-flagged) ───────
    # When enabled, income-statement (informe) emails are published to the
    # centralized agnes-notifications-service queue (link-in-body) instead of the
    # internal notification_queue/scheduler. Internal engine stays the default.
    AGNES_NOTIFICATIONS_ENABLED: bool = Field(default=False)
    # OCI Queue OCID + messages endpoint of the agnes-notifications-service queue
    # (different from spark-jobs; OCI auth is shared with the OCI_* vars above).
    AGNES_NOTIFICATIONS_QUEUE_ID: str = Field(default="")
    AGNES_NOTIFICATIONS_QUEUE_ENDPOINT: str = Field(default="")
    # Base URL of the portal page where the worker downloads their informe PDF.
    # Used to build the secure link placed in the email body (no attachment).
    PORTAL_INFORME_BASE_URL: str = Field(default="")

    # dev/stg only: quando setado, o reenvio do portal entrega o e-mail neste
    # endereço fixo em vez do destinatário real (validação/teste do fluxo sem
    # enviar para o colaborador). Vazio = sem override (produção).
    # Mantido por precaução — não utilizado; substituído por NOTIFICATION_REDIRECT_EMAIL.
    PORTAL_REENVIO_OVERRIDE_EMAIL: str = Field(default="")

    # dev/stg only: quando setado, TODOS os e-mails (portal e agendamento) são
    # redirecionados para este endereço, ignorando o e-mail real do trabalhador.
    # Vazio em produção = usa o e-mail real (comportamento normal).
    NOTIFICATION_REDIRECT_EMAIL: str = Field(default="")

    # ╔═══════════════════════════════════════════════════════════════════════╗
    # ║  ⚠️  CÓDIGO TEMPORÁRIO — VAI MORRER  ⚠️                                 ║
    # ║  SMTP único para o mock de dev/stg do reenvio do portal.              ║
    # ║  Substitui o provider hardcoded (O365 Basic Auth morto) enquanto o    ║
    # ║  sender_config por empresa não está ligado no scheduler.              ║
    # ║  REMOVER quando _get_provider_for_empresa usar sender_config (DB).    ║
    # ╚═══════════════════════════════════════════════════════════════════════╝
    NOTIFICATION_SMTP_HOST: str = Field(default="")
    NOTIFICATION_SMTP_PORT: int = Field(default=587)
    NOTIFICATION_SMTP_USERNAME: str = Field(default="")
    NOTIFICATION_SMTP_PASSWORD: str = Field(default="")
    NOTIFICATION_SMTP_ENCRYPTION: str = Field(default="tls")
    NOTIFICATION_SMTP_FROM_EMAIL: str = Field(default="")
    NOTIFICATION_SMTP_FROM_NAME: str = Field(default="Agnes")

    SECRET_KEY: str = Field(default="your-secret-key-change-in-production")
    ALGORITHM: str = Field(default="HS256")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(default=30)
    REFRESH_TOKEN_EXPIRE_DAYS: int = Field(default=7)

    # SMTP Email Global — Removido: SMTP agora é exclusivamente por empresa via sender_config
    # SMTP_HOST: str = Field(default="smtp.office365.com")
    # SMTP_PORT: int = Field(default=587)
    # SMTP_USERNAME: str = Field(default="noreply@agcapital.com.br")
    # SMTP_PASSWORD: str = Field(default="")  # Configurar via Secret no Rancher (OCI Vault)
    # SMTP_ENCRYPTION: str = Field(default="tls")
    # EMAIL_DEFAULT_SENDER: str = Field(default="noreply@agcapital.com.br")
    # EMAIL_DEFAULT_SENDER_NAME: str = Field(default="AG Capital")

    NOTIFICATION_SCHEDULER_INTERVAL_SECONDS: int = Field(default=60)
    # TODO: REMOVER APÓS TESTES — whitelist de empresas permitidas para envio de notificações. Quando vazio, permite todas.
    NOTIFICATION_ALLOWED_COMPANY_IDS: str = Field(
        default="a0a0a0a0-1111-4000-a000-000000000001,a0a0a0a0-1111-4000-a000-000000000002"
    )

    # SSO (cloud — deixar vazio em on-premise)
    # URL base do Keycloak — ex: https://sso.agnes.com.br
    # Um realm por tenant: issuer = {KEYCLOAK_BASE_URL}/realms/{tenant}
    KEYCLOAK_BASE_URL: str = Field(default="")
    # URL interna do KC para chamadas server-to-server (evita DNS externo no K8s).
    # Se vazio, usa KEYCLOAK_BASE_URL.
    KEYCLOAK_INTERNAL_URL: str = Field(default="")
    # Client do lion-api no Keycloak — mesmo nome em todos os realms
    KEYCLOAK_CLIENT_ID: str = Field(default="lion-api")
    # Fallback global — usado se TENANT_PROVIDER_* não estiver configurado
    KEYCLOAK_CLIENT_SECRET: str = Field(default="")
    # Provisionamento (tenant-provider → lion-api, cloud apenas)
    SERVICE_TOKEN: str = Field(default="")

    # Tenant Provider (lion-api → tenant-provider, para buscar client_secret por tenant)
    # Se não configurado, usa KEYCLOAK_CLIENT_SECRET global como fallback
    TENANT_PROVIDER_BASE_URL: str = Field(default="")
    TENANT_PROVIDER_REALM: str = Field(default="tenant-provider-dev")
    TENANT_PROVIDER_CLIENT_ID: str = Field(default="")
    TENANT_PROVIDER_CLIENT_SECRET: str = Field(default="")
    # Service token para chamar o BFF proxy de usuários do tenant-provider
    # (POST /api/v1/tenants/{slug}/users). Validado contra o SERVICE_TOKEN do
    # tenant-provider. Usado no modo multitenant para provisionar usuários no Keycloak.
    TENANT_PROVIDER_SERVICE_TOKEN: str = Field(default="")

    @property
    def keycloak_server_url(self) -> str:
        """URL do KC para chamadas server-to-server (interna se disponível)."""
        return self.KEYCLOAK_INTERNAL_URL or self.KEYCLOAK_BASE_URL

    @property
    def is_tenant_provider_configured(self) -> bool:
        return bool(
            self.TENANT_PROVIDER_BASE_URL
            and self.TENANT_PROVIDER_CLIENT_ID
            and self.TENANT_PROVIDER_CLIENT_SECRET
        )

    STALE_ETL_JOB_TIMEOUT_MINUTES: int = Field(default=30)

    # Deployment mode — on-premise: local DB para tudo; multitenant: usa serviços da plataforma Agnes
    # (agnes-companies-service, agnes-user-service, etc.)
    APP_MODE: str = Field(default="on-premise")  # "on-premise" | "multitenant"
    ENABLE_COMPANY_RLS: bool = Field(
        default=False
    )  # True = filter companies by role + user_company_permissions
    AGNES_USERS_URL: str = Field(default="http://agnes-user-service:8005")
    AGNES_USERS_SERVICE_TOKEN: str = Field(default="")
    # Tenant fallback para usuários locais (sem claim SSO) em modo multitenant
    DEFAULT_TENANT: str = Field(default="")

    # SE Service — digital certificates
    SE_SERVICE_URL: str = Field(default="http://localhost:9096")
    SE_SERVICE_USERNAME: str = Field(default="")
    SE_SERVICE_PASSWORD: str = Field(default="")

    ETL_PATH: str = Field(default="/app/lion-etl")
    API_BASE_URL: str = Field(default="http://localhost:8001")
    NGROK_URL: str = Field(default="")

    @property
    def callback_base_url(self) -> str:
        if self.NGROK_URL:
            return self.NGROK_URL.rstrip("/")
        return self.API_BASE_URL.rstrip("/")

    # Observability
    LOG_JSON: bool = Field(default=False)  # True em K8s (STG/PRD) — logs JSON para Loki

    @property
    def is_development(self) -> bool:
        return self.APP_ENV == "development"

    @property
    def is_production(self) -> bool:
        return self.APP_ENV == "production"


settings = Settings()