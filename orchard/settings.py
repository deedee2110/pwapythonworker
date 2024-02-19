from pydantic_settings import BaseSettings


class OrchardSettings(BaseSettings):
    DB_URI: str = "mysql://deverhood:serviceByDeverhood@127.0.0.1/test?charset=utf8mb4"
    # COFFER
    COFFER_AES_KEY: str = "DeverhoodHT2021!"
    COFFER_AES_IV: str = "ABCD1234EFGH5678"
    MINIO_ENDPOINT: str = ""
    MINIO_ACCESS_KEY: str = ""
    MINIO_SECRET_KEY: str = ""
    MINIO_BUCKET: str = ""
    MINIO_SECURE: str = ""
    # RabbitMQ
    MQ_HOST: str = 'rabbitmq'
    MQ_PORT: int = 5672
    MQ_USER: str = 'rabbitmq'
    MQ_PASS: str = 'rabbitByDeverhood'
    MQ_EXCHANGE: str = ''
    MQ_EXCHANGE_TYPE: str = 'topic'
    MQ_QUEUE_NAME: str = "worker"
    # HTML2PDF
    HTML2PDF_BIN: str = "/usr/bin/wkhtmltopdf"
    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'



config = OrchardSettings()
