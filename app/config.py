import os

# === GitHub Config ===
GITHUB_REPO_URL = os.getenv("REPO_URL", "https://github.com/mi-org/mi-repo")
GITHUB_FILE_PATH = os.getenv("GITHUB_PATH", "rules/rules.xlsx")
GITHUB_BRANCH = os.getenv("REPO_BRANCH", "main")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

# Validación explícita para evitar fallos silenciosos
if not GITHUB_TOKEN:
    raise ValueError("❌ GITHUB_TOKEN no está definido como variable de entorno.")

# === S3 Config ===
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "bucket-validaciones")
S3_RULES_OBJECT_KEY = os.getenv("S3_RULES_OBJECT_KEY", "rules/rulesmetadata.json")
S3_KEY = os.getenv("RULES_KEY", "rules_metadata.json")
S3_HASH_OBJECT_KEY = os.getenv("S3_HASH_OBJECT_KEY", "rules/rules.hash")  # ahora configurable

# === Regla por defecto ===
DEFAULT_RULE_TYPE = os.getenv("RULE_TYPE", "semantica")

# === Entorno ===
IS_LAMBDA = os.getenv("IS_LAMBDA", "false").lower() in ("true", "1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("us-east-1")