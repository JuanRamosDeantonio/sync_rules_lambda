import logging
import os

# Entorno puede ser 'local', 'lambda', 'test', etc.
ENVIRONMENT = os.getenv("ENVIRONMENT", "local")

def get_logger(name: str = "validator") -> logging.Logger:
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)

        # Handler de consola
        console_handler = logging.StreamHandler()

        # Formato estructurado para logs
        formatter = logging.Formatter(
            '[%(levelname)s] | %(asctime)s | módulo=%(module)s | línea=%(lineno)d | mensaje=%(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Para evitar duplicación de logs en entornos de testing
        logger.propagate = False

    return logger
