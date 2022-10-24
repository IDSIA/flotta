import os

STORAGE_ARTIFACTS: str = str(os.path.join('.', 'storage', 'artifacts'))
STORAGE_CLIENTS: str = str(os.path.join('.', 'storage', 'clients'))
STORAGE_MODELS: str = str(os.path.join('.', 'storage', 'models'))

FILE_CHUNK_SIZE: int = 4096

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'standard': {
            'format': '%(asctime)s %(name)48s %(levelname)7s %(message)s'
        }
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'stream': 'ext://sys.stdout',
        },
        'console_critical': {
            'level': 'ERROR',
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'stream': 'ext://sys.stdout',
        },
        'file': {
            'level': 'WARN',
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'standard',
            'filename': 'ferdelance.log',
            'maxBytes': 100000,
            'backupCount': 5,
        }
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': False,
        },
        'uvicorn': {
            'handlers': ['console_critical', 'file'],
            'level': 'INFO',
            'propagate': False,
        },
    },
}
