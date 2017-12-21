LOCAL_ID = "eltapos_001"
EXEC_INTERVAL = 15000
STOP_CHECK_INTERVAL = 500
REWRITE_MODULES = False


SERVER_DB = {
    "USER": "ffba_170908",
    "PASSWORD": "f__",
}


LOG_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "detail": {
            "format": "%(levelname)s: %(asctime)s %(module)s:%(lineno)d(%(funcName)s) %(message)s"
        },
    },
    "handlers": {
        "default": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "detail",
        },
        "file_handler": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "detail",
            "filename": "cashreg_service.log",
            "maxBytes": 262144,
            "backupCount": 4,
            "encoding": "UTF-8",
        },
        "smtp_handler": {
            "class": "cashreg_service.SSLSMTPHandler",
            "level": "ERROR",
            "formatter": "detail",
            "mailhost": ["smtp.yandex.com", 465],
            "fromaddr": "admin@eltacafe.ru",
            "toaddrs": ["admin@eltacafe.ru"],
            "subject": "Cashreg logging",
            "credentials": ["admin@eltacafe.ru", "ABCD12abcd"]
        },
        "db_handler": {
            "class": "cashreg_service.LogDbHandler",
            "parms": SERVER_DB,
            "local_id": LOCAL_ID,
            "formatter": "detail",
        },
    },
    "loggers": {
        "": {
            "handlers": ["default", "file_handler", "smtp_handler", "db_handler"],
            "level": "DEBUG",
            "propagate": True,
        },
    },
}
