import logging
import pathlib
from typing import Optional


class LoggingConfig:
    """
    Carries the logging configuration and allows the log file pathname and logging level to be
    updated.
    """
    def __init__(self, config: Optional[dict] = None):
        if config is None:
            self.config = {
                'version': 1,
                'formatters': {'standard': {'format': '%(asctime)s %(levelname)s %(funcName)s %(message)s'}
                               },
                'handlers': {'file-handler': {'class': 'logging.handlers.RotatingFileHandler',
                                              'filename': 'place holder',
                                              'formatter': 'standard',
                                              'level': logging.DEBUG
                                              },
                             },
                'loggers': {
                    'scheduled_entry': {
                        'handlers': ('file-handler',),
                        'level': logging.DEBUG,
                    },
                    'unit_tests': {
                        'handlers': ('file-handler',),
                        'level': logging.DEBUG,
                    }
                }
            }
        else:
            self.config = config

    def set_log_filename(self, filename: str) -> None:
        path = pathlib.Path(filename)
        if path.parent.exists():
            self.config['handlers']['file-handler']['filename'] = filename
        else:
            raise FileNotFoundError(f'Path {path.parent.__str__()} does not exist')

    def set_log_level(self, level) -> None:
        if level in [logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL, logging.DEBUG, logging.FATAL]:
            self.config['handlers']['file-handler']['level]'] = level
        else:
            raise ValueError(f'Invalid Log Level')




