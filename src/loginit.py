import structlog


class LogInitializer:
    def __init__(self):
        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.processors.add_log_level,
                structlog.processors.TimeStamper("iso"),
                structlog.dev.ConsoleRenderer(colors=True)
            ],
        )
        self.__logger = structlog.getLogger()

    @property
    def logger(self):
        return self.__logger
