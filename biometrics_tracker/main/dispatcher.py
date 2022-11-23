import logging
import threading
import time

import schedule


class Dispatcher(threading.Thread):
    """
    Uses schedule library functionality to run jobs at the appropriate date and time
    """
    def __init__(self, logger: logging.Logger, sleep_seconds: int = 120):
        """
        Creates and instance of Runner

        :param logger: a logger instance
        :type logger: logging.Logger
        :param sleep_seconds: the number of seconds the process should sleep after checking for pending jobs
        :type sleep_seconds: int
        :return None

        """
        threading.Thread.__init__(self)
        self.logger = logger
        self.sleep_seconds = sleep_seconds
        self.quit = False

    def run(self):
        """
        Monitor and run pending jobs

        :return: None

        """
        self.logger.info('Starting Job Monitor')
        while not self.quit and schedule.idle_seconds() is not None:
            jobs = schedule.get_jobs()
            if jobs is not None and len(jobs) > 0:
                for job in jobs:
                    self.logger.debug(f'Scheduled Job: {job.__str__()}')
            else:
                self.logger.debug('No jobs retrieved')
            self.logger.debug('Check for pending jobs.')
            schedule.run_pending()
            time.sleep(self.sleep_seconds)
        if not self.quit:
            self.logger.info('No pending jobs, Job Monitor ending.')
        else:
            self.logger.info('Job Monitor ended.')

    def stop(self):
        self.quit = True
        self.logger.info('Initiated End Job Monitor.')
