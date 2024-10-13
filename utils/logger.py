import logging
import os
from logging.handlers import QueueHandler

def get_logger(logfile=None, queue=None):
    """
    Custom logging method for system logs based on filename.
    If a queue is provided, logs are sent to the queue for centralized logging in multiprocessing.
    
    Parameters:
        logfile: Name of the file to store the logs (used by the listener process).
        queue: A multiprocessing queue to send log messages from worker processes.
    
    Returns:
        logger: Configured logger object.
    """
    logger = logging.getLogger(__name__)

    # Avoid adding multiple handlers by checking if any exist
    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)

        if queue:
            # In worker processes, log to the queue
            queue_handler = QueueHandler(queue)
            logger.addHandler(queue_handler)
        else:
            # In the main process, log to file and console
            logdir = "./tmp"
            if not os.path.exists(logdir):
                os.makedirs(logdir)

            log_file = os.path.join(logdir, logfile)
            c_handler = logging.StreamHandler()
            f_handler = logging.FileHandler(log_file, mode='w')
            log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

            c_handler.setLevel(logging.DEBUG)
            f_handler.setLevel(logging.DEBUG)
            c_handler.setFormatter(log_format)
            f_handler.setFormatter(log_format)

            logger.addHandler(c_handler)
            logger.addHandler(f_handler)

    logger.propagate = False  # Prevent propagation to the root logger

    if not queue:
        logger.info("Logs are stored in: {}".format(logfile))

    return logger
