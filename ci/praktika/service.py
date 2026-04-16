from abc import ABC, abstractmethod


class Service(ABC):
    """Abstract base class for long-running services managed during CI jobs.

    Concrete subclasses implement the three lifecycle methods that together
    cover the full service lifetime: `start`, `wait_ready`, and `shutdown`.
    """

    @abstractmethod
    def start(self) -> bool:
        """Launch the service process.

        Returns True when the process was started successfully (but not
        necessarily ready to serve requests yet — call `wait_ready` for that).
        """

    @abstractmethod
    def wait_ready(self) -> bool:
        """Block until the service is ready to accept requests.

        Returns True when the service is healthy, False if it fails to become
        ready within the expected time.
        """

    @abstractmethod
    def shutdown(self, force: bool = False) -> bool:
        """Stop the service.

        If *force* is True the service is killed immediately rather than asked
        to stop gracefully.  Returns True when the service has stopped.
        """
