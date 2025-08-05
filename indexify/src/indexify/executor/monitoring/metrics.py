import prometheus_client

_INF = float("inf")


def latency_metric_for_fast_operation(
    operation_name: str, operation_description: str
) -> prometheus_client.Histogram:
    """Creates a histogram metric for latency of a fast operation.

    A fast operation is typically expected to complete in a few tens of milliseconds."""
    return prometheus_client.Histogram(
        f"{operation_name}_latency_seconds",
        f"Latency of {operation_description}",
        # Buckets are in seconds
        buckets=[
            _ms_to_sec(1),
            _ms_to_sec(2),
            _ms_to_sec(3),
            _ms_to_sec(4),
            _ms_to_sec(5),
            _ms_to_sec(6),
            _ms_to_sec(7),
            _ms_to_sec(8),
            _ms_to_sec(9),
            _ms_to_sec(10),
            _ms_to_sec(15),
            _ms_to_sec(20),
            _ms_to_sec(25),
            _ms_to_sec(30),
            _ms_to_sec(35),
            _ms_to_sec(40),
            _ms_to_sec(45),
            _ms_to_sec(50),
            _ms_to_sec(55),
            _ms_to_sec(60),
            _ms_to_sec(65),
            _ms_to_sec(70),
            _ms_to_sec(75),
            _ms_to_sec(80),
            _ms_to_sec(85),
            _ms_to_sec(90),
            _ms_to_sec(95),
            _ms_to_sec(100),
            _ms_to_sec(150),
            _ms_to_sec(200),
            _ms_to_sec(250),
            _ms_to_sec(300),
            _ms_to_sec(350),
            _ms_to_sec(400),
            _ms_to_sec(450),
            _ms_to_sec(500),
            _ms_to_sec(550),
            _ms_to_sec(600),
            _ms_to_sec(650),
            _ms_to_sec(700),
            _ms_to_sec(750),
            _ms_to_sec(800),
            _ms_to_sec(850),
            _ms_to_sec(900),
            _ms_to_sec(950),
            1,
            _ms_to_sec(1050),
            _ms_to_sec(1100),
            _ms_to_sec(1150),
            _ms_to_sec(1200),
            _ms_to_sec(1250),
            _ms_to_sec(1300),
            _ms_to_sec(1350),
            _ms_to_sec(1400),
            _ms_to_sec(1450),
            _ms_to_sec(1500),
            _ms_to_sec(1550),
            _ms_to_sec(1600),
            _ms_to_sec(1650),
            _ms_to_sec(1700),
            _ms_to_sec(1750),
            _ms_to_sec(1800),
            _ms_to_sec(1850),
            _ms_to_sec(1900),
            _ms_to_sec(1950),
            2,
            _ms_to_sec(2100),
            _ms_to_sec(2200),
            _ms_to_sec(2300),
            _ms_to_sec(2400),
            _ms_to_sec(2500),
            _ms_to_sec(2600),
            _ms_to_sec(2700),
            _ms_to_sec(2800),
            _ms_to_sec(2900),
            3,
            _ms_to_sec(3500),
            4,
            _ms_to_sec(4500),
            5,
            10,
            60,
            _INF,
        ],
    )


def latency_metric_for_slow_operation(
    operation_name: str, operation_description: str
) -> prometheus_client.Histogram:
    """Creates a histogram metric for latency of a slow operation.

    A slow operation is typically expected to complete within a few tens of seconds."""
    return prometheus_client.Histogram(
        f"{operation_name}_latency_seconds",
        f"Latency of {operation_description}",
        # Buckets are in seconds
        buckets=[
            _ms_to_sec(10),
            _ms_to_sec(20),
            _ms_to_sec(30),
            _ms_to_sec(40),
            _ms_to_sec(50),
            _ms_to_sec(60),
            _ms_to_sec(70),
            _ms_to_sec(80),
            _ms_to_sec(90),
            _ms_to_sec(100),
            _ms_to_sec(200),
            _ms_to_sec(300),
            _ms_to_sec(400),
            _ms_to_sec(500),
            _ms_to_sec(600),
            _ms_to_sec(700),
            _ms_to_sec(800),
            _ms_to_sec(900),
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            15,
            20,
            25,
            30,
            35,
            40,
            45,
            50,
            _minutes_to_sec(1),
            _minutes_to_sec(2),
            _minutes_to_sec(3),
            _minutes_to_sec(4),
            _minutes_to_sec(5),
            _minutes_to_sec(6),
            _minutes_to_sec(7),
            _minutes_to_sec(8),
            _minutes_to_sec(9),
            _minutes_to_sec(10),
            _minutes_to_sec(20),
            _INF,
        ],
    )


def latency_metric_for_customer_controlled_operation(
    operation_name: str, operation_description: str
) -> prometheus_client.Histogram:
    """Creates a histogram metric for latency of a customer controlled operation.

    Example of a customer controlled operation is executing customer code.
    The buckets in this histrogram tend to be quite large because we don't
    know how long customer code will take to execute and because we don't need
    a precise understanding of its duration."""
    return prometheus_client.Histogram(
        f"{operation_name}_latency_seconds",
        f"Latency of {operation_description}",
        # Buckets are in seconds
        buckets=[
            _ms_to_sec(1),
            _ms_to_sec(5),
            _ms_to_sec(50),
            _ms_to_sec(100),
            _ms_to_sec(200),
            _ms_to_sec(300),
            _ms_to_sec(400),
            _ms_to_sec(500),
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            15,
            20,
            30,
            40,
            50,
            _minutes_to_sec(1),
            _minutes_to_sec(2),
            _minutes_to_sec(3),
            _minutes_to_sec(4),
            _minutes_to_sec(5),
            _minutes_to_sec(6),
            _minutes_to_sec(7),
            _minutes_to_sec(8),
            _minutes_to_sec(9),
            _minutes_to_sec(10),
            _minutes_to_sec(20),
            _minutes_to_sec(30),
            _minutes_to_sec(40),
            _minutes_to_sec(50),
            _hours_to_sec(1),
            _hours_to_sec(2),
            _hours_to_sec(3),
            _hours_to_sec(4),
            _hours_to_sec(5),
            _hours_to_sec(10),
            _hours_to_sec(20),
            _days_to_sec(1),
            _days_to_sec(2),
            _INF,
        ],
    )


def _ms_to_sec(ms: int) -> float:
    return ms / 1000.0


def _minutes_to_sec(minutes: int) -> float:
    return minutes * 60.0


def _hours_to_sec(hours: int) -> float:
    return _minutes_to_sec(hours * 60)


def _days_to_sec(days: int) -> float:
    return _hours_to_sec(days * 24)


class IdempotentCounterChanger:
    """A wrapper that ensures that inc/dec operations on a counter are done only once.

    This is useful for tracking the number of in-progress operations or objects that exist.
    """

    def __init__(self, counter: prometheus_client.Counter) -> None:
        self._counter: prometheus_client.Counter = counter
        self.is_incremented: bool = False
        self.is_decremented: bool = False

    def inc(self) -> None:
        if not self.is_incremented:
            self._counter.inc()
            self.is_incremented = True

    def dec(self) -> None:
        if self.is_incremented and not self.is_decremented:
            self._counter.dec()
            self.is_decremented = True
