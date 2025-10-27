import time
from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class Metric:
    name: str
    value: float
    timestamp: float
    tags: dict[str, str] = field(default_factory=dict)


class MetricsCollector:
    _counters: dict[str, float]
    _gauges: dict[str, float]
    _histograms: dict[str, list[float]]
    _timers: dict[str, float]

    def __init__(self) -> None:
        self._counters = defaultdict(float)
        self._gauges = {}
        self._histograms = defaultdict(list)
        self._timers = {}

    def increment_counter(self, name: str, value: float = 1.0) -> None:
        self._counters[name] += value

    def set_gauge(self, name: str, value: float) -> None:
        self._gauges[name] = value

    def record_histogram(self, name: str, value: float) -> None:
        self._histograms[name].append(value)

    def start_timer(self, name: str) -> None:
        self._timers[name] = time.time()

    def stop_timer(self, name: str) -> float | None:
        if name not in self._timers:
            return None
        start_time = self._timers.pop(name)
        duration = time.time() - start_time
        self.record_histogram(f"{name}_duration", duration)
        return duration

    def get_counter(self, name: str) -> float:
        return self._counters.get(name, 0.0)

    def get_gauge(self, name: str) -> float | None:
        return self._gauges.get(name)

    def get_histogram(self, name: str) -> list[float]:
        return self._histograms.get(name, [])

    def get_histogram_stats(self, name: str) -> dict[str, float] | None:
        values = self._histograms.get(name, [])
        if not values:
            return None
        return {
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values),
            "count": len(values),
        }

    def get_all_metrics(self) -> dict[str, object]:
        return {
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "histograms": {
                name: self.get_histogram_stats(name) for name in self._histograms
            },
        }

    def reset(self) -> None:
        self._counters.clear()
        self._gauges.clear()
        self._histograms.clear()
        self._timers.clear()


_global_metrics = MetricsCollector()


def get_metrics_collector() -> MetricsCollector:
    return _global_metrics


__all__ = ["Metric", "MetricsCollector", "get_metrics_collector"]
