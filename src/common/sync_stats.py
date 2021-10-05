import abc
from abc import ABC


class SyncStats(ABC):
    def __init__(self, initial_event_counts):
        self.event_counts = initial_event_counts

    def add_event(self, event):
        if event not in self.event_counts:
            self.event_counts[event] = 0
        self.event_counts[event] += 1

    def add_events(self, events):
        for event in events:
            self.add_event(event)

    def add_stats(self, stats):
        for k, v in stats.event_counts.items():
            self.event_counts[k] += v

    @abc.abstractmethod
    def print_summary(self):
        pass
