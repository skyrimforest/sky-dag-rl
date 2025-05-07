class Event:
    def __init__(self, timestamp, event_type, payload):
        self.timestamp = timestamp  # 事件发生的时间
        self.event_type = event_type  # 如 'task_finish', 'machine_fail'
        self.payload = payload  # 可以是 Operation 或 Job 等对象

class EventQueue:
    def __init__(self):
        self.events = []

    def add_event(self, event: Event):
        self.events.append(event)
        self.events.sort(key=lambda e: e.timestamp)

    def pop_ready_events(self, current_time):
        ready = [e for e in self.events if e.timestamp <= current_time]
        self.events = [e for e in self.events if e.timestamp > current_time]
        return ready