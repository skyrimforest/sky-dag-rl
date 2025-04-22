class Node:
    def __init__(self, name, position, speed):
        self.name = name
        self.position = position
        self.speed = speed
        self.state = 'idle'
        self.progress = 0.0
        self.successors = []

    def connect_to(self, other_node, delay=1):
        self.successors.append(Edge(other_node, delay))

    def start(self):
        if self.state == 'idle':
            self.state = 'running'

    def step(self):
        triggered = []
        if self.state == 'running':
            self.progress += self.speed
            if self.progress >= 1.0:
                self.state = 'finished'
                triggered = self.successors
        return triggered


class Edge:
    def __init__(self, target_node, delay):
        self.target_node = target_node
        self.delay = delay
