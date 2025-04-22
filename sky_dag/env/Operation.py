class Operation:
    def __init__(self, op_id, cpu_req, mem_req, duration):
        self.id = op_id
        self.cpu_req = cpu_req
        self.mem_req = mem_req
        self.duration = duration
        self.state = "pending"
        self.progress = 0.0
        self.dependencies = []
        self.assigned_node = None

    def add_dependency(self, op):
        self.dependencies.append(op)

    def is_ready(self):
        return all(dep.state == "finished" for dep in self.dependencies)

    def assign_to_node(self, node):
        self.assigned_node = node
        self.state = "running"

    def step(self):
        if self.state == "running":
            self.progress += 1.0 / self.duration
            if self.progress >= 1.0:
                self.state = "finished"
