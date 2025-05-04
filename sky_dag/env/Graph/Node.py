class Node:
    def __init__(self, node_id, position, cpu_capacity, mem_capacity):
        self.id = node_id
        self.position = position
        self.cpu_capacity = cpu_capacity
        self.mem_capacity = mem_capacity
        self.running_operations = []
        self.connections = []  # Underlay: 连接到的其他 Node，带传输延迟

    def connect_to(self, other_node, delay=1):
        self.connections.append({"target": other_node, "delay": delay})

    def can_run(self, op):
        used_cpu = sum(o.cpu_req for o in self.running_operations)
        used_mem = sum(o.mem_req for o in self.running_operations)
        return (used_cpu + op.cpu_req <= self.cpu_capacity and
                used_mem + op.mem_req <= self.mem_capacity)

    def assign_operation(self, op):
        if self.can_run(op):
            op.assign_to_node(self)
            self.running_operations.append(op)
            return True
        return False

    # ----------正常运行阶段----------
    def monitor(self):
        """
        监控资源
        :return:
        """
        pass

    def step(self):
        """
        单步运行
        :return:
        """
        finished_ops = []
        for op in self.running_operations:
            op.step()
            if op.state == "finished":
                finished_ops.append(op)
        self.running_operations = [
            op for op in self.running_operations if op.state != "finished"
        ]
        return finished_ops

    # ----------重调度阶段----------
    def available(self):
        """
        给出当前的可用资源
        :return:
        """
        pass

    def preempt(self):
        """
        抢占当前的物理机
        :return:
        """
        pass