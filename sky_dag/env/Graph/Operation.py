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

    # ----------正常运行阶段----------
    def progress(self):
        """
        执行相关的操作
        :return:
        """
        pass

    def step(self):
        if self.state == "running":
            self.progress += 1.0 / self.duration
            if self.progress >= 1.0:
                self.state = "finished"

    def check_dependencies(self):
        """
        判断前置条件是否满足
        :return:
        """
        pass

    def cal_qos(self):
        """
        查看运行质量
        :return:
        """
        pass

    # ----------重调度阶段----------
    def request(self):
        """
        请求资源
        :return:
        """
        pass

    def choose(self):
        """
        进行物理机的选择
        :return:
        """
        pass

    def assign_to_node(self, node):
        """
        分配到节点上
        :param node:
        :return:
        """
        self.assigned_node = node
        self.state = "running"