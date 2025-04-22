import random

class BaseScheduler:
    def assign(self, jobs, nodes) -> dict:
        """
        生成 operation -> node 的映射。
        :param jobs: 所有 job 列表
        :param nodes: 所有 node 列表
        :return: dict(op_id -> node_id)
        """
        raise NotImplementedError


class RandomScheduler(BaseScheduler):
    def assign(self, jobs, nodes) -> dict:
        assignment = {}
        for job in jobs:
            for op in job.operations:
                node = random.choice(nodes)
                assignment[op.id] = node.id
        return assignment


class LoadBalancingScheduler:
    pass

class HeuristicScheduler:
    pass

class MLScheduler:
    pass



