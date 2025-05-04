from abc import ABC, abstractmethod
import random

class BaseScheduler(ABC):
    @abstractmethod
    def assign(self, jobs, nodes) -> dict:
        """
        生成 operation -> node 的映射。
        :param jobs: 所有 job 列表
        :param nodes: 所有 node 列表
        :return: dict(op_id -> node_id)
        """
        pass

    @abstractmethod
    def to_str(self) -> str:
        """返回调度器名称"""
        pass

class RandomScheduler(BaseScheduler):
    def assign(self, jobs, nodes) -> dict:
        assignment = {}
        for job in jobs:
            for op in job.operations:
                node = random.choice(nodes)
                assignment[op.id] = node.id
        return assignment

    def to_str(self):
        return "random"


class FixedScheduler(BaseScheduler):
    def assign(self, jobs, nodes) -> dict:
        assignment = {}
        node_dict = {node.id: node for node in nodes}
        for job in jobs:
            for op in job.operations:
                if op.id in node_dict:
                    assignment[op.id] = op.id
                else:
                    print(f"[警告] 找不到与 Operation {op.id} 同名的 Node，跳过该操作。")
        return assignment

    def to_str(self):
        return "fixed"


class LoadBalancingScheduler(BaseScheduler):
    def assign(self, jobs, nodes) -> dict:
        raise NotImplementedError

    def to_str(self):
        return "load_balancing"

class HeuristicScheduler(BaseScheduler):
    def assign(self, jobs, nodes) -> dict:
        raise NotImplementedError

    def to_str(self):
        return "heuristic"

class MLScheduler(BaseScheduler):
    def assign(self, jobs, nodes) -> dict:
        raise NotImplementedError

    def to_str(self):
        return "ml_based"



