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

class FixedScheduler(BaseScheduler):
    def assign(self, jobs, nodes) -> dict:
        assignment = {}
        node_dict = {node.id: node for node in nodes}  # 将节点转为字典方便查找
        for job in jobs:
            for op in job.operations:
                if op.id in node_dict:
                    assignment[op.id] = op.id  # 将 operation 分配给同名节点
                else:
                    print(f"[警告] 找不到与 Operation {op.id} 同名的 Node，跳过该操作的调度。")
                    # 或者 raise ValueError(f"无法为操作 {op.id} 找到匹配的节点。")
        return assignment

class LoadBalancingScheduler:
    pass

class HeuristicScheduler:
    pass

class MLScheduler:
    pass



