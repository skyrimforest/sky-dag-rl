from typing import Union

from pettingzoo import ParallelEnv
import numpy as np

from sky_dag.env.Agent.BaseAgent import BaseAgent
from sky_dag.env.Graph.Node import Node
from sky_dag.env.Graph.Job import Job
from sky_dag.env.Graph.Operation import Operation
from sky_dag.env.Scheduler.BaseScheduler import BaseScheduler,FixedScheduler
import json


class SkyDagEnv(ParallelEnv):
    metadata = {"render_modes": ["human"], "name": "sky_dag_env"}

    def __init__(self,
                 node_config_path="node_config.json",
                 job_config_path="job_config.json",
                 assign_scheduler=None,
                 graph_agent: BaseAgent = None,
                 underlay_agent: BaseAgent = None,
                 overlay_agent: BaseAgent = None,
                 operation_agent: Union[BaseAgent, list] = None,
                 node_agent: Union[BaseAgent, list] = None,
                 ):
        self.node_config_path = node_config_path
        self.job_config_path = job_config_path

        # 模拟源点和终点
        self.source=None
        self.destination=None

        # underlay状态
        self.nodes = {}
        self.grid_size = ()

        # overlay状态
        self.jobs = {}
        self.operations = []

        # 环境本身的状态
        self.env_timeline = 0
        self.reward=0

        # 智能体相关的状态
        self.graph_agent = None
        self.underlay_agent = None
        self.overlay_agent = None
        self.operation_agents = []
        self.node_agents = []

        # 调度器
        self.scheduler = None
        if assign_scheduler is None:
            self.scheduler = FixedScheduler()  # 默认调度器，可替换
        else:
            self.set_scheduler(assign_scheduler)
            self.scheduler = assign_scheduler

    # ---------- 自定义状态更新函数 ----------
    def set_env_timeline(self, count):
        assert count >= 0 and isinstance(count, int)
        self.env_timeline = count

    def get_env_timeline(self) -> int:
        return self.env_timeline

    def set_scheduler(self, scheduler):
        assert isinstance(scheduler, BaseScheduler)
        self.scheduler = scheduler

    def get_scheduler(self) -> str:
        return self.scheduler.to_str()

    # Scheduler的任务
    # def assign_operations(self, job_assignment: Dict[str, str]):
    #     """
    #     job_assignment: {"op_1": "node_1", "op_2": "node_2", ...}
    #     """
    #     for job in self.jobs:
    #         for op in job.operations:
    #             if op.id in job_assignment:
    #                 node_id = job_assignment[op.id]
    #                 node = self.nodes[node_id]
    #                 op.assign_to_node(node)

    # Scheduler的任务
    # def run_scheduling_cycle(self):
    #     """
    #     实际执行指派的结果
    #     :return: None
    #     """
    #     assignment = self.scheduler.assign(self.jobs, self.nodes)
    #     self.assign_operations(assignment)

    def refresh_underlay(self):
        """
        刷新当前环境的underlay
        :return:
        """
        self.nodes.clear()

    def refresh_overlay(self):
        """
        刷新当前环境的overlay
        :return:
        """
        self.jobs.clear()
        self.operations.clear()

    def clear_graph(self):
        """
        清空状态
        :return:None
        """
        # ----------初始化图本身----------
        self.refresh_overlay()
        self.refresh_underlay()

        print("Environment Initialized Successfully.")

    def load_node_graph(self):
        """
        读取并创建underlay图
        :return: None
        """
        with open(self.node_config_path, 'r') as f:
            config = json.load(f)
        for node_cfg in config['nodes']:
            node = Node(
                node_id=node_cfg['id'],
                position=tuple(node_cfg['position']),
                cpu_capacity=node_cfg['cpu'],
                mem_capacity=node_cfg['memory']
            )
            self.nodes[node.id] = node

        for link in config.get('links', []):
            self.nodes[link['from']].connect_to(self.nodes[link['to']], link['delay'])

    def load_job_graph(self):
        """
        读取job图的信息
        :return: None
        """
        with open(self.job_config_path, 'r') as f:
            config = json.load(f)

        for job_cfg in config['jobs']:
            job = Job(job_cfg['id'])  # 确保创建的是 Job 实例
            op_dict = {}

            # 创建 operation 实例并加入 job
            for op_cfg in job_cfg['operations']:
                op = Operation(
                    op_id=op_cfg['id'],
                    cpu_req=op_cfg['cpu_req'],
                    mem_req=op_cfg['mem_req'],
                    duration=op_cfg['duration']
                )
                job.add_operation(op)
                op_dict[op.id] = op
                self.operations.append(op)

            # 建立操作之间的依赖关系
            for dep in job_cfg.get('dependencies', []):
                from_op = op_dict[dep['from']]
                to_op = op_dict[dep['to']]
                to_op.add_dependency(from_op)

            self.jobs[job.id] = job  # 存储 Job 对象，确保是 Job 类型

        # 初始化待运行操作列表（那些已经准备好、没有未完成依赖的操作）
        self.pending_operations = [op for op in self.operations if op.is_ready()]

    def step(self, actions=None):
        """
        执行一步仿真逻辑。当前版本为纯仿真环境，不包含 agent 与动作，op 自动分配到 node 上执行。
        :param actions: None（非 RL 模式）
        :return: obs, rewards, terminations, truncations, infos
        """
        rewards = {}
        terminations = {}
        truncations = {}
        infos = {}

        # === 1. 分配待调度的操作到可用节点 ===
        for op in self.pending_operations[:]:  # 拷贝列表避免循环中修改
            for node in self.nodes.values():
                if node.assign_operation(op):  # 成功分配则移除
                    self.pending_operations.remove(op)
                    break

        # === 2. 推进每个节点上的操作执行 ===
        for node in self.nodes.values():
            finished_ops = node.step()  # 返回已完成的操作列表

            for op in finished_ops:
                # 更新其后继操作的依赖状态
                for next_op in op.successors:
                    next_op.mark_dependency_finished(op.id)
                    # 若下一个操作就绪则加入待调度队列
                    if next_op.is_ready():
                        self.pending_operations.append(next_op)

        # === 3. 步数更新 ===
        self.set_env_timeline(self.env_timeline+1)

        # === 4. 构造返回值 ===
        obs = self._get_obs()
        for agent in self.agents:
            rewards[agent] = 0.0  # 你可以根据实际逻辑修改为奖励函数
            terminations[agent] = self._check_done(agent)  # 可选：某个 node 完成所有任务
            truncations[agent] = False  # 暂无截断逻辑
            infos[agent] = {}

        return obs, rewards, terminations, truncations, infos

    def _get_obs(self):
        """
        获得环境观察的信息
        :return: 物理节点的观察信息
        """
        obs = {}
        for name, node in self.nodes.items():
            cpu_load = sum(o.cpu_req for o in node.running_operations) / node.cpu_capacity
            mem_load = sum(o.mem_req for o in node.running_operations) / node.mem_capacity
            obs[name] = np.array([
                min(cpu_load, 1.0),
                min(mem_load, 1.0),
                len(node.running_operations)
            ], dtype=np.float32)
        return obs

    def reset(self, seed=None, options=None):
        """
        返回环境初始状态下智能体的观察值,使环境整体的状态回到开始时的样子。
        :param seed: 随机种子,复现实验
        :param options:选项
        :return:
        """
        # ---------- 清理阶段 ----------
        self.set_env_timeline(0)
        self.clear_graph()
        # ---------- 重建阶段 ----------
        self.load_node_graph()
        self.load_job_graph()
        obs = self._get_obs()
        return obs

    def render(self):
        """
        给出观察的渲染结果
        :return:
        """
        print(f"\n=== Step {self.get_env_timeline()} | Grid State ===")
        for name, node in self.nodes.items():
            print(f"{name} at {node.position}: {len(node.running_operations)} ops running")

    def print_all_status(self):
        """
        打印所有节点、作业、操作及其当前状态
        :return: None
        """
        print("\n[节点状态]")
        for node_id, node in self.nodes.items():
            print(
                f"Node {node_id} - CPU: {node.cpu_capacity}, Memory: {node.mem_capacity}, Connections: {len(node.connections)}")

        print("\n[作业状态]")
        for job_id, job in self.jobs.items():
            print(f"Job {job_id} - Operations: {len(job.operations)}")
            for op in job.operations:
                print(
                    f"  Operation {op.id} - State: {op.state}, Assigned Node: {op.assigned_node.id if op.assigned_node else 'None'}")

        print("\n[待运行操作]")
        for op in self.pending_operations:
            print(f"  Operation {op.id} - State: {op.state}")

    def observation_space(self, agent):
        return self.observation_spaces[agent]

    def action_space(self, agent):
        return self.action_spaces[agent]
