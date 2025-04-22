from pettingzoo import ParallelEnv
from gymnasium import spaces
import numpy as np
from Node import Node


class SkyDagEnv(ParallelEnv):
    metadata = {"render_modes": ["human"], "name": "sky_dag_env"}

    def __init__(self, config=None):
        self.grid_size = ()
        self.nodes = {}
        self.edges = []

        self.pending_transfers = []
        self.step_count = 0

        self.agents = []
        self.possible_agents = []
        self.observation_spaces = {}
        self.action_spaces = {}

        if config:
            self._load_from_config(config)
        else:
            # 示例初始化
            self.grid_size=(5, 5)
            self.add_node("node_1", (1, 1), 0.3)
            self.add_node("node_2", (2, 1), 0.5)
            self.add_node("node_3", (3, 1), 0.4)
            self.connect_nodes("node_1", "node_2", delay=2)
            self.connect_nodes("node_2", "node_3", delay=1)

    def _load_from_config(self, config):
        """从配置字典中构建 DAG 图"""
        self.clear_config()
        for node_conf in config.get("nodes", []):
            self.add_node(
                node_conf["name"],
                tuple(node_conf["position"]),
                node_conf["speed"]
            )

        for edge_conf in config.get("edges", []):
            self.connect_nodes(
                edge_conf["from"],
                edge_conf["to"],
                edge_conf.get("delay", 1)
            )

    def clear_config(self):
        """清空现有的配置，包括节点、边和其他相关信息"""
        self.nodes.clear()
        self.edges.clear()
        self.agents.clear()
        self.possible_agents.clear()
        self.observation_spaces.clear()
        self.action_spaces.clear()
        self.pending_transfers.clear()
        self.step_count = 0

    def add_node(self, name, position, speed):
        """添加新节点并更新空间信息"""
        print(self.nodes)
        if name in self.nodes:
            raise ValueError(f"节点 {name} 已存在")
        new_node = Node(name, position, speed)
        self.nodes[name] = new_node
        self.agents.append(name)
        self.possible_agents.append(name)
        self.observation_spaces[name] = spaces.Box(0, 1, shape=(3,), dtype=np.float32)
        self.action_spaces[name] = spaces.Discrete(2)

    def connect_nodes(self, source_name, target_name, delay=1):
        """连接两个已存在的节点"""
        if source_name not in self.nodes or target_name not in self.nodes:
            raise ValueError("源或目标节点不存在")
        self.nodes[source_name].connect_to(self.nodes[target_name], delay)

    def reset(self, seed=None, options=None):
        for node in self.nodes.values():
            node.state = 'idle'
            node.progress = 0.0
        self.pending_transfers.clear()
        self.step_count = 0

        if "node_1" in self.nodes:
            self.nodes["node_1"].start()  # 默认启动 node_1
        return self._get_obs()

    def step(self, actions):
        rewards = {}
        terminations = {}
        truncations = {}
        infos = {}

        for name, action in actions.items():
            node = self.nodes[name]
            if action == 1:
                node.start()

        for node in self.nodes.values():
            triggered_edges = node.step()
            for edge in triggered_edges:
                due = self.step_count + edge.delay
                self.pending_transfers.append((due, edge.target_node))

        for due, node in list(self.pending_transfers):
            if due <= self.step_count:
                node.start()
        self.pending_transfers = [
            (due, node) for due, node in self.pending_transfers if due > self.step_count
        ]

        self.step_count += 1
        obs = self._get_obs()

        for name, node in self.nodes.items():
            rewards[name] = 1.0 if node.state == 'finished' else 0.0
            terminations[name] = node.state == 'finished'
            truncations[name] = False
            infos[name] = {}

        return obs, rewards, terminations, truncations, infos

    def _get_obs(self):
        obs = {}
        for name, node in self.nodes.items():
            state_code = {"idle": 0, "running": 0.5, "finished": 1.0}[node.state]
            obs[name] = np.array([
                state_code,
                min(node.progress, 1.0),
                float(len(node.successors))
            ], dtype=np.float32)
        return obs

    def render(self):
        print(f"\n=== Step {self.step_count} | Grid State ===")
        for name, node in self.nodes.items():
            print(f"{name} at {node.position}: {node.state} ({node.progress:.2f})")
