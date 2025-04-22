import json
import random
import numpy as np
from sky_dag import SkyDagEnv  # 确保你把 SkyDagEnv 定义在这个模块里
from util import generate_random_dag_config  # 确保生成器在这个模块里

import matplotlib.pyplot as plt
import networkx as nx


def visualize_dag(config):
    G = nx.DiGraph()
    pos_map = {}

    for node in config["nodes"]:
        name = node["name"]
        G.add_node(name)
        pos_map[name] = node["position"]

    for edge in config["edges"]:
        G.add_edge(edge["from"], edge["to"], delay=edge["delay"])

    plt.figure(figsize=(6, 6))
    nx.draw(G, pos=pos_map, with_labels=True, node_size=1000, node_color='lightblue', arrowsize=20)
    edge_labels = {(e["from"], e["to"]): f'd{e["delay"]}' for e in config["edges"]}
    nx.draw_networkx_edge_labels(G, pos=pos_map, edge_labels=edge_labels)
    plt.title("Random DAG Task Graph")
    plt.grid(True)
    plt.show()


def debug_env():
    # 1. 生成随机 DAG 配置
    config = generate_random_dag_config(num_nodes=6, grid_size=(5, 5))

    # 2. 创建并加载环境
    env = SkyDagEnv(config)
    env.load_from_config(config)

    # 3. 可视化 DAG
    visualize_dag(config)

    # 4. 重置环境
    obs = env.reset()
    print("Initial Observations:")
    for agent, ob in obs.items():
        print(f"{agent}: {ob}")

    # 5. 多步运行环境
    for _ in range(10):
        actions = {agent: random.choice([0, 1]) for agent in env.agents}
        obs, rewards, terms, truncs, infos = env.step(actions)
        env.render()
        print("Actions:", actions)
        print("Rewards:", rewards)
        print("Terminated:", terms)


if __name__ == "__main__":
    debug_env()

