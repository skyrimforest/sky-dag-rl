import random

def generate_random_dag_config(
    num_nodes: int = 5,
    grid_size: tuple = (5, 5),
    max_successors: int = 2,
    min_delay: int = 1,
    max_delay: int = 3,
    min_speed: float = 0.2,
    max_speed: float = 0.5,
):
    nodes = []
    edges = []
    used_positions = set()

    # 创建节点
    for i in range(num_nodes):
        name = f"node_{i}"
        while True:
            pos = (random.randint(0, grid_size[0] - 1), random.randint(0, grid_size[1] - 1))
            if pos not in used_positions:
                used_positions.add(pos)
                break
        speed = round(random.uniform(min_speed, max_speed), 2)
        nodes.append({"name": name, "position": pos, "speed": speed})

    # 保证拓扑顺序：节点 i 只连接 i+1 ~ n 的节点，避免形成环
    for i in range(num_nodes):
        from_node = nodes[i]["name"]
        possible_targets = [nodes[j]["name"] for j in range(i + 1, num_nodes)]
        successors = random.sample(possible_targets, min(len(possible_targets), max_successors))
        for to_node in successors:
            delay = random.randint(min_delay, max_delay)
            edges.append({"from": from_node, "to": to_node, "delay": delay})

    return {
        "nodes": nodes,
        "edges": edges
    }
