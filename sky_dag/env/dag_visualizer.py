import pygame
import sys

# 颜色定义
WHITE = (255, 255, 255)
BLACK = (0, 0, 0)
GRAY = (200, 200, 200)
GREEN = (0, 200, 0)
BLUE = (100, 100, 255)
RED = (200, 50, 50)

NODE_RADIUS = 25

def draw_arrow(surface, start, end, color=BLACK, width=2):
    pygame.draw.line(surface, color, start, end, width)
    # 箭头头部
    dx, dy = end[0]-start[0], end[1]-start[1]
    length = max((dx**2 + dy**2)**0.5, 0.001)
    unit_dx, unit_dy = dx/length, dy/length
    arrow_size = 10
    left = (end[0] - arrow_size * (unit_dx + unit_dy),
            end[1] - arrow_size * (unit_dy - unit_dx))
    right = (end[0] - arrow_size * (unit_dx - unit_dy),
             end[1] - arrow_size * (unit_dy + unit_dx))
    pygame.draw.polygon(surface, color, [end, left, right])


def visualize_env(env, scale=100, fps=1):
    pygame.init()
    width, height = env.grid_size[0] * scale, env.grid_size[1] * scale
    screen = pygame.display.set_mode((width, height))
    pygame.display.set_caption("SkyDagEnv Visualizer")
    clock = pygame.time.Clock()

    running = True
    while running:
        screen.fill(WHITE)

        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False

        # 绘制连接线
        for node in env.nodes.values():
            x1, y1 = node.position[0] * scale, node.position[1] * scale
            for edge in node.successors:
                x2, y2 = edge.target_node.position[0] * scale, edge.target_node.position[1] * scale
                draw_arrow(screen, (x1, y1), (x2, y2), GRAY)

        # 绘制节点
        for name, node in env.nodes.items():
            x, y = node.position[0] * scale, node.position[1] * scale
            color = {
                "idle": GRAY,
                "running": BLUE,
                "finished": GREEN
            }.get(node.state, RED)

            pygame.draw.circle(screen, color, (x, y), NODE_RADIUS)
            font = pygame.font.SysFont(None, 24)
            text = font.render(name, True, BLACK)
            screen.blit(text, (x - NODE_RADIUS, y - NODE_RADIUS - 18))

        pygame.display.flip()
        clock.tick(fps)

    pygame.quit()
    sys.exit()
