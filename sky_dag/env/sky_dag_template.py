from pettingzoo import ParallelEnv


class SkyDagEnvironment(ParallelEnv):
    metadata = {
        "name": "sky_dag_v0",
    }

    def __init__(self):
        pass

    def reset(self, seed=None, options=None):
        pass

    def step(self, actions):
        pass

    def render(self):
        pass

    def observation_space(self, agent):
        return self.observation_spaces[agent]

    def action_space(self, agent):
        return self.action_spaces[agent]