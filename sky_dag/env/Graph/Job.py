class Job:
    def __init__(self, job_id):
        self.id = job_id
        self.operations = []

    def add_operation(self, op):
        self.operations.append(op)

    def is_finished(self):
        return all(op.state == "finished" for op in self.operations)
