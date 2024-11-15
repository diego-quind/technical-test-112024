from datetime import datetime
from src.enums.executionState import ExecutionState
import uuid

class PipelineExecution:
    def __init__(self, pipeline_id):
        self.execution_id = uuid.uuid4()
        self.pipeline_id = pipeline_id
        self.state = ExecutionState.IN_PROGRESS
        self.start_time = datetime.now()
        self.end_time = None
        self.log = []

    def complete_execution(self):
        self.state = ExecutionState.COMPLETED
        self.end_time = datetime.now()

    def fail_execution(self, error_message):
        self.state = ExecutionState.FAILED
        self.end_time = datetime.now()
        self.log.append(f"Error: {error_message}")

    def add_log(self, message):
        self.log.append(f"{datetime.now()} - {message}")