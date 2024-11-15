from src.pipeline.execution import PipelineExecution

class PipelineManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(PipelineManager, cls).__new__(cls)
            cls._instance.pipelines = {}
            cls._instance.executions = {}
        return cls._instance

    def add_pipeline(self, pipeline):
        self.pipelines[pipeline.pipeline_id] = pipeline

    def get_pipeline(self, pipeline_id):
        return self.pipelines.get(pipeline_id)

    def delete_pipeline(self, pipeline_id):
        if pipeline_id in self.pipelines:
            del self.pipelines[pipeline_id]
            self.executions.pop(pipeline_id, None)

    def register_execution(self, pipeline_id):
        if pipeline_id not in self.pipelines:
            raise ValueError("Pipeline no encontrado")
        execution = PipelineExecution(pipeline_id)
        if pipeline_id not in self.executions:
            self.executions[pipeline_id] = []
        self.executions[pipeline_id].append(execution)
        return execution

    def get_execution_history(self, pipeline_id):
        return self.executions.get(pipeline_id, [])