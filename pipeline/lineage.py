"""OpenLineage events for each pipeline job.

Every job emits START, then COMPLETE (or FAIL), naming the datasets it read and wrote together with
their schemas and output row counts. Events are appended as JSON lines to a file; pointing the
OpenLineage client at Marquez or another backend instead is a transport change only.
"""
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import InputDataset, Job, OutputDataset, Run, RunEvent, RunState
from openlineage.client.facet_v2 import output_statistics_output_dataset, schema_dataset
from openlineage.client.transport.file import FileConfig, FileTransport
from pyspark.sql.types import StructType

PRODUCER = "https://github.com/guhankesav/spark-lineage-pipeline"
DATASET_NAMESPACE = "file"


def _schema_facet(schema: StructType | None) -> dict:
    if schema is None:
        return {}
    fields = [schema_dataset.SchemaDatasetFacetFields(name=f.name, type=f.dataType.simpleString()) for f in schema.fields]
    return {"schema": schema_dataset.SchemaDatasetFacet(fields=fields)}


@dataclass
class _JobRecorder:
    outputs: list[OutputDataset] = field(default_factory=list)

    def output(self, path: str, schema: StructType, row_count: int) -> None:
        self.outputs.append(OutputDataset(
            namespace=DATASET_NAMESPACE,
            name=path,
            facets=_schema_facet(schema),
            outputFacets={"outputStatistics": output_statistics_output_dataset.OutputStatisticsOutputDatasetFacet(
                rowCount=row_count)},
        ))


class Lineage:
    def __init__(self, log_path: str, job_namespace: str = "spark-lineage-pipeline"):
        self.client = OpenLineageClient(transport=FileTransport(FileConfig(log_file_path=log_path, append=True)))
        self.job_namespace = job_namespace

    @contextmanager
    def job(self, name: str, inputs: list[tuple[str, StructType | None]]):
        """Wraps one pipeline step. ``inputs`` are (path, schema) pairs; outputs are recorded on the yielded object."""
        run = Run(runId=str(uuid.uuid4()))
        job = Job(namespace=self.job_namespace, name=name)
        input_datasets = [InputDataset(namespace=DATASET_NAMESPACE, name=path, facets=_schema_facet(schema))
                          for path, schema in inputs]
        self._emit(RunState.START, run, job, input_datasets, [])
        recorder = _JobRecorder()
        try:
            yield recorder
        except Exception:
            self._emit(RunState.FAIL, run, job, input_datasets, recorder.outputs)
            raise
        self._emit(RunState.COMPLETE, run, job, input_datasets, recorder.outputs)

    def _emit(self, state, run, job, inputs, outputs) -> None:
        self.client.emit(RunEvent(
            eventType=state,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=run,
            job=job,
            producer=PRODUCER,
            inputs=inputs,
            outputs=outputs,
        ))
