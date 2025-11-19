from opentelemetry import trace
from opentelemetry.trace import set_tracer_provider
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
)

def create_tracer(*, name:str, endpoint:str, resource_attributes: dict):
    resource = Resource.create(resource_attributes)
    tracer_provider = TracerProvider(resource=resource)
    set_tracer_provider(tracer_provider)
    exporter = OTLPSpanExporter(endpoint=endpoint)
    tracer_provider.add_span_processor(BatchSpanProcessor(exporter))
    return trace.get_tracer(name)
