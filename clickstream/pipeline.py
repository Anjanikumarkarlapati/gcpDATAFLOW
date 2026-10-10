"""Streaming clickstream aggregation pipeline.

Pub/Sub -> parse/validate -> event-time stamp -> dedupe -> late routing ->
5-min fixed windows (early/on-time/late triggers, ACCUMULATING) -> BigQuery.
"""
import argparse
import logging
import time

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import trigger
from apache_beam.transforms.deduplicate import DeduplicatePerKey
from apache_beam.transforms.window import FixedWindows, TimestampedValue

from clickstream import schemas
from clickstream.aggregation import EngagementCombineFn, FormatMetric
from clickstream.config import EVENT_TIME_ATTRIBUTE, WindowConfig
from clickstream.late import ON_TIME, TOO_LATE, RouteTooLate
from clickstream.parsing import DEAD_LETTER, VALID, ParseAndValidate


class ClickstreamOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser: argparse.ArgumentParser):
        # Not argparse-required so importing this module never breaks other
        # pipelines/tests; run() enforces them.
        parser.add_argument("--subscription", help="projects/<p>/subscriptions/<s>")
        parser.add_argument("--metrics_table", help="project:dataset.table")
        parser.add_argument("--dead_letter_table", help="project:dataset.table")
        parser.add_argument("--late_events_table", help="project:dataset.table")
        parser.add_argument("--window_seconds", type=int, default=WindowConfig.window_seconds)
        parser.add_argument("--early_firing_seconds", type=int,
                            default=WindowConfig.early_firing_seconds)
        parser.add_argument("--allowed_lateness_seconds", type=int,
                            default=WindowConfig.allowed_lateness_seconds)
        parser.add_argument("--dedup_retention_seconds", type=int,
                            default=WindowConfig.dedup_retention_seconds)


def process_clickstream(raw, cfg: WindowConfig = WindowConfig(), clock=None):
    """Core transform: PCollection[bytes] -> (metrics, late_events, dead_letters).

    Kept free of I/O so it can be exercised with TestStream in unit tests.
    """
    parsed = raw | "ParseValidate" >> beam.ParDo(ParseAndValidate(clock)).with_outputs(
        VALID, DEAD_LETTER)

    stamped = (
        parsed[VALID]
        | "AssignEventTime" >> beam.Map(lambda e: TimestampedValue(e, e["event_ts"]))
        | "KeyByEventId" >> beam.Map(lambda e: (e["event_id"], e)).with_output_types(
            tuple[str, dict])
        | "Deduplicate" >> DeduplicatePerKey(
            processing_time_duration=cfg.dedup_retention_seconds)
        | "DropEventId" >> beam.Values()
    )

    routed = stamped | "RouteTooLate" >> beam.ParDo(RouteTooLate(cfg, clock)).with_outputs(
        ON_TIME, TOO_LATE)

    metrics = (
        routed[ON_TIME]
        | "KeyByPage" >> beam.Map(lambda e: (e["page_url"], e))
        | "Window5Min" >> beam.WindowInto(
            FixedWindows(cfg.window_seconds),
            trigger=trigger.AfterWatermark(
                early=trigger.AfterProcessingTime(cfg.early_firing_seconds),
                late=trigger.AfterCount(1)),
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
            allowed_lateness=cfg.allowed_lateness_seconds)
        | "AggregateEngagement" >> beam.CombinePerKey(EngagementCombineFn(cfg.sketch_size))
        | "FormatMetric" >> beam.ParDo(FormatMetric(cfg.window_seconds, clock))
    )
    return metrics, routed[TOO_LATE], parsed[DEAD_LETTER]


def _write(pcoll, table, schema, label):
    return pcoll | label >> WriteToBigQuery(
        table,
        schema=schema,
        method=WriteToBigQuery.Method.STREAMING_INSERTS,
        create_disposition=BigQueryDisposition.CREATE_NEVER,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
        insert_retry_strategy="RETRY_ON_TRANSIENT_ERROR",
    )


def run(argv=None):
    options = PipelineOptions(argv)
    opts = options.view_as(ClickstreamOptions)
    options.view_as(StandardOptions).streaming = True
    missing = [n for n in ("subscription", "metrics_table", "dead_letter_table",
                           "late_events_table") if not getattr(opts, n)]
    if missing:
        raise SystemExit("missing required options: " + ", ".join("--" + n for n in missing))
    cfg = WindowConfig(
        window_seconds=opts.window_seconds,
        early_firing_seconds=opts.early_firing_seconds,
        allowed_lateness_seconds=opts.allowed_lateness_seconds,
        dedup_retention_seconds=opts.dedup_retention_seconds,
    )
    with beam.Pipeline(options=options) as p:
        raw = p | "ReadPubSub" >> beam.io.ReadFromPubSub(
            subscription=opts.subscription,
            # Watermark follows application event_time, not publish time.
            timestamp_attribute=EVENT_TIME_ATTRIBUTE)
        metrics, late, dead = process_clickstream(raw, cfg, clock=time.time)
        _write(metrics, opts.metrics_table, schemas.METRICS_SCHEMA, "WriteMetrics")
        _write(late, opts.late_events_table, schemas.LATE_EVENTS_SCHEMA, "WriteLateEvents")
        _write(dead, opts.dead_letter_table, schemas.DEAD_LETTER_SCHEMA, "WriteDeadLetters")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
