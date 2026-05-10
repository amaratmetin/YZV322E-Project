"""Generate kibana/saved_objects.ndjson.

Run this whenever the dashboard layout changes:

    python kibana/build_saved_objects.py

The output file is checked into git and is what `kibana-init` uploads on
`docker compose up`.
"""

from __future__ import annotations

import json
from pathlib import Path


MEASUREMENTS_PATTERN_ID = "aq-measurements-pattern"
SUMMARIES_PATTERN_ID = "aq-daily-summaries-pattern"
DASHBOARD_ID = "tokyo-air-quality-dashboard"
PARAMETER_TERMS_SIZE = 20

OUTPUT_PATH = Path(__file__).parent / "saved_objects.ndjson"


def index_pattern(object_id: str, title: str, time_field: str) -> dict:
    return {
        "id": object_id,
        "type": "index-pattern",
        "attributes": {"title": title, "timeFieldName": time_field},
        "references": [],
    }


def visualization(object_id: str, title: str, vis_state: dict, pattern_id: str, query: str = "") -> dict:
    search_source = {
        "query": {"query": query, "language": "kuery"},
        "filter": [],
        "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
    }
    return {
        "id": object_id,
        "type": "visualization",
        "attributes": {
            "title": title,
            "visState": json.dumps(vis_state, separators=(",", ":")),
            "uiStateJSON": "{}",
            "description": "",
            "version": 1,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps(search_source, separators=(",", ":")),
            },
        },
        "references": [
            {
                "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                "type": "index-pattern",
                "id": pattern_id,
            }
        ],
    }


def parameter_query(parameters: list[str]) -> str:
    return " or ".join(f'parameter : "{parameter}"' for parameter in parameters)


def line_vis_state(title: str, metric_field: str, time_field: str, group_field: str, y_axis_title: str) -> dict:
    return {
        "title": title,
        "type": "line",
        "aggs": [
            {"id": "1", "enabled": True, "type": "avg", "schema": "metric", "params": {"field": metric_field}},
            {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment", "params": {"field": time_field, "interval": "auto"}},
            {"id": "3", "enabled": True, "type": "terms", "schema": "group", "params": {"field": group_field, "size": PARAMETER_TERMS_SIZE, "order": "desc", "orderBy": "1"}},
        ],
        "params": {
            "type": "line",
            "grid": {"categoryLines": False},
            "addLegend": True,
            "legendPosition": "right",
            "addTooltip": True,
            "categoryAxes": [
                {
                    "id": "CategoryAxis-1",
                    "type": "category",
                    "position": "bottom",
                    "show": True,
                    "scale": {"type": "linear"},
                    "labels": {"show": True, "truncate": 100},
                    "title": {},
                }
            ],
            "valueAxes": [
                {
                    "id": "ValueAxis-1",
                    "name": "LeftAxis-1",
                    "type": "value",
                    "position": "left",
                    "show": True,
                    "scale": {"type": "linear", "mode": "normal"},
                    "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                    "title": {"text": y_axis_title},
                }
            ],
            "seriesParams": [
                {
                    "show": True,
                    "type": "line",
                    "mode": "normal",
                    "data": {"label": y_axis_title, "id": "1"},
                    "valueAxis": "ValueAxis-1",
                    "drawLinesBetweenPoints": True,
                    "showCircles": True,
                }
            ],
        },
    }


def pie_vis_state(title: str, group_field: str) -> dict:
    return {
        "title": title,
        "type": "pie",
        "aggs": [
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "segment", "params": {"field": group_field, "size": PARAMETER_TERMS_SIZE, "order": "desc", "orderBy": "1"}},
        ],
        "params": {"type": "pie", "addTooltip": True, "addLegend": True, "legendPosition": "right", "isDonut": True},
    }


def bar_vis_state(title: str, group_field: str, size: int = 10) -> dict:
    return {
        "title": title,
        "type": "histogram",
        "aggs": [
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "segment", "params": {"field": group_field, "size": size, "order": "desc", "orderBy": "1"}},
        ],
        "params": {
            "type": "histogram",
            "grid": {"categoryLines": False},
            "addLegend": False,
            "addTooltip": True,
            "categoryAxes": [
                {
                    "id": "CategoryAxis-1",
                    "type": "category",
                    "position": "bottom",
                    "show": True,
                    "scale": {"type": "linear"},
                    "labels": {"show": True, "truncate": 100, "rotate": 30},
                    "title": {},
                }
            ],
            "valueAxes": [
                {
                    "id": "ValueAxis-1",
                    "name": "LeftAxis-1",
                    "type": "value",
                    "position": "left",
                    "show": True,
                    "scale": {"type": "linear", "mode": "normal"},
                    "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                    "title": {"text": "Measurement count"},
                }
            ],
            "seriesParams": [
                {
                    "show": True,
                    "type": "histogram",
                    "mode": "stacked",
                    "data": {"label": "Count", "id": "1"},
                    "valueAxis": "ValueAxis-1",
                    "drawLinesBetweenPoints": True,
                    "showCircles": True,
                }
            ],
        },
    }


def metric_vis_state(title: str) -> dict:
    return {
        "title": title,
        "type": "metric",
        "aggs": [{"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}}],
        "params": {
            "addTooltip": True,
            "addLegend": False,
            "type": "metric",
            "metric": {
                "percentageMode": False,
                "useRanges": False,
                "colorSchema": "Green to Red",
                "metricColorMode": "None",
                "colorsRange": [{"from": 0, "to": 10000}],
                "labels": {"show": True},
                "invertColors": False,
                "style": {"bgFill": "#000", "bgColor": False, "labelColor": False, "subText": "", "fontSize": 60},
            },
        },
    }


def panel(panel_index: int, x: int, y: int, w: int, h: int, viz_id: str, title: str) -> dict:
    return {
        "version": "8.15.0",
        "type": "visualization",
        "gridData": {"x": x, "y": y, "w": w, "h": h, "i": str(panel_index)},
        "panelIndex": str(panel_index),
        "embeddableConfig": {"enhancements": {}},
        "panelRefName": f"panel_{panel_index}",
        "title": title,
    }


def dashboard(panels: list[dict], references: list[dict]) -> dict:
    panels_json = json.dumps(panels, separators=(",", ":"))
    search_source = {"query": {"query": "", "language": "kuery"}, "filter": []}
    return {
        "id": DASHBOARD_ID,
        "type": "dashboard",
        "attributes": {
            "title": "Tokyo Air Quality",
            "description": "Hourly measurements and daily summaries from OpenAQ Tokyo sensors.",
            "panelsJSON": panels_json,
            "optionsJSON": json.dumps({"useMargins": True, "syncColors": False, "hidePanelTitles": False}),
            "version": 1,
            "timeRestore": True,
            "timeFrom": "2026-04-01T00:00:00.000Z",
            "timeTo": "2026-05-01T00:00:00.000Z",
            "refreshInterval": {"pause": True, "value": 0},
            "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps(search_source, separators=(",", ":"))},
        },
        "references": references,
    }


def main() -> None:
    objects: list[dict] = []

    objects.append(index_pattern(MEASUREMENTS_PATTERN_ID, "aq-measurements*", "measurement_time_utc"))
    objects.append(index_pattern(SUMMARIES_PATTERN_ID, "aq-daily-summaries*", "summary_date"))

    objects.append(
        visualization(
            "viz-count-by-parameter",
            "Measurement count by parameter",
            pie_vis_state("Measurement count by parameter", group_field="parameter"),
            MEASUREMENTS_PATTERN_ID,
        )
    )

    objects.append(
        visualization(
            "viz-top-locations",
            "Top locations by measurement count",
            bar_vis_state("Top locations by measurement count", group_field="location_name", size=10),
            MEASUREMENTS_PATTERN_ID,
        )
    )

    objects.append(
        visualization(
            "viz-total-measurements",
            "Total measurements",
            metric_vis_state("Total measurements"),
            MEASUREMENTS_PATTERN_ID,
        )
    )

    objects.append(
        visualization(
            "viz-daily-avg-gases",
            "Daily average: gases only",
            line_vis_state(
                "Daily average: gases only",
                metric_field="avg_value",
                time_field="summary_date",
                group_field="parameter",
                y_axis_title="Daily average (ppm)",
            ),
            SUMMARIES_PATTERN_ID,
            query=parameter_query(["co", "no", "no2", "nox", "so2"]),
        )
    )

    objects.append(
        visualization(
            "viz-daily-avg-particles",
            "Daily average: particles only",
            line_vis_state(
                "Daily average: particles only",
                metric_field="avg_value",
                time_field="summary_date",
                group_field="parameter",
                y_axis_title="Daily average",
            ),
            SUMMARIES_PATTERN_ID,
            query=parameter_query(["pm1", "pm25", "um003"]),
        )
    )

    objects.append(
        visualization(
            "viz-daily-avg-weather",
            "Daily average: weather/other only",
            line_vis_state(
                "Daily average: weather/other only",
                metric_field="avg_value",
                time_field="summary_date",
                group_field="parameter",
                y_axis_title="Daily average",
            ),
            SUMMARIES_PATTERN_ID,
            query=parameter_query(["temperature", "relativehumidity"]),
        )
    )

    panel_definitions = [
        ("viz-total-measurements", "Total measurements", 0, 0, 12, 6),
        ("viz-count-by-parameter", "Measurement count by parameter", 12, 0, 24, 12),
        ("viz-daily-avg-gases", "Daily average: gases only", 0, 6, 16, 12),
        ("viz-daily-avg-particles", "Daily average: particles only", 16, 6, 16, 12),
        ("viz-daily-avg-weather", "Daily average: weather/other only", 32, 6, 16, 12),
        ("viz-top-locations", "Top locations by measurement count", 0, 18, 48, 12),
    ]
    panels: list[dict] = []
    references: list[dict] = []
    for index, (viz_id, title, x, y, w, h) in enumerate(panel_definitions, start=1):
        panels.append(panel(index, x, y, w, h, viz_id, title))
        references.append({"name": f"panel_{index}", "type": "visualization", "id": viz_id})

    objects.append(dashboard(panels, references))

    summary = {
        "exportedCount": len(objects),
        "missingRefCount": 0,
        "missingReferences": [],
        "excludedObjects": [],
        "excludedObjectsCount": 0,
    }

    with OUTPUT_PATH.open("w", encoding="utf-8") as handle:
        for obj in objects:
            handle.write(json.dumps(obj, separators=(",", ":")))
            handle.write("\n")
        handle.write(json.dumps(summary, separators=(",", ":")))
        handle.write("\n")

    print(f"Wrote {len(objects)} saved objects to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
