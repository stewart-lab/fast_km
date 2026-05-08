"""Tests for hypothesis_eval params + result-collection helpers.

Excludes the full ``run_hypothesis_eval_job`` flow (needs apptainer +
HTCondor token + skimgpt image). Those still get integration coverage in
the real environment.
"""
import json
import os

import pytest
from pydantic import ValidationError

from src.jobs.hypothesis_eval.job import (
    _collect_results,
    _compute_windows,
    _iteration_from_path,
)
from src.jobs.hypothesis_eval.params import HypothesisEvalJobParams


# --- iterations param --------------------------------------------------------

def _km_dch_payload(**overrides):
    """Minimal valid DCH payload, with overrides merged in."""
    payload = {
        "data": [
            {"a_term": "diseaseX", "b_term": "geneA", "ab_pmid_intersection": []},
            {"a_term": "diseaseX", "b_term": "geneB", "ab_pmid_intersection": []},
        ],
        "KM_hypothesis": "{a_term} is caused by {b_term}",
        "is_dch": True,
    }
    payload.update(overrides)
    return payload


def test_iterations_defaults_to_one():
    p = HypothesisEvalJobParams(**_km_dch_payload())
    assert p.iterations == 1


def test_iterations_accepts_in_range():
    for n in (1, 3, 10):
        p = HypothesisEvalJobParams(**_km_dch_payload(iterations=n))
        assert p.iterations == n


def test_iterations_rejects_zero_or_negative():
    for bad in (0, -1, -10):
        with pytest.raises(ValidationError):
            HypothesisEvalJobParams(**_km_dch_payload(iterations=bad))


def test_iterations_rejects_above_cap():
    with pytest.raises(ValidationError):
        HypothesisEvalJobParams(**_km_dch_payload(iterations=11))


# --- censor_year_increment param --------------------------------------------

def test_increment_defaults_to_none():
    p = HypothesisEvalJobParams(**_km_dch_payload())
    assert p.censor_year_increment is None


def test_increment_accepts_positive():
    for n in (1, 2, 5):
        p = HypothesisEvalJobParams(**_km_dch_payload(censor_year_increment=n))
        assert p.censor_year_increment == n


def test_increment_rejects_zero_or_negative():
    for bad in (0, -1):
        with pytest.raises(ValidationError):
            HypothesisEvalJobParams(**_km_dch_payload(censor_year_increment=bad))


# --- _compute_windows --------------------------------------------------------

def test_windows_none_increment_is_single_window():
    assert _compute_windows(2020, 2025, None) == [(2020, 2025)]


def test_windows_inc_one_is_per_year():
    assert _compute_windows(2020, 2022, 1) == [(2020, 2020), (2021, 2021), (2022, 2022)]


def test_windows_inc_two_tiles_evenly():
    assert _compute_windows(2020, 2025, 2) == [(2020, 2021), (2022, 2023), (2024, 2025)]


def test_windows_inc_two_clamps_uneven_tail():
    assert _compute_windows(2020, 2024, 2) == [(2020, 2021), (2022, 2023), (2024, 2024)]


def test_windows_single_year_range():
    assert _compute_windows(2020, 2020, 1) == [(2020, 2020)]
    assert _compute_windows(2020, 2020, 5) == [(2020, 2020)]


# --- _iteration_from_path ----------------------------------------------------

def test_iteration_from_path_extracts_index():
    p = os.path.join("/job/output", "iteration_3", "diseaseX_geneA_km.json")
    assert _iteration_from_path(p) == 3


def test_iteration_from_path_returns_none_for_flat_layout():
    p = os.path.join("/job/output", "diseaseX_geneA_km.json")
    assert _iteration_from_path(p) is None


def test_iteration_from_path_raises_for_unparseable_index():
    p = os.path.join("/job/output", "iteration_abc", "x.json")
    with pytest.raises(ValueError):
        _iteration_from_path(p)


def test_iteration_from_path_handles_nested_layout():
    p = os.path.join("/job/output", "results", "iteration_2", "diseaseX_geneA_km.json")
    assert _iteration_from_path(p) == 2


# --- _collect_results --------------------------------------------------------

def _write_result_json(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(payload, f)


def _km_result_payload(a="diseaseX", b="geneA"):
    return {"A_B_Relationship": {"a_term": a, "b_term": b, "Result": "ok"}}


def test_collect_results_tags_single_pass_with_iteration_one(tmp_path):
    _write_result_json(str(tmp_path / "diseaseX_geneA_km.json"), _km_result_payload())
    results = _collect_results(str(tmp_path), expected_iterations=1)
    assert len(results) == 1
    assert results[0]["iteration"] == 1


def test_collect_results_tags_multi_iteration(tmp_path):
    for i in (1, 2, 3):
        _write_result_json(
            str(tmp_path / "output" / f"iteration_{i}" / "diseaseX_geneA_km.json"),
            _km_result_payload(),
        )
    results = _collect_results(str(tmp_path), expected_iterations=3)
    assert sorted(r["iteration"] for r in results) == [1, 2, 3]


def test_collect_results_warns_on_missing_iteration(tmp_path, capsys):
    for i in (1, 3):
        _write_result_json(
            str(tmp_path / "output" / f"iteration_{i}" / "diseaseX_geneA_km.json"),
            _km_result_payload(),
        )
    _collect_results(str(tmp_path), expected_iterations=3)
    out = capsys.readouterr().out
    assert "WARNING" in out
    assert "[2]" in out  # the missing iteration


def test_collect_results_raises_when_no_files(tmp_path):
    with pytest.raises(FileNotFoundError):
        _collect_results(str(tmp_path), expected_iterations=1)


def test_collect_results_skips_config_and_secrets(tmp_path):
    _write_result_json(str(tmp_path / "config.json"), {"x": 1})
    _write_result_json(str(tmp_path / "secrets.json"), {"x": 1})
    _write_result_json(str(tmp_path / "diseaseX_geneA_km.json"), _km_result_payload())
    results = _collect_results(str(tmp_path), expected_iterations=1)
    assert len(results) == 1


def test_collect_results_skips_debug_subtree(tmp_path):
    _write_result_json(str(tmp_path / "output" / "diseaseX_geneA_km.json"), _km_result_payload())
    _write_result_json(str(tmp_path / "output" / "debug" / "trace.json"), {"trace": "noise"})
    results = _collect_results(str(tmp_path), expected_iterations=1)
    assert len(results) == 1
