"""Tests for hypothesis_eval params + result-collection helpers.

Excludes the full ``run_hypothesis_eval_job`` flow (needs apptainer +
HTCondor token + skimgpt image). Those still get integration coverage in
the real environment.
"""
import os

import pytest
from pydantic import ValidationError

from src.jobs.hypothesis_eval.job import _iteration_from_path
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


# --- _iteration_from_path ----------------------------------------------------

def test_iteration_from_path_extracts_index():
    p = os.path.join("/job/output", "iteration_3", "diseaseX_geneA_km.json")
    assert _iteration_from_path(p) == 3


def test_iteration_from_path_returns_none_for_flat_layout():
    p = os.path.join("/job/output", "diseaseX_geneA_km.json")
    assert _iteration_from_path(p) is None


def test_iteration_from_path_returns_none_for_unparseable_index():
    p = os.path.join("/job/output", "iteration_abc", "x.json")
    assert _iteration_from_path(p) is None
