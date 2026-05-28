"""Experiment 3: select the best index, publish it, and audit the result."""

from pipelines import argmax

from .artifacts import (
    AuditMarker,
    ManualWinnerReport,
    PublishedBundle,
    WinnerReport,
)
from .experiment_merge import indices as merged_indices
from .experiment_variants import indices as variant_indices

candidates = tuple(variant_indices + merged_indices)
best = argmax(candidates, key=lambda index: index.unique_words)

automatic = WinnerReport(label="most-unique", best=best)
manual = ManualWinnerReport(label="most-unique", best=best)
bundle = PublishedBundle(
    label="most-unique",
    automatic=automatic,
    manual=manual,
)
audit = AuditMarker(label="most-unique", bundle=bundle)

reports = [automatic, manual]
targets = [bundle, audit]
