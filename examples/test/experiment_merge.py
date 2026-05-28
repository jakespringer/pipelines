"""Experiment 2: merge normalized inputs and index the combined document."""

from pipelines import Project

from .artifacts import MergedText, Preview, WordIndex
from .experiment_variants import normalized

lower_inputs = tuple(text for text in normalized if text.text_case == "lower")
merged = MergedText(name="all-lower", texts=lower_inputs)

indices = [
    WordIndex(text=merged, min_length=min_length)
    for min_length in Project.config.min_lengths
]

previews = [
    Preview(index=index, label=f"all-lower-min{index.min_length}")
    for index in indices
]

targets = previews
