"""Experiment 1: normalize each input in several ways and preview indexes."""

from itertools import product

from pipelines import Project

from .artifacts import LocalDocument, NormalizedText, Preview, WordIndex

documents = {
    name: LocalDocument(name=name)
    for name in Project.config.document_names
}

normalized = [
    NormalizedText(
        source=documents[name],
        text_case=text_case,
        remove=tuple(Project.config.remove_words),
    )
    for name, text_case in product(
        Project.config.document_names,
        Project.config.text_case_modes,
    )
]

indices = [
    WordIndex(text=text, min_length=min_length)
    for text, min_length in product(normalized, Project.config.min_lengths)
]

previews = [
    Preview(
        index=index,
        label=(
            f"{index.text.source.name}-{index.text.text_case}-"
            f"min{index.min_length}"
        ),
    )
    for index in indices
]

targets = previews
