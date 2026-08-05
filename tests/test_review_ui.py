from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_initial_review_does_not_collect_linguistic_dimensions() -> None:
    html = (ROOT / "review" / "index.html").read_text(encoding="utf-8")
    app = (ROOT / "review" / "app.js").read_text(encoding="utf-8")

    removed_control_ids = {
        "naturalnessInput",
        "pragmatismInput",
        "roomForInterpretationInput",
        "requiresGraphContextKnowledgeInput",
        "clearInterpretiveBtn",
    }
    for control_id in removed_control_ids:
        assert control_id not in html
        assert control_id not in app
