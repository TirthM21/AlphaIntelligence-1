"""Tests for AI agent prompt construction."""

from src.ai.ai_agent import AIAgent


def test_build_newsletter_prompt_daily_has_brief_and_vertical_list_rules():
    """Daily mode should enforce BRIEF header and vertical list preservation."""
    agent = AIAgent(api_key="test-key")

    prompt = agent._build_newsletter_prompt(
        newsletter_md="## 🏛️ AlphaIntelligence Capital BRIEF\nBody",
        evidence_payload={"foo": "bar"},
        mode="daily",
    )

    assert "MAINTAIN the '## 🏛️ AlphaIntelligence Capital BRIEF' header" in prompt
    assert "KEEP all vertical lists" in prompt
    assert "DO NOT convert them into tables" in prompt


def test_build_newsletter_prompt_quarterly_uses_quarterly_structure_rules():
    """Quarterly mode should preserve table/reporting blocks and avoid daily-only constraints."""
    agent = AIAgent(api_key="test-key")

    prompt = agent._build_newsletter_prompt(
        newsletter_md="## Quarterly Overview\n| Metric | Value |",
        evidence_payload={"allowed_percentages": ["2.5%"]},
        mode="quarterly",
    )

    assert "MAINTAIN the '## 🏛️ AlphaIntelligence Capital BRIEF' header" not in prompt
    assert "KEEP all vertical lists" not in prompt
    assert "Preserve markdown tables and section subtitles verbatim unless a minimal grammar edit is required" in prompt
    assert "Do not rewrite deterministic numeric/reporting blocks" in prompt
    assert "Do not introduce percentages that are not present in the authoritative payload" in prompt



def test_call_ai_with_model_skips_when_cooldown_active():
    """AI calls should short-circuit during cooldown to avoid long hangs."""
    agent = AIAgent(api_key="test-key")
    agent._cooldown_until = 10**12

    result = agent._call_ai_with_model(
        "hello",
        model=agent.model,
        api_key="test-key",
    )

    assert result is None


def test_record_ai_failure_enables_cooldown():
    """Repeated provider failures should trigger cooldown state."""
    agent = AIAgent(api_key="test-key")
    agent.failure_threshold = 2
    agent.cooldown_seconds = 10

    agent._record_ai_failure()
    assert agent._cooldown_until == 0.0

    agent._record_ai_failure()
    assert agent._cooldown_until > 0.0
