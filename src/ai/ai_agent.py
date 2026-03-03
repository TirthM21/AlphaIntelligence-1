"""AI analysis engine with configurable NVIDIA-hosted model support."""

import logging
import os
import json
import re
import time
from typing import Dict, List, Optional

from openai import OpenAI
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Optional model-specific API keys can be configured through env vars.
MODEL_KEY_ENV_MAP = {
    'qwen/qwen3.5-397b-a17b': 'NVIDIA_API_KEY_QWEN',
    'openai/gpt-oss-120b': 'NVIDIA_API_KEY_GPT_OSS',
    'z-ai/glm4.7': 'NVIDIA_API_KEY_GLM47',
    'z-ai/glm5': 'NVIDIA_API_KEY_GLM5',
    'deepseek-ai/deepseek-v3.2': 'NVIDIA_API_KEY_DEEPSEEK',
    'moonshotai/kimi-k2.5': 'NVIDIA_API_KEY_KIMI',
}

class AIAgent:
    """Uses LLM to generate professional financial commentary and newsletter content."""
    
    def __init__(self, api_key: Optional[str] = None):
        load_dotenv()

        self.base_url = os.getenv('NVIDIA_BASE_URL', 'https://integrate.api.nvidia.com/v1')

        self.supported_models = [
            'qwen/qwen3.5-397b-a17b',
            'openai/gpt-oss-120b',
            'z-ai/glm4.7',
            'z-ai/glm5',
            'deepseek-ai/deepseek-v3.2',
            'moonshotai/kimi-k2.5',
        ]
        self.model = os.getenv('NVIDIA_MODEL', self.supported_models[5])
        if self.model not in self.supported_models:
            logger.warning('Requested model %s not in supported set, falling back to %s', self.model, self.supported_models[5])
            self.model = self.supported_models[5]

        self.api_key = (
            api_key
            or os.getenv('FREE_LLM_API_KEY')
            or os.getenv('NVIDIA_API_KEY')
            or os.getenv('NVAPI_KEY')
            or os.getenv('OPENAI_API_KEY')
            or os.getenv(MODEL_KEY_ENV_MAP.get(self.model, ''))
        )

        try:
            self.request_timeout_seconds = float(os.getenv("NVIDIA_AI_TIMEOUT_SECONDS", "45"))
        except (TypeError, ValueError):
            self.request_timeout_seconds = 45.0

        try:
            self.max_retries = int(os.getenv("NVIDIA_AI_MAX_RETRIES", "1"))
        except (TypeError, ValueError):
            self.max_retries = 1

        try:
            self.max_tokens = int(os.getenv("NVIDIA_AI_MAX_TOKENS", "4096"))
        except (TypeError, ValueError):
            self.max_tokens = 4096

        try:
            self.failure_threshold = max(1, int(os.getenv("NVIDIA_AI_FAILURE_THRESHOLD", "2")))
        except (TypeError, ValueError):
            self.failure_threshold = 2

        try:
            self.cooldown_seconds = max(0, int(os.getenv("NVIDIA_AI_COOLDOWN_SECONDS", "900")))
        except (TypeError, ValueError):
            self.cooldown_seconds = 900

        self._consecutive_failures = 0
        self._cooldown_until = 0.0

        try:
            self.client = OpenAI(
                base_url=self.base_url,
                api_key=self.api_key,
                max_retries=self.max_retries,
                timeout=self.request_timeout_seconds,
            )
            logger.info(
                "AIAgent initialized using NVIDIA model=%s timeout=%ss max_retries=%s max_tokens=%s",
                self.model,
                self.request_timeout_seconds,
                self.max_retries,
                self.max_tokens,
            )
        except Exception as e:
            logger.error(f"Failed to initialize AI client: {e}")
            self.client = None

    def _is_in_cooldown(self) -> bool:
        """Return True when AI calls should be skipped due to repeated provider failures."""
        if self._cooldown_until <= 0:
            return False
        if time.time() < self._cooldown_until:
            remaining = int(self._cooldown_until - time.time())
            logger.warning("AI cooldown active for %ss; skipping provider call", max(remaining, 1))
            return True
        self._cooldown_until = 0.0
        return False

    def _record_ai_failure(self):
        """Track failures and activate temporary cooldown after threshold breaches."""
        self._consecutive_failures += 1
        if self._consecutive_failures >= self.failure_threshold and self.cooldown_seconds > 0:
            self._cooldown_until = time.time() + self.cooldown_seconds
            logger.warning(
                "AI provider failure threshold reached (%s); enabling cooldown for %ss",
                self._consecutive_failures,
                self.cooldown_seconds,
            )

    def _record_ai_success(self):
        """Reset transient AI failure state after a successful call."""
        self._consecutive_failures = 0
        self._cooldown_until = 0.0

    def _sanitize_data(self, data: any) -> any:
        """Recursively convert non-serializable objects (like pd.Timestamp) to strings."""
        if isinstance(data, dict):
            return {str(k): self._sanitize_data(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._sanitize_data(i) for i in data]
        elif hasattr(data, 'isoformat'): # Handles pd.Timestamp and datetime
            return data.isoformat()
        return data

    def generate_commentary(self, ticker: str, data: Dict) -> str:
        """Generate AI commentary for a specific stock breakout."""
        if not self.client:
            return "AI Commentary unavailable (Client Error)."

        sanitized_data = self._sanitize_data(data)

        prompt = f"""
        Act as a senior quantitative equity analyst. Analyze this stock data for {ticker}:
        
        Data: {json.dumps(sanitized_data, indent=2)}
        
        Provide a concise, professional 3-sentence summary of the investment thesis, focusing on the quality of the breakout and financial strength.
        """
        
        return self._call_ai(prompt) or "Breakout confirmed by technical indicators with supporting fundamental growth."

    def enhance_newsletter(self, newsletter_md: str) -> str:
        """Improve the language and structure of the newsletter."""
        return self.enhance_newsletter_with_validation(newsletter_md)

    def enhance_newsletter_with_validation(
        self,
        newsletter_md: str,
        evidence_payload: Optional[Dict] = None,
        prior_newsletter_md: str = ""
    ) -> str:
        """Enhance newsletter prose with validation and constrained regeneration."""
        return self._enhance_with_validation(
            newsletter_md,
            evidence_payload=evidence_payload,
            prior_newsletter_md=prior_newsletter_md,
            mode="daily",
            fallback_to_template=True,
        )

    def enhance_quarterly_newsletter_with_validation(
        self,
        newsletter_md: str,
        evidence_payload: Optional[Dict] = None,
        prior_newsletter_md: str = "",
    ) -> str:
        """Enhance quarterly newsletter with hard evidence constraints and deterministic fallback."""
        return self._enhance_with_validation(
            newsletter_md,
            evidence_payload=evidence_payload,
            prior_newsletter_md=prior_newsletter_md,
            mode="quarterly",
            fallback_to_template=True,
        )

    def _enhance_with_validation(
        self,
        newsletter_md: str,
        evidence_payload: Optional[Dict],
        prior_newsletter_md: str,
        mode: str,
        fallback_to_template: bool,
    ) -> str:
        """Shared constrained enhancement path for newsletter variants."""
        if not self.client:
            return newsletter_md

        evidence_payload = evidence_payload or {}
        base_prompt = self._build_newsletter_prompt(
            newsletter_md,
            evidence_payload,
            prior_newsletter_md=prior_newsletter_md,
            stricter=False,
            mode=mode,
        )
        enhanced = self._call_ai(base_prompt, low_temp=True)
        if not enhanced:
            return newsletter_md

        issues = self._validate_newsletter(enhanced, evidence_payload, prior_newsletter_md, mode=mode)
        if not issues:
            return enhanced

        logger.warning("Newsletter validation failed (%s). Regenerating with stricter prompt.", "; ".join(issues))
        retry_prompt = self._build_newsletter_prompt(
            newsletter_md,
            evidence_payload,
            prior_newsletter_md=prior_newsletter_md,
            stricter=True,
            validation_issues=issues,
            mode=mode,
        )
        retry = self._call_ai(retry_prompt, temperature=0.05)
        if not retry:
            return newsletter_md

        retry_issues = self._validate_newsletter(retry, evidence_payload, prior_newsletter_md, mode=mode)
        if not retry_issues:
            return retry
        return newsletter_md

    def _build_newsletter_prompt(
        self,
        newsletter_md: str,
        evidence_payload: Dict,
        prior_newsletter_md: str = "",
        stricter: bool = False,
        validation_issues: Optional[List[str]] = None,
        mode: str = "daily",
    ) -> str:
        """Construct constrained newsletter editing prompt."""
        max_reused_sentences = 0 if stricter else 1
        extra_guardrails = ""
        if validation_issues:
            extra_guardrails = f"\nFailed checks from prior draft: {', '.join(validation_issues)}. Resolve every failed check."

        mode_guardrails = ""
        payload_mode = str(evidence_payload.get("report_type", mode)).strip().lower() or mode
        time_horizon_days = evidence_payload.get("time_horizon_days")
        structural_rules = """
        CRITICAL STRUCTURAL RULES:
        1. MAINTAIN the '## 🏛️ AlphaIntelligence Capital BRIEF' header.
        2. KEEP all vertical lists (e.g. Sector Performance, Market Sentiment, Today's Events) EXACTLY as they are. DO NOT convert them into tables.
        3. Do NOT add new sections that weren't in the original text.
        4. Maintain all technical data points, URLs, and markdown formatting.
        5. Improve narrative transitions using concise analyst language.
        """
        if mode == "quarterly":
            mode_guardrails = """
        TIME-HORIZON REQUIREMENT:
        - Quarterly mode is strictly multi-quarter (90-365+ day) and regime/allocation focused.
        - Frame commentary around allocation durability, drawdown control, and multi-quarter catalysts.
        - Do NOT include short-horizon tape commentary, intraday framing, or "today/tomorrow/this week" catalyst language.

        QUARTERLY HARD CONSTRAINTS (non-negotiable):
        - Do not express macro outcomes with certainty (forbidden: will, guaranteed, certain, inevitably).
        - Do not introduce percentages that are not present in the authoritative payload.
        - Do not introduce named entities (tickers, companies, institutions, events) absent from the authoritative payload.
        - If payload evidence is missing for a claim, rewrite as uncertainty-aware and evidence-limited language.
        - Preserve markdown tables and section subtitles verbatim unless a minimal grammar edit is required.
        - Do not rewrite deterministic numeric/reporting blocks (tables, scorecards, KPI lines, and metric bullets); only fix clear grammar.
            """
            structural_rules = """
        CRITICAL STRUCTURAL RULES:
        1. Preserve existing section order and headings; do NOT add new sections.
        2. Maintain all technical data points, URLs, and markdown formatting.
        3. Preserve markdown tables and section subtitles verbatim unless a minimal grammar edit is required.
        4. Do not rewrite deterministic numeric/reporting blocks (tables, scorecards, KPI lines, and metric bullets).
        5. Improve narrative transitions using concise analyst language around the fixed reporting blocks.
        """
        else:
            mode_guardrails = """
        TIME-HORIZON REQUIREMENT:
        - Daily mode is strictly short-horizon (1-3 day) market tape.
        - Prioritize immediate catalysts, next-session setup, and event risk inside the next 1-3 trading days.
        - Avoid regime/allocation/multi-quarter language unless explicitly anchored to immediate tape risk.
            """

        prompt = f"""
        Act as a professional financial editor for AlphaIntelligence Capital. 
        Enhance the following newsletter to make it sound institutional, elite, and authoritative.
        Keep voice concise, analyst-style, and evidence-led (short declarative sentences, no hype, no rhetorical questions, no clichés).
        
        {structural_rules}

        REQUIRED EVIDENCE SLOTS (must be explicit, each on its own bullet in the market overview narrative):
        - Index Move: cite index and exact move.
        - Sector Leader/Laggard: identify both with numeric spread.
        - Mover Stats: include at least one advancing and one declining ticker move.
        - Event References: reference upcoming/active macro or earnings events.

        SENTENCE REUSE CONSTRAINT:
        - Reuse at most {max_reused_sentences} full sentence(s) from prior day's newsletter text.
        {mode_guardrails}

        MODE MARKERS (must be respected exactly):
        - report_type: {payload_mode}
        - time_horizon_days: {time_horizon_days}
        - If these markers conflict with draft language, rewrite the draft to match the markers.

        DATA PAYLOAD (authoritative facts only):
        {json.dumps(self._sanitize_data(evidence_payload), indent=2)}

        PRIOR DAY NEWSLETTER (for anti-repetition only):
        {prior_newsletter_md[:6000] if prior_newsletter_md else 'N/A'}
        {extra_guardrails}
        
        Newsletter:
        {newsletter_md}
        
        Return ONLY the updated markdown newsletter.
        """
        return prompt

    def _validate_newsletter(self, text: str, evidence_payload: Dict, prior_newsletter_md: str, mode: str = "daily") -> List[str]:
        """Validate generated newsletter for evidence anchors, repetition, and unsupported claims."""
        issues = []

        payload_report_type = str(evidence_payload.get("report_type", mode)).strip().lower() or mode
        expected_mode = "quarterly" if payload_report_type == "quarterly" else "daily"
        if mode != expected_mode:
            issues.append(f"mode mismatch: validator mode={mode} payload report_type={payload_report_type}")

        lower_text = text.lower()
        if mode != "quarterly":
            required_slot_terms = ["index move", "sector leader", "laggard", "mover", "event"]
            if not all(term in lower_text for term in required_slot_terms):
                issues.append("missing explicit evidence slots")

            numeric_anchors = re.findall(r"[-+]?\d+(?:\.\d+)?%?", text)
            if len(numeric_anchors) < 8:
                issues.append("missing numeric anchors")

        if prior_newsletter_md:
            repeated = self._find_reused_phrases(text, prior_newsletter_md)
            if repeated:
                issues.append("repeated phrases from prior run")

        unsupported = self._find_unsupported_claims(text, evidence_payload)
        if unsupported:
            issues.append("unsupported claims not present in fetched data payload")

        if mode == "quarterly":
            required_sections = [
                "quarterly investment thesis",
                "macro news regime review",
                "portfolio governance & architecture",
                "event horizon — key quarterly catalysts",
            ]
            forbidden_sections = [
                "new since yesterday",
                "today's events",
                "sector performance",
            ]
            forbidden_terms = [
                "market open",
                "opening bell",
                "into tomorrow",
                "next session",
                "intraday",
                "pre-market",
                "post-market",
            ]
            required_mode_terms = ["regime", "allocation", "multi-quarter"]

            forward_certainty = re.search(r"\b(will|guaranteed?|certain(?:ly)?|inevitably|undoubtedly)\b", text, flags=re.IGNORECASE)
            if forward_certainty:
                issues.append("forward macro certainty language is not allowed")

            unsupported_percentages = self._find_unsupported_percentages(text, evidence_payload)
            if unsupported_percentages:
                issues.append("unsupported percentages not present in evidence payload")

            if re.search(r"\b(today|tomorrow|this week|intraday)\b", text, flags=re.IGNORECASE):
                issues.append("quarterly draft contains daily short-horizon language")
        else:
            required_sections = [
                "new since yesterday",
                "today's events",
                "sector performance",
            ]
            forbidden_sections = [
                "quarterly investment thesis",
                "macro news regime review",
                "portfolio governance & architecture",
                "event horizon — key quarterly catalysts",
            ]
            forbidden_terms = [
                "multi-quarter",
                "strategic asset allocation",
                "portfolio governance",
                "regime review",
                "12-month",
            ]
            required_mode_terms = ["today", "new since yesterday", "catalyst"]

        for section in required_sections:
            if section not in lower_text:
                issues.append(f"missing required {mode} section: {section}")
        for section in forbidden_sections:
            if section in lower_text:
                issues.append(f"cross-mode section leakage detected: {section}")
        for term in forbidden_terms:
            if term in lower_text:
                issues.append(f"cross-mode language leakage detected: {term}")

        for term in required_mode_terms:
            if term not in lower_text:
                issues.append(f"missing {mode} narrative marker: {term}")

        time_horizon_days = evidence_payload.get("time_horizon_days")
        if isinstance(time_horizon_days, (int, float)):
            if mode == "daily" and time_horizon_days > 7:
                issues.append("daily payload time_horizon_days exceeds short-horizon limit")
            if mode == "quarterly" and time_horizon_days < 60:
                issues.append("quarterly payload time_horizon_days below multi-quarter limit")

        return issues

    def _find_unsupported_percentages(self, text: str, evidence_payload: Dict) -> List[str]:
        """Reject percentages not anchored to explicit evidence allow-list."""
        allowed_percentages = set(str(v).strip() for v in (evidence_payload.get("allowed_percentages") or []))
        if not allowed_percentages:
            return re.findall(r"\b\d+(?:\.\d+)?%\b", text)[:5]
        unsupported = []
        for pct in re.findall(r"\b\d+(?:\.\d+)?%\b", text):
            if pct not in allowed_percentages:
                unsupported.append(pct)
        return unsupported[:5]

    def _find_reused_phrases(self, current_text: str, prior_text: str) -> List[str]:
        """Return long repeated phrases reused from prior run."""
        current_sentences = {s.strip().lower() for s in re.split(r"[\n\.!?]+", current_text) if len(s.split()) >= 9}
        prior_sentences = {s.strip().lower() for s in re.split(r"[\n\.!?]+", prior_text) if len(s.split()) >= 9}
        return sorted(current_sentences.intersection(prior_sentences))[:5]

    def _find_unsupported_claims(self, text: str, evidence_payload: Dict) -> List[str]:
        """Detect referenced symbols/events that are not in payload allow-list."""
        payload_blob = json.dumps(self._sanitize_data(evidence_payload)).lower()
        unsupported_tokens = []
        allowed_entities = {str(v).strip().lower() for v in (evidence_payload.get("allowed_named_entities") or []) if str(v).strip()}

        for token in re.findall(r"\b[A-Z]{2,5}\b", text):
            if token in {"SMA", "RSI", "GDP", "CPI", "EPS"}:
                continue
            if allowed_entities:
                if token.lower() not in allowed_entities:
                    unsupported_tokens.append(token)
            elif token.lower() not in payload_blob:
                unsupported_tokens.append(token)


        return unsupported_tokens[:5]

    def generate_qotd(self) -> Dict[str, str]:
        """Generate a 'Question of the Day' with institutional insight."""
        if not self.client:
            return {
                "question": "What is the historical average return of February?",
                "answer": "Historically, February is one of the weakest months for the S&P 500, often showing a negative average return.",
                "insight": "Investors should be selective and look for relative strength during seasonal weakness."
            }

        prompt = """
        Generate a 'Question of the Day' about stock market history, seasonality, or quantitative indicators.
        
        Return JSON with:
        "question": A compelling question.
        "answer": A factual answer.
        "insight": A professional takeaway.
        
        Ensure it is factual and high-quality.
        """
        
        try:
            resp = self._call_ai(prompt, low_temp=False)
            if resp:
                clean_resp = resp.strip().replace('```json', '').replace('```', '')
                return json.loads(clean_resp)
        except Exception as e:
            logger.error(f"Failed to generate QotD: {e}")
            
        return {
            "question": "What happens after a 1% market drop?",
            "answer": "It typically takes about four weeks on average for the market to fully recover from a 1% single-day drop.",
            "insight": "Market pullbacks are often temporary mean-reversion events within a larger trend."
        }


    def choose_chart_keys(self, context: Dict, available_keys: List[str]) -> List[str]:
        """Use AI to pick which charts to include from a fixed available key set."""
        if not available_keys:
            return []

        # Deterministic fallback if AI unavailable.
        if not self.client:
            return available_keys[:4]

        prompt = (
            "You are selecting charts for a professional daily market brief. "
            "Pick the best 3 to 5 chart keys from the provided available_keys only. "
            "Return JSON only: {\"chart_keys\": [..]}.\n\n"
            f"available_keys={available_keys}\n"
            f"context={json.dumps(self._sanitize_data(context))[:4000]}"
        )
        raw = self._call_ai(prompt, low_temp=True)
        if not raw:
            return available_keys[:4]
        try:
            clean = raw.strip().replace('```json', '').replace('```', '')
            payload = json.loads(clean)
            picks = payload.get('chart_keys') if isinstance(payload, dict) else []
            picks = [str(k) for k in picks if str(k) in available_keys]
            if len(picks) >= 3:
                return picks[:5]
        except Exception:
            pass
        return available_keys[:4]

    def generate_multi_model_sections(self, section_payloads: Dict[str, Dict]) -> Dict[str, str]:
        """Generate section text using all configured models in round-robin assignment."""
        results: Dict[str, str] = {}
        if not section_payloads:
            return results

        for idx, (section_name, payload) in enumerate(section_payloads.items()):
            model = self.supported_models[idx % len(self.supported_models)]
            model_key = os.getenv(MODEL_KEY_ENV_MAP.get(model, '')) or self.api_key
            context = payload.get('context') or {}
            instruction = payload.get('instruction') or 'Write 2 concise institutional bullets.'
            fallback = payload.get('fallback') or '- Keep positioning balanced and data-driven.'

            prompt = (
                f"You are an institutional market strategist. Section: {section_name}. "
                f"{instruction} Use only the provided context. Return only bullet points.\n\n"
                f"context={json.dumps(self._sanitize_data(context))[:5000]}"
            )
            generated = self._call_ai_with_model(
                prompt,
                model=model,
                api_key=model_key,
                temperature=0.35,
            )
            results[section_name] = generated or fallback

        return results

    def _call_ai_with_model(
        self,
        prompt: str,
        *,
        model: str,
        api_key: Optional[str],
        low_temp: bool = False,
        temperature: Optional[float] = None,
    ) -> Optional[str]:
        """Call NVIDIA API with an explicitly selected model/key pair."""
        if not api_key:
            return None
        if self._is_in_cooldown():
            return None
        try:
            client = OpenAI(
                base_url=self.base_url,
                api_key=api_key,
                max_retries=self.max_retries,
                timeout=self.request_timeout_seconds,
            )
            completion = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=temperature if temperature is not None else (0.1 if low_temp else 1.0),
                top_p=1.00,
                max_tokens=self.max_tokens,
                extra_body={"chat_template_kwargs": {"thinking": True}},
                stream=False,
            )
            if not completion.choices:
                self._record_ai_failure()
                return None
            content = completion.choices[0].message.content
            if isinstance(content, str):
                self._record_ai_success()
                return content.strip()
            if isinstance(content, list):
                text_parts = []
                for part in content:
                    if isinstance(part, dict):
                        text_val = part.get("text")
                        if isinstance(text_val, str):
                            text_parts.append(text_val)
                joined = "".join(text_parts).strip()
                if joined:
                    self._record_ai_success()
                else:
                    self._record_ai_failure()
                return joined or None
            self._record_ai_failure()
            return None
        except Exception as e:
            self._record_ai_failure()
            logger.error("AI API call failed for model %s: %s", model, e)
            return None

    def _call_ai(self, prompt: str, low_temp: bool = False, temperature: Optional[float] = None) -> Optional[str]:
        """Call NVIDIA Integrated API using currently selected model and key."""
        return self._call_ai_with_model(
            prompt,
            model=self.model,
            api_key=self.api_key,
            low_temp=low_temp,
            temperature=temperature,
        )
