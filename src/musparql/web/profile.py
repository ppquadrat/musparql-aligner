"""Phase 4 reviewer onboarding and profile administration."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import re
import unicodedata
import uuid

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from musparql.database.models import (
    ExpertiseDomain,
    Reviewer,
    ReviewerDomainExpertise,
    ReviewerExperience,
    ReviewerLanguage,
)
from musparql.reviewer_provenance import validate_reviewer_domain_expertise_assertions
from musparql.source_catalog import load_expertise_domain_suggestions

from .auth import timestamp, utc_now


TECHNICAL_LEVELS = ("none", "occasional", "regular", "expert")
SUBJECT_LEVELS = ("none", "basic", "working", "advanced", "expert")
LANGUAGE_LEVELS = ("basic", "advanced", "fluent", "native")
LANGUAGE_TAG_RE = re.compile(r"^[a-z]{2,3}(?:-[A-Z]{2})?$")
MAX_DOMAINS = 20
MAX_LANGUAGES = 20


@dataclass(frozen=True)
class DomainValue:
    domain_id: str
    assertion_id: str
    entered_label: str
    expertise_level: str


@dataclass(frozen=True)
class ProfileValue:
    name: str
    affiliation: str
    email: str
    kg_ontology_experience: str
    sparql_experience: str
    nlp_llm_experience: str
    languages: tuple[tuple[str, str], ...]
    domains: tuple[DomainValue, ...]
    notice_current: bool


def normalize_domain_label(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).strip().split()).casefold()


class ProfileService:
    def __init__(
        self,
        *,
        sessions: sessionmaker[Session],
        notice_version: str,
        suggestions_path: Path,
        language_options_path: Path,
    ) -> None:
        self.sessions = sessions
        self.notice_version = notice_version
        payload = load_expertise_domain_suggestions(suggestions_path)
        self.suggestion_snapshot_id = str(payload["snapshot_id"])
        self.suggestions = tuple(payload["suggestions"])
        self._suggestions_by_label = {
            normalize_domain_label(label): suggestion
            for suggestion in self.suggestions
            for label in (
                suggestion["preferred_label"],
                *suggestion["alternative_labels"],
            )
        }
        self._sources = {item["source_id"]: item for item in payload["sources"]}
        self.euroscivoc_suggestion_count = sum(
            suggestion["source_id"] == "euroscivoc-reference"
            for suggestion in self.suggestions
        )
        language_payload = json.loads(language_options_path.read_text(encoding="utf-8"))
        if language_payload.get("schema") != "musparql.language-options.v1":
            raise ValueError("Language options use an unsupported contract")
        options = language_payload.get("options")
        if not isinstance(options, list) or not options:
            raise ValueError("Language options are empty")
        validated_options: list[dict[str, str]] = []
        seen_tags: set[str] = set()
        for option in options:
            if not isinstance(option, dict):
                raise ValueError("A language option is not an object")
            tag = option.get("tag")
            name = option.get("name")
            if (
                not isinstance(tag, str)
                or not LANGUAGE_TAG_RE.fullmatch(tag)
                or not isinstance(name, str)
                or not name.strip()
                or tag in seen_tags
            ):
                raise ValueError("A language option is invalid or duplicated")
            seen_tags.add(tag)
            validated_options.append({"tag": tag, "name": name.strip()})
        self.language_options = tuple(validated_options)
        self.language_tags = frozenset(seen_tags)
        self.language_snapshot_id = str(language_payload["snapshot_id"])

    def is_complete(self, reviewer_id: str) -> bool:
        with self.sessions() as session:
            reviewer = session.get(Reviewer, reviewer_id)
            if reviewer is None:
                return False
            return self._is_complete(session, reviewer)

    def completion_by_reviewer(self) -> dict[str, bool]:
        with self.sessions() as session:
            return {
                reviewer.id: self._is_complete(session, reviewer)
                for reviewer in session.scalars(select(Reviewer).order_by(Reviewer.id))
            }

    def load(self, reviewer_id: str) -> ProfileValue:
        with self.sessions() as session:
            reviewer = session.get(Reviewer, reviewer_id)
            if reviewer is None:
                raise ValueError("Unknown reviewer")
            experience = session.get(ReviewerExperience, reviewer_id)
            languages = tuple(
                (item.language_tag, item.level)
                for item in session.scalars(
                    select(ReviewerLanguage)
                    .where(ReviewerLanguage.reviewer_id == reviewer_id)
                    .order_by(ReviewerLanguage.language_tag)
                )
            )
            domains = tuple(
                DomainValue(
                    domain_id=assertion.domain_id,
                    assertion_id=assertion.id,
                    entered_label=domain.entered_label,
                    expertise_level=assertion.expertise_level,
                )
                for assertion, domain in self._domain_heads(session, reviewer_id)
            )
            return ProfileValue(
                name=reviewer.name,
                affiliation=reviewer.affiliation,
                email=reviewer.email_display,
                kg_ontology_experience=(experience.kg_ontology_experience if experience else ""),
                sparql_experience=(experience.sparql_experience if experience else ""),
                nlp_llm_experience=(experience.nlp_llm_experience if experience else ""),
                languages=languages,
                domains=domains,
                notice_current=(
                    reviewer.privacy_notice_version == self.notice_version
                    and reviewer.privacy_notice_acknowledged_at is not None
                ),
            )

    def update(
        self,
        reviewer_id: str,
        form: Mapping[str, str],
        *,
        existing_domain_ids: Sequence[str],
        existing_assertion_ids: Sequence[str],
        existing_domain_levels: Sequence[str],
        new_domain_labels: Sequence[str],
        new_domain_levels: Sequence[str],
        language_tags: Sequence[str],
        language_levels: Sequence[str],
        now: datetime | None = None,
    ) -> None:
        now_text = timestamp(now or utc_now())
        name = unicodedata.normalize("NFC", form.get("name", "")).strip()
        affiliation = unicodedata.normalize("NFC", form.get("affiliation", "")).strip()
        if not name:
            raise ValueError("Enter your name.")
        if len(name) > 200:
            raise ValueError("Name must be 200 characters or fewer.")
        if len(affiliation) > 300:
            raise ValueError("Affiliation must be 300 characters or fewer.")

        technical = {
            field: form.get(field, "")
            for field in (
                "kg_ontology_experience",
                "sparql_experience",
                "nlp_llm_experience",
            )
        }
        if any(value not in TECHNICAL_LEVELS for value in technical.values()):
            raise ValueError("Every technical-experience field is required")
        languages = self._parse_languages(language_tags, language_levels)
        new_domains = self._parse_new_domains(new_domain_labels, new_domain_levels)
        if not (
            len(existing_domain_ids)
            == len(existing_assertion_ids)
            == len(existing_domain_levels)
        ):
            raise ValueError("Domain fields are inconsistent")
        if (
            len(set(existing_domain_ids)) != len(existing_domain_ids)
            or len(set(existing_assertion_ids)) != len(existing_assertion_ids)
        ):
            raise ValueError("Existing domains must be unique")
        if any(level not in SUBJECT_LEVELS for level in existing_domain_levels):
            raise ValueError("Every domain requires an expertise level")
        if len(existing_domain_ids) + len(new_domains) > MAX_DOMAINS:
            raise ValueError("Too many expertise domains")

        with self.sessions.begin() as session:
            reviewer = session.get(Reviewer, reviewer_id)
            if reviewer is None or reviewer.status != "active":
                raise ValueError("The reviewer account is not active")
            notice_is_current = (
                reviewer.privacy_notice_version == self.notice_version
                and reviewer.privacy_notice_acknowledged_at is not None
            )
            if not notice_is_current and form.get("notice_acknowledged") != "yes":
                raise ValueError("The current privacy notice must be acknowledged")

            heads = {item.domain_id: (item, domain) for item, domain in self._domain_heads(session, reviewer_id)}
            if set(existing_domain_ids) != set(heads):
                raise ValueError("The submitted domain set is stale or invalid")
            if any(
                heads[domain_id][0].id != assertion_id
                for domain_id, assertion_id in zip(
                    existing_domain_ids, existing_assertion_ids, strict=True
                )
            ):
                raise ValueError("The profile changed in another request")
            normalized_labels = {domain.normalized_label for _, domain in heads.values()}
            normalized_labels.update(item[1] for item in new_domains)
            if len(normalized_labels) != len(heads) + len(new_domains):
                raise ValueError("Expertise domains must be unique")
            if not heads and not new_domains:
                raise ValueError("At least one expertise domain is required")

            reviewer.name = name
            reviewer.affiliation = affiliation
            reviewer.updated_at = now_text
            if not notice_is_current:
                reviewer.privacy_notice_version = self.notice_version
                reviewer.privacy_notice_acknowledged_at = now_text

            experience = session.get(ReviewerExperience, reviewer_id)
            if experience is None:
                experience = ReviewerExperience(reviewer_id=reviewer_id, assessed_at=now_text, **technical)
                session.add(experience)
            elif any(getattr(experience, key) != value for key, value in technical.items()):
                for key, value in technical.items():
                    setattr(experience, key, value)
                experience.assessed_at = now_text

            existing_languages = {
                item.language_tag: item
                for item in session.scalars(
                    select(ReviewerLanguage).where(ReviewerLanguage.reviewer_id == reviewer_id)
                )
            }
            for tag in set(existing_languages) - set(languages):
                session.delete(existing_languages[tag])
            for tag, level in languages.items():
                item = existing_languages.get(tag)
                if item is None:
                    session.add(
                        ReviewerLanguage(
                            reviewer_id=reviewer_id,
                            language_tag=tag,
                            level=level,
                            first_asserted_at=now_text,
                            updated_at=now_text,
                        )
                    )
                elif item.level != level:
                    item.level = level
                    item.updated_at = now_text

            for domain_id, level in zip(existing_domain_ids, existing_domain_levels, strict=True):
                head, domain = heads[domain_id]
                if head.expertise_level != level:
                    self._append_domain_assertion(
                        session, reviewer_id, domain, level, now_text, supersedes=head
                    )
            for entered_label, normalized_label, level in new_domains:
                domain = self._new_domain(entered_label, normalized_label)
                session.add(domain)
                session.flush()
                self._append_domain_assertion(
                    session, reviewer_id, domain, level, now_text, supersedes=None
                )

    def _is_complete(self, session: Session, reviewer: Reviewer) -> bool:
        if (
            reviewer.privacy_notice_version != self.notice_version
            or reviewer.privacy_notice_acknowledged_at is None
        ):
            return False
        if session.get(ReviewerExperience, reviewer.id) is None:
            return False
        language_count = session.scalar(
            select(func.count()).select_from(ReviewerLanguage).where(
                ReviewerLanguage.reviewer_id == reviewer.id
            )
        )
        return bool(language_count and self._domain_heads(session, reviewer.id))

    @staticmethod
    def _domain_heads(
        session: Session, reviewer_id: str
    ) -> list[tuple[ReviewerDomainExpertise, ExpertiseDomain]]:
        superseded = select(ReviewerDomainExpertise.supersedes_id).where(
            ReviewerDomainExpertise.supersedes_id.is_not(None)
        )
        return list(
            session.execute(
                select(ReviewerDomainExpertise, ExpertiseDomain)
                .join(ExpertiseDomain, ExpertiseDomain.id == ReviewerDomainExpertise.domain_id)
                .where(
                    ReviewerDomainExpertise.reviewer_id == reviewer_id,
                    ReviewerDomainExpertise.id.not_in(superseded),
                )
                .order_by(ExpertiseDomain.normalized_label)
            )
        )

    def _parse_languages(
        self, tags: Sequence[str], levels: Sequence[str]
    ) -> dict[str, str]:
        if len(tags) != len(levels):
            raise ValueError("Language fields are inconsistent")
        result: dict[str, str] = {}
        for raw_tag, level in zip(tags, levels, strict=True):
            tag = unicodedata.normalize("NFC", raw_tag).strip()
            if not tag and not level:
                continue
            if not tag:
                raise ValueError("Choose a language for every selected language level.")
            if not LANGUAGE_TAG_RE.fullmatch(tag):
                raise ValueError("Choose each language from the language list.")
            if level not in LANGUAGE_LEVELS:
                raise ValueError(f"Choose a proficiency level for {tag}.")
            if tag in result:
                raise ValueError(f"Language {tag} was added more than once.")
            result[tag] = level
        if not result:
            raise ValueError("Add at least one language.")
        if len(result) > MAX_LANGUAGES:
            raise ValueError("Add no more than twenty languages.")
        return result

    @staticmethod
    def _parse_new_domains(labels: Sequence[str], levels: Sequence[str]) -> list[tuple[str, str, str]]:
        if len(labels) != len(levels):
            raise ValueError("New-domain fields are inconsistent")
        result: list[tuple[str, str, str]] = []
        for raw_label, level in zip(labels, levels, strict=True):
            label = unicodedata.normalize("NFC", raw_label).strip()
            if not label and not level:
                continue
            if not label:
                raise ValueError("Enter a research domain for every selected expertise level.")
            if len(label) > 200:
                raise ValueError("Research domains must be 200 characters or fewer.")
            if level not in SUBJECT_LEVELS:
                raise ValueError(f"Choose an expertise level for {label}.")
            result.append((label, normalize_domain_label(label), level))
        return result

    def _new_domain(self, entered_label: str, normalized_label: str) -> ExpertiseDomain:
        suggestion = self._suggestions_by_label.get(normalized_label)
        vocabulary_name = vocabulary_uri = vocabulary_version = None
        if suggestion and suggestion.get("vocabulary_concept_uri"):
            source = self._sources[str(suggestion["source_id"])]
            vocabulary_name = source["source_id"]
            vocabulary_uri = suggestion["vocabulary_concept_uri"]
            vocabulary_version = suggestion["vocabulary_version"]
        return ExpertiseDomain(
            id=f"domain-{uuid.uuid4()}",
            entered_label=entered_label,
            normalized_label=normalized_label,
            vocabulary_name=vocabulary_name,
            vocabulary_concept_uri=vocabulary_uri,
            vocabulary_version=vocabulary_version,
            created_by="reviewer",
        )

    @staticmethod
    def _append_domain_assertion(
        session: Session,
        reviewer_id: str,
        domain: ExpertiseDomain,
        level: str,
        asserted_at: str,
        *,
        supersedes: ReviewerDomainExpertise | None,
    ) -> None:
        record = {
            "schema": "musparql.reviewer-domain-expertise-assertion.v1",
            "id": f"domain-assertion-{uuid.uuid4()}",
            "reviewer_id": reviewer_id,
            "domain_id": domain.id,
            "entered_label": domain.entered_label,
            "normalized_label": domain.normalized_label,
            "vocabulary_name": domain.vocabulary_name,
            "vocabulary_concept_uri": domain.vocabulary_concept_uri,
            "vocabulary_version": domain.vocabulary_version,
            "expertise_level": level,
            "asserted_at": asserted_at,
            "supersedes_id": supersedes.id if supersedes else None,
        }
        history = list(
            session.scalars(
                select(ReviewerDomainExpertise)
                .where(
                    ReviewerDomainExpertise.reviewer_id == reviewer_id,
                    ReviewerDomainExpertise.domain_id == domain.id,
                )
                .order_by(ReviewerDomainExpertise.asserted_at)
            )
        )
        history_records = [
            {
                **record,
                "id": item.id,
                "expertise_level": item.expertise_level,
                "asserted_at": item.asserted_at,
                "supersedes_id": item.supersedes_id,
            }
            for item in history
        ]
        validate_reviewer_domain_expertise_assertions([*history_records, record])
        session.add(
            ReviewerDomainExpertise(
                id=record["id"],
                reviewer_id=reviewer_id,
                domain_id=domain.id,
                expertise_level=level,
                asserted_at=asserted_at,
                supersedes_id=record["supersedes_id"],
            )
        )


__all__ = [
    "LANGUAGE_LEVELS",
    "SUBJECT_LEVELS",
    "TECHNICAL_LEVELS",
    "ProfileService",
]
