"""
Module Qualité des Données
Tests : complétude, cohérence, validité
"""

import json
import logging
from datetime import datetime
from typing import Any
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataQualityReport:
    def __init__(self):
        self.checks = []
        self.passed = 0
        self.failed = 0

    def add(self, name: str, ok: bool, detail: str = ""):
        self.checks.append({"check": name, "passed": ok, "detail": detail})
        if ok: self.passed += 1
        else:   self.failed += 1

    def summary(self) -> dict:
        total = self.passed + self.failed
        return {
            "total_checks": total,
            "passed": self.passed,
            "failed": self.failed,
            "score": round(self.passed / total * 100, 1) if total else 0,
            "checks": self.checks,
            "generated_at": datetime.utcnow().isoformat(),
        }


class ArticleQualityChecker:
    """Vérifie la qualité d'un article individuel"""

    MIN_TITLE_LEN   = 10
    MIN_CONTENT_LEN = 100
    MAX_CONTENT_LEN = 100_000
    VALID_LANGS     = {"fr","en","ar","es","de","pt","unknown"}

    def check(self, article: dict) -> dict:
        report = DataQualityReport()
        title   = article.get("title", "") or ""
        content = article.get("content", "") or ""
        url     = article.get("url", "") or ""
        pub_at  = article.get("published_at", "") or ""
        source  = article.get("source", "") or ""
        lang    = article.get("language", "") or ""

        # ── Complétude ────────────────────────────────────
        report.add("title_present",   bool(title),   f"Titre: '{title[:40]}'")
        report.add("content_present", bool(content), f"Contenu: {len(content)} chars")
        report.add("url_present",     bool(url),     f"URL: {url[:60]}")
        report.add("date_present",    bool(pub_at),  f"Date: {pub_at}")
        report.add("source_present",  bool(source),  f"Source: {source}")

        # ── Validité ──────────────────────────────────────
        report.add("title_min_length",
                   len(title) >= self.MIN_TITLE_LEN,
                   f"Longueur titre: {len(title)} (min {self.MIN_TITLE_LEN})")
        report.add("content_min_length",
                   len(content) >= self.MIN_CONTENT_LEN,
                   f"Longueur contenu: {len(content)} (min {self.MIN_CONTENT_LEN})")
        report.add("content_not_too_long",
                   len(content) <= self.MAX_CONTENT_LEN,
                   f"Longueur contenu: {len(content)} (max {self.MAX_CONTENT_LEN})")
        report.add("url_valid_format",
                   bool(re.match(r'https?://.+', url)) if url else False,
                   f"URL format valide")
        report.add("language_known",
                   lang in self.VALID_LANGS,
                   f"Langue: {lang}")

        # ── Cohérence ─────────────────────────────────────
        report.add("date_parseable",
                   self._is_date_valid(pub_at),
                   f"Date parseable: {pub_at}")
        report.add("no_html_in_title",
                   '<' not in title and '>' not in title,
                   "Titre sans HTML")
        report.add("no_html_in_content",
                   self._html_ratio(content) < 0.1,
                   f"Contenu sans HTML excessif")
        report.add("title_not_duplicate_url",
                   title.lower() not in url.lower(),
                   "Titre distinct de l'URL")

        result = report.summary()
        result["article_id"] = article.get("id", "")
        result["is_valid"] = result["score"] >= 70  # Seuil
        return result

    @staticmethod
    def _is_date_valid(date_str: str) -> bool:
        if not date_str:
            return False
        formats = ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S.%f"]
        for fmt in formats:
            try:
                datetime.strptime(date_str[:len(fmt)], fmt)
                return True
            except ValueError:
                continue
        return bool(re.search(r'\d{4}', date_str))  # Permissif si contient une année

    @staticmethod
    def _html_ratio(text: str) -> float:
        if not text:
            return 0.0
        tags = len(re.findall(r'<[^>]+>', text))
        words = len(text.split())
        return tags / max(words, 1)


class BatchQualityChecker:
    """Vérifie la qualité d'un lot d'articles"""

    def __init__(self):
        self.checker = ArticleQualityChecker()

    def run(self, articles: list[dict]) -> dict:
        logger.info(f"🔍 Qualité: analyse de {len(articles)} articles...")

        individual_reports = [self.checker.check(a) for a in articles]
        valid_count  = sum(1 for r in individual_reports if r["is_valid"])
        invalid_count = len(individual_reports) - valid_count

        # Agréger les erreurs
        error_counts: dict[str, int] = {}
        for rep in individual_reports:
            for chk in rep["checks"]:
                if not chk["passed"]:
                    error_counts[chk["check"]] = error_counts.get(chk["check"], 0) + 1

        avg_score = sum(r["score"] for r in individual_reports) / len(individual_reports) if individual_reports else 0

        batch_report = {
            "batch_checked_at": datetime.utcnow().isoformat(),
            "total_articles": len(articles),
            "valid_articles": valid_count,
            "invalid_articles": invalid_count,
            "validity_rate": round(valid_count / len(articles) * 100, 1) if articles else 0,
            "average_quality_score": round(avg_score, 1),
            "top_errors": sorted(error_counts.items(), key=lambda x: -x[1])[:10],
            "individual_reports": individual_reports,
        }

        logger.info(f"✅ Qualité: {valid_count}/{len(articles)} valides ({batch_report['validity_rate']}%)")
        logger.info(f"   Score moyen: {avg_score:.1f}%")
        if error_counts:
            top_err = sorted(error_counts.items(), key=lambda x: -x[1])[:3]
            logger.info(f"   Top erreurs: {top_err}")

        return batch_report

    def get_valid_articles(self, articles: list[dict]) -> list[dict]:
        """Retourne uniquement les articles valides"""
        valid = []
        for art in articles:
            rep = self.checker.check(art)
            if rep["is_valid"]:
                valid.append(art)
        return valid


if __name__ == "__main__":
    # Test avec des articles fictifs
    test_articles = [
        {
            "id": "abc123",
            "title": "Le gouvernement annonce de nouvelles mesures économiques",
            "author": "Jean Dupont",
            "published_at": "2026-05-09T10:00:00",
            "category": "Économie",
            "content": "Le gouvernement a annoncé ce matin plusieurs mesures visant à soutenir l'économie nationale. " * 5,
            "source": "Le Monde",
            "url": "https://lemonde.fr/article/123",
            "language": "fr",
        },
        {
            "id": "def456",
            "title": "",   # ← titre manquant
            "author": "",
            "published_at": "",   # ← date manquante
            "category": "Politique",
            "content": "Court.",  # ← contenu trop court
            "source": "Unknown",
            "url": "",
            "language": "xx",
        },
    ]

    checker = BatchQualityChecker()
    report = checker.run(test_articles)
    print(json.dumps(report, indent=2, ensure_ascii=False))