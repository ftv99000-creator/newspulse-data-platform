"""
News Article Scraper
Collecte des articles depuis plusieurs sources d'actualité
"""

import requests
from bs4 import BeautifulSoup
import json
import uuid
import hashlib
from datetime import datetime
from langdetect import detect
import logging
import time
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ArticleScraper:
    """Scraper générique pour sites d'actualité"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; NewsBot/1.0)',
            'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8,ar;q=0.7'
        })

    def _generate_id(self, url: str) -> str:
        return hashlib.md5(url.encode()).hexdigest()

    def _safe_get(self, url: str, timeout: int = 10) -> BeautifulSoup | None:
        try:
            resp = self.session.get(url, timeout=timeout)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, 'html.parser')
        except Exception as e:
            logger.error(f"Erreur GET {url}: {e}")
            return None

    def _detect_language(self, text: str) -> str:
        try:
            return detect(text[:500])
        except Exception:
            return "unknown"

    def scrape_hespress(self) -> list[dict]:
        """Scraper pour Hespress.com"""
        articles = []
        base_url = "https://www.hespress.com"
        soup = self._safe_get(base_url)
        if not soup:
            return articles

        for item in soup.select("article.post")[:20]:
            try:
                title_el = item.select_one("h2.post-title a, h3.post-title a")
                link_el = item.select_one("a[href]")
                date_el = item.select_one("span.time, time")
                cat_el = item.select_one("span.category, a.category")

                if not title_el:
                    continue

                url = link_el['href'] if link_el else ""
                if url and not url.startswith("http"):
                    url = base_url + url

                # Récupérer contenu de l'article
                content = ""
                author = "Hespress"
                if url:
                    art_soup = self._safe_get(url)
                    if art_soup:
                        content_el = art_soup.select_one("div.post-content, div.article-content")
                        if content_el:
                            content = content_el.get_text(separator=' ', strip=True)
                        author_el = art_soup.select_one("span.author, a.author")
                        if author_el:
                            author = author_el.get_text(strip=True)

                article = {
                    "id": self._generate_id(url),
                    "title": title_el.get_text(strip=True),
                    "author": author,
                    "published_at": date_el.get_text(strip=True) if date_el else datetime.utcnow().isoformat(),
                    "category": cat_el.get_text(strip=True) if cat_el else "General",
                    "content": content,
                    "source": "Hespress",
                    "source_country": "MA",
                    "url": url,
                    "language": self._detect_language(title_el.get_text(strip=True)),
                    "scraped_at": datetime.utcnow().isoformat(),
                    "raw_html": str(item)
                }
                articles.append(article)
                time.sleep(0.5)  # Politeness delay
            except Exception as e:
                logger.error(f"Erreur parsing article Hespress: {e}")

        logger.info(f"Hespress: {len(articles)} articles collectés")
        return articles

    def scrape_aljazeera(self) -> list[dict]:
        """Scraper pour Al Jazeera"""
        articles = []
        base_url = "https://www.aljazeera.com"
        soup = self._safe_get(base_url)
        if not soup:
            return articles

        for item in soup.select("article, div.article-card")[:20]:
            try:
                title_el = item.select_one("h2, h3, .article-card__title")
                link_el = item.select_one("a[href]")
                date_el = item.select_one("time, .article-card__date")
                cat_el = item.select_one(".article-card__topic, .topic")

                if not title_el:
                    continue

                url = link_el['href'] if link_el else ""
                if url and not url.startswith("http"):
                    url = base_url + url

                content = ""
                author = "Al Jazeera"
                if url and url.startswith("http"):
                    art_soup = self._safe_get(url)
                    if art_soup:
                        content_el = art_soup.select_one("div.wysiwyg, div.article-p-wrapper")
                        if content_el:
                            content = content_el.get_text(separator=' ', strip=True)
                        author_el = art_soup.select_one("a.article-author__name, span.author-name")
                        if author_el:
                            author = author_el.get_text(strip=True)

                article = {
                    "id": self._generate_id(url),
                    "title": title_el.get_text(strip=True),
                    "author": author,
                    "published_at": date_el.get('datetime', date_el.get_text(strip=True)) if date_el else datetime.utcnow().isoformat(),
                    "category": cat_el.get_text(strip=True) if cat_el else "General",
                    "content": content,
                    "source": "Al Jazeera",
                    "source_country": "QA",
                    "url": url,
                    "language": self._detect_language(title_el.get_text(strip=True)),
                    "scraped_at": datetime.utcnow().isoformat(),
                    "raw_html": str(item)
                }
                articles.append(article)
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"Erreur parsing article AJ: {e}")

        logger.info(f"Al Jazeera: {len(articles)} articles collectés")
        return articles

    def scrape_bbc(self) -> list[dict]:
        """Scraper pour BBC News"""
        articles = []
        base_url = "https://www.bbc.com/news"
        soup = self._safe_get(base_url)
        if not soup:
            return articles

        for item in soup.select("div[data-testid='card-headline'], h3.sc-4fedabc7-3")[:20]:
            try:
                title = item.get_text(strip=True)
                parent = item.find_parent('a') or item.find_parent('div')
                link = parent.get('href', '') if parent else ''
                if link and not link.startswith('http'):
                    link = "https://www.bbc.com" + link

                article = {
                    "id": self._generate_id(link or title),
                    "title": title,
                    "author": "BBC News",
                    "published_at": datetime.utcnow().isoformat(),
                    "category": "News",
                    "content": "",
                    "source": "BBC News",
                    "source_country": "GB",
                    "url": link,
                    "language": "en",
                    "scraped_at": datetime.utcnow().isoformat(),
                    "raw_html": str(item)
                }
                articles.append(article)
            except Exception as e:
                logger.error(f"Erreur parsing BBC: {e}")

        logger.info(f"BBC: {len(articles)} articles collectés")
        return articles

    def scrape_all(self) -> list[dict]:
        """Lance tous les scrapers"""
        all_articles = []
        scrapers = [
            self.scrape_hespress,
            self.scrape_aljazeera,
            self.scrape_bbc,
        ]
        for scraper_fn in scrapers:
            try:
                articles = scraper_fn()
                all_articles.extend(articles)
            except Exception as e:
                logger.error(f"Erreur scraper {scraper_fn.__name__}: {e}")
        logger.info(f"Total: {len(all_articles)} articles collectés")
        return all_articles


if __name__ == "__main__":
    scraper = ArticleScraper()
    articles = scraper.scrape_all()
    with open("/tmp/articles_raw.json", "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)
    print(f"✅ {len(articles)} articles sauvegardés dans /tmp/articles_raw.json")