#!/usr/bin/env python3
"""
Wayfair PDP Scrapy Scraper
==========================
High-performance Scrapy-based crawler for Wayfair Product Detail Pages (PDP).
Features:
- Rotating Proxy Middleware with cooldown & auto-failover (HTTP, HTTPS, SOCKS5).
- Dynamic JavaScript rendering via Playwright (scrapy-playwright) with lazy-load scrolling.
- Traversed URL & SKU Deduplication against existing database (wayfair_products.json).
- Comprehensive multi-level camelCase extraction (Name, Brand, SKU, Breadcrumbs, Price,
  Color & Variants, At a Glance, Description & Features, Specifications, Documents, Images).
- Parallel Sharding (--shard-index / --shard-total) and Shard Merging (--merge-dir).
"""

import os
import sys
import re
import json
import time
import random
import logging
import argparse
import threading
from typing import Dict, Any, List, Optional, Set, Tuple
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup

# Scrapy & Twisted imports
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy import signals
from scrapy.utils.response import response_status_message

logger = logging.getLogger("WayfairScrapy")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
]


# ============================================================================
# 1. URL NORMALIZATION & CAMELCASE UTILITIES
# ============================================================================

def normalize_pdp_url(url: str, shop_product_type: str = "furniture") -> str:
    """
    Transforms any Wayfair product URL into canonical PDP format:
    https://www.wayfair.com/<type>/pdp/<slug>.html?piid=<piid>
    """
    url = url.strip()
    if not url:
        return ""
    
    parsed = urlparse(url)
    domain = f"{parsed.scheme}://{parsed.netloc}" if parsed.netloc else "https://www.wayfair.com"
    path = parsed.path
    qs = parse_qs(parsed.query)

    piid = None
    for k, v in qs.items():
        if k.lower() in ["piid", "piid[]", "piid%5b%5d"]:
            if v and v[0]:
                piid = v[0]
                break

    if "/pdp/" in path:
        clean_path = path.lower()
        clean_path = re.sub(r'-+', '-', clean_path)
        clean_url = f"{domain}{clean_path}"
        if piid:
            clean_url += f"?piid={piid}"
        return clean_url

    filename = path.strip("/").split("/")[-1]
    if filename.lower().endswith(".html"):
        name_part = filename[:-5]
    else:
        name_part = filename

    m_sku = re.search(r'~(CMU[0-9]+|[A-Za-z0-9]+)$', name_part, re.I)
    if m_sku:
        sku = m_sku.group(1).lower()
        title_part = name_part[:m_sku.start()]
        title_part = re.sub(r'-[A-Za-z0-9]+-[A-Za-z0-9]+-[A-Za-z0-9]+$', '', title_part)
        slug_raw = f"{title_part}-{sku}"
    else:
        slug_raw = name_part

    slug = re.sub(r'[^a-zA-Z0-9]', '-', slug_raw)
    slug = re.sub(r'-+', '-', slug).strip('-').lower()

    new_path = f"/{shop_product_type}/pdp/{slug}.html"
    new_url = f"{domain}{new_path}"
    if piid:
        new_url += f"?piid={piid}"
    return new_url


def to_camel_case(s: str) -> str:
    """Converts string to camelCase."""
    s = re.sub(r'[\s\-_]+', ' ', s).strip()
    if not s:
        return ""
    words = s.split(' ')
    return words[0].lower() + ''.join(w.capitalize() for w in words[1:])


# ============================================================================
# 2. COMPREHENSIVE WAYFAIR HTML & HYDRATION DATA PARSER
# ============================================================================

class WayfairParser:
    """
    Extracts multi-level structured camelCase JSON from Wayfair HTML and embedded Next.js data.
    """
    def __init__(self, html: str, provided_url: str, page_url: str):
        self.soup = BeautifulSoup(html, "html.parser")
        self.html = html
        self.provided_url = provided_url
        self.page_url = page_url
        self.next_data = self._extract_next_data()

    def _extract_next_data(self) -> Dict[str, Any]:
        """Extracts JSON from __NEXT_DATA__ script tag if present."""
        try:
            script = self.soup.find("script", id="__NEXT_DATA__")
            if script and script.string:
                return json.loads(script.string)
        except Exception:
            pass
        return {}

    def parse(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "providedUrl": self.provided_url,
            "pageUrl": self.page_url,
            "name": self.extract_name(),
            "brand": self.extract_brand(),
            "sku": self.extract_sku(),
            "breadcrumbs": self.extract_breadcrumbs(),
            "price": self.extract_price(),
            "colorAndVariants": self.extract_variants(),
            "atAGlance": self.extract_at_a_glance(),
            "description": self.extract_description(),
            "specifications": self.extract_specifications(),
            "images": self.extract_images()
        }
        return data

    def extract_name(self) -> str:
        h1 = self.soup.find("h1")
        if h1:
            return h1.get_text(strip=True)
        meta_title = self.soup.find("meta", property="og:title")
        if meta_title and meta_title.get("content"):
            return meta_title["content"].split("|")[0].strip()
        return ""

    def extract_brand(self) -> str:
        h1 = self.soup.find("h1")
        if h1:
            p = h1
            for _ in range(4):
                if p:
                    b_link = p.find("a", href=re.compile(r"/brand/bnd/"))
                    if b_link:
                        return b_link.get_text(strip=True)
                    by_node = p.find(string=re.compile(r"^By\s+[A-Za-z0-9]", re.I))
                    if by_node:
                        return re.sub(r"^By\s+", "", by_node.strip(), flags=re.I).strip()
                    p = p.parent
        b_node = self.soup.find(lambda t: t.name != "script" and re.match(r"^By\s+[A-Za-z0-9]", t.get_text(strip=True)))
        if b_node:
            return re.sub(r"^By\s+", "", b_node.get_text(strip=True), flags=re.I).strip()
        return ""

    def extract_sku(self) -> str:
        for m in self.soup.find_all(string=re.compile(r"SKU:\s*[A-Z0-9]+", re.I)):
            if m.parent and m.parent.name != "script" and len(m.strip()) < 40:
                match = re.search(r"SKU:\s*([A-Za-z0-9]+)", m, re.I)
                if match:
                    val = match.group(1).upper()
                    if val != "DISPLAYLISTINGID":
                        return val
        m_url = re.search(r'([A-Za-z]{3}[0-9]{4,6})\.html', self.page_url, re.I)
        if m_url:
            return m_url.group(1).upper()
        return ""

    def extract_breadcrumbs(self) -> List[str]:
        breadcrumbs = []
        for s in self.soup.find_all("script", type="application/ld+json"):
            try:
                d = json.loads(s.string)
                if d.get("@type") == "BreadcrumbList":
                    for item in d.get("itemListElement", []):
                        name = item.get("item", {}).get("name") or item.get("name")
                        if name and name not in breadcrumbs:
                            breadcrumbs.append(name.strip())
            except Exception:
                pass
        
        if not breadcrumbs:
            for nav in self.soup.find_all(["nav", "ol", "ul"]):
                aria = nav.get("aria-label", "").lower()
                if "breadcrumb" in aria:
                    for a in nav.find_all("a"):
                        txt = a.get_text(strip=True)
                        if txt and txt not in breadcrumbs:
                            breadcrumbs.append(txt)
        return breadcrumbs

    def extract_price(self) -> Dict[str, Any]:
        price_data: Dict[str, Any] = {
            "currentPrice": None,
            "originalPrice": None,
            "discountPercentage": None,
            "financingDetails": None,
            "rewardsOffer": None
        }

        # 1. Primary: PricingFull-leadPrice
        lead_price_elem = self.soup.find(attrs={"data-test-id": "PricingFull-leadPrice"})
        if lead_price_elem:
            price_text = lead_price_elem.get_text(strip=True)
            m = re.search(r"\$\d+(?:,\d+)*(?:\.\d{2})?", price_text)
            if m:
                price_data["currentPrice"] = m.group(0)

            pricing_block = lead_price_elem.find_parent(lambda t: t.name in ["div", "section"] and (
                t.get("data-name") == "Pricing" or 
                t.get("data-node-id", "").startswith("PricingFull") or 
                t.find(attrs={"data-test-id": "StandardPricingPrice-PREVIOUS"}) or
                t.find("s")
            )) or lead_price_elem.parent.parent.parent

            if pricing_block:
                strike = pricing_block.find(attrs={"data-test-id": "StandardPricingPrice-PREVIOUS"}) or pricing_block.find("s")
                if strike:
                    m_orig = re.search(r"\$\d+(?:,\d+)*(?:\.\d{2})?", strike.get_text(strip=True))
                    if m_orig:
                        price_data["originalPrice"] = m_orig.group(0)

                disc = pricing_block.find(string=re.compile(r"\d+%\s*Off", re.I))
                if disc:
                    m_disc = re.search(r"\d+%\s*Off", disc, re.I)
                    if m_disc:
                        price_data["discountPercentage"] = m_disc.group(0)

        # 2. Fallback: Search in Buy Box
        if not price_data["currentPrice"]:
            cart_btn = self.soup.find(lambda t: t.name in ["button", "input"] and "Add to Cart" in t.get_text())
            if cart_btn:
                curr = cart_btn
                buybox = None
                while curr and curr.name != "body":
                    if curr.name in ["div", "section"] and (curr.find("h1") or "Body Fabric" in curr.get_text() or "Color" in curr.get_text()):
                        buybox = curr
                        break
                    curr = curr.parent

                search_scope = buybox if buybox else self.soup
                for elem in search_scope.find_all(attrs={"data-name-id": "PriceDisplay"}):
                    txt = elem.get_text(strip=True)
                    if re.match(r"^\$\d+(?:,\d+)*(?:\.\d{2})?$", txt):
                        price_data["currentPrice"] = txt
                        break

        # 3. Fallback: Generic search
        if not price_data["currentPrice"]:
            strike = self.soup.find("s", string=re.compile(r"^\$\d+[\d,.]*$"))
            if strike:
                price_data["originalPrice"] = strike.get_text(strip=True)
                p_box = strike.find_parent("div")
                if p_box:
                    cur = p_box.find("span", string=re.compile(r"^\$\d+[\d,.]*$"))
                    if cur:
                        price_data["currentPrice"] = cur.get_text(strip=True)
                    disc = p_box.find(string=re.compile(r"\d+%\s*Off", re.I))
                    if disc:
                        price_data["discountPercentage"] = disc.strip()

        if not price_data["currentPrice"]:
            for span in self.soup.find_all("span", string=re.compile(r"^\$\d+[\d,.]*$")):
                if span.parent and span.parent.name != "s":
                    price_data["currentPrice"] = span.get_text(strip=True)
                    break

        # Financing
        mo_span = self.soup.find("span", string=re.compile(r"/mo", re.I))
        if mo_span:
            mo_box = mo_span.find_parent(lambda t: t.name in ["div", "p"] and len(t.get_text(strip=True)) < 250)
            if mo_box:
                cleaned = BeautifulSoup(str(mo_box), "html.parser")
                for sr in cleaned.find_all(attrs={"data-test-id": re.compile(r"ScreenReaderText", re.I)}):
                    sr.decompose()
                txt = cleaned.get_text(" ", strip=True)
                txt = re.sub(r"\{[A-Za-z0-9_]+\}", "", txt)
                txt = re.sub(r"\b(\w+(\s+\w+){0,3})\s+\1\b", r"\1", txt, flags=re.I)
                txt = re.sub(r"\s+", " ", txt).strip()
                price_data["financingDetails"] = txt

        # Rewards offer
        rew = self.soup.find(string=re.compile(r"Earn\s+\$\d+.*in\s+rewards", re.I))
        if rew:
            p_rew = rew.find_parent(lambda t: t.name in ["div", "p"] and len(t.get_text(strip=True)) < 250)
            if p_rew:
                cleaned = BeautifulSoup(str(p_rew), "html.parser")
                for sr in cleaned.find_all(attrs={"data-test-id": re.compile(r"ScreenReaderText", re.I)}):
                    sr.decompose()
                txt = cleaned.get_text(" ", strip=True)
                txt = re.sub(r"\b(\w+(\s+\w+){2,6})\s+\1\b", r"\1", txt, flags=re.I)
                price_data["rewardsOffer"] = re.sub(r"\s+", " ", txt).strip()

        return price_data

    def extract_variants(self) -> Dict[str, Any]:
        variants_data: Dict[str, Any] = {
            "selectedColor": None,
            "availableOptions": []
        }

        c_node = self.soup.find(string=lambda t: t and t.strip() in ["Color", "Color:"])
        if c_node:
            p_row = c_node.find_parent(lambda t: t.name in ["div", "p"] and "Color" in t.get_text())
            if p_row:
                txt = p_row.get_text(" ", strip=True)
                m_c = re.search(r"Color\s*:?\s*([A-Za-z0-9\s]+)", txt)
                if m_c:
                    val = m_c.group(1).split("FREE")[0].split("Get it")[0].strip()
                    if val:
                        variants_data["selectedColor"] = val

        seen = set()
        for img in self.soup.find_all("img"):
            alt = img.get("alt", "").strip()
            src = img.get("src") or img.get("data-src") or ""
            if alt and any(k in alt.lower() for k in ["selected", "out of stock"]):
                if alt not in seen:
                    seen.add(alt)
                    opt_name = re.sub(r"\s+(selected|is out of stock)", "", alt, flags=re.I).strip()
                    status = "selected" if "selected" in alt.lower() else "out of stock"
                    variants_data["availableOptions"].append({
                        "name": opt_name,
                        "rawLabel": alt,
                        "status": status,
                        "thumbnailImage": src
                    })
                    if status == "selected" and not variants_data["selectedColor"]:
                        variants_data["selectedColor"] = opt_name

        return variants_data

    def extract_at_a_glance(self) -> List[str]:
        items: List[str] = []
        glance_h = self.soup.find(lambda t: t.name in ["h2", "h3"] and "At a Glance" in t.get_text())
        if glance_h:
            p_box = glance_h.parent
            for p in p_box.find_all("p"):
                txt = p.get_text(strip=True)
                if txt and txt != "At a Glance" and txt not in items and len(txt) < 100:
                    items.append(txt)
        return items

    def extract_description(self) -> Dict[str, Any]:
        desc_data: Dict[str, Any] = {
            "aboutThisProduct": "",
            "features": []
        }

        about_h = self.soup.find(lambda t: t.name in ["p", "h2", "h3", "div"] and t.get_text(strip=True) == "About This Product")
        if about_h:
            parent_div = about_h.find_parent("div")
            paras = []
            if parent_div and parent_div.parent:
                for div in parent_div.parent.find_all("div", class_=lambda c: c and "_1yxomy20" in c):
                    txt = div.get_text(strip=True)
                    if txt and txt not in paras:
                        paras.append(txt)
            desc_data["aboutThisProduct"] = " ".join(paras)

        feat_h = self.soup.find(lambda t: t.name in ["p", "h2", "h3", "div"] and t.get_text(strip=True) == "Features")
        if feat_h:
            parent_div = feat_h.find_parent("div")
            features = []
            if parent_div and parent_div.parent:
                container = parent_div.parent
                for li in container.find_all("li"):
                    txt = li.get_text(strip=True)
                    if txt and txt not in features:
                        features.append(txt)
            desc_data["features"] = features

        return desc_data

    def extract_documents(self) -> List[Dict[str, str]]:
        documents = []
        seen_urls = set()

        doc_heading = self.soup.find(lambda t: t.name in ["p", "h2", "h3", "h4", "div", "span"] and t.get_text(strip=True).lower() == "documents")
        if doc_heading:
            box = doc_heading.find_parent(lambda t: t.name in ["div", "section"] and t.find_all("a"))
            if box:
                for a in box.find_all("a"):
                    href = a.get("href")
                    if href and href not in seen_urls:
                        seen_urls.add(href)
                        documents.append({
                            "name": a.get_text(strip=True),
                            "url": href
                        })

        for a in self.soup.find_all("a"):
            href = a.get("href", "")
            txt = a.get_text(strip=True)
            if (".pdf" in href.lower() or "/document/" in href.lower() or "(pdf)" in txt.lower()) and href not in seen_urls:
                seen_urls.add(href)
                documents.append({
                    "name": txt,
                    "url": href
                })

        return documents

    def extract_specifications(self) -> Dict[str, Any]:
        specs: Dict[str, Any] = {
            "productDimensions": {
                "dimensionImageUrl": None,
                "measurements": {}
            },
            "otherDimensions": {},
            "documents": self.extract_documents(),
            "details": {},
            "assembly": {}
        }

        prod_dim_p = self.soup.find(lambda t: t.name == "p" and t.get_text(strip=True) == "Product Dimensions")
        if prod_dim_p:
            box = prod_dim_p.find_parent("div")
            if box and box.parent:
                img = box.parent.find("img")
                if img:
                    specs["productDimensions"]["dimensionImageUrl"] = img.get("src") or img.get("data-src")
                dl = box.parent.find("dl")
                if dl:
                    for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
                        k = to_camel_case(dt.get_text(strip=True))
                        specs["productDimensions"]["measurements"][k] = dd.get_text(strip=True)

        other_dim_p = self.soup.find(lambda t: t.name == "p" and t.get_text(strip=True) == "Other Dimensions")
        if other_dim_p:
            box = other_dim_p.find_parent("div")
            if box:
                dl = box.find_next_sibling("dl") or (box.parent and box.parent.find("dl"))
                if dl:
                    for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
                        k = to_camel_case(dt.get_text(strip=True))
                        specs["otherDimensions"][k] = dd.get_text(strip=True)

        for dl in self.soup.find_all("dl"):
            dts = [dt.get_text(strip=True) for dt in dl.find_all("dt")]
            dds = [dd.get_text(strip=True) for dd in dl.find_all("dd")]
            for raw_k, v in zip(dts, dds):
                k = to_camel_case(raw_k)
                if "assembly" in raw_k.lower():
                    specs["assembly"][k] = v
                elif k not in specs["otherDimensions"] and k not in specs["productDimensions"]["measurements"]:
                    specs["details"][k] = v

        return specs

    def extract_images(self) -> Dict[str, Any]:
        images_data: Dict[str, Any] = {
            "mainImage": None,
            "galleryImages": []
        }

        product_name = self.extract_name()
        sig_words = [w.lower() for w in re.sub(r'[^a-zA-Z0-9\s]', ' ', product_name).split() if len(w) > 3]

        seen_ids = set()
        gallery: List[str] = []

        for img in self.soup.find_all("img"):
            src = img.get("src") or img.get("data-src") or ""
            alt = (img.get("alt") or "").strip()

            if "assets.wfcdn.com" not in src:
                continue

            m = re.search(r'assets\.wfcdn\.com/im/([0-9a-zA-Z]+)/resize-[^/]+/([0-9]+)/([0-9]+)/([^?#]+)', src)
            if not m:
                continue

            im_hash, subfolder, image_id, filename = m.groups()
            fn_lower = filename.lower()

            if any(bad in fn_lower for bad in ['default_name', 'logo', 'badge', 'icon', 'rating', 'star', 'swatch']):
                continue
            if fn_lower in ['black.jpg', 'white.jpg', 'espresso.jpg']:
                continue

            parent_bad = img.find_parent(lambda t: t.name in ['div', 'section'] and any(k in " ".join(t.get('class', [])).lower() for k in ['sponsored', 'similar', 'crosssell', 'browsegrid']))
            if parent_bad:
                continue

            if sig_words:
                src_alt_text = f"{src.lower()} {alt.lower()}"
                matches = sum(1 for w in sig_words if w in src_alt_text)
                if matches < min(2, len(sig_words)):
                    continue

            if image_id not in seen_ids:
                seen_ids.add(image_id)
                high_res_url = f"https://assets.wfcdn.com/im/{im_hash}/resize-h800-w800%5Ecompr-r85/{subfolder}/{image_id}/{filename}"
                gallery.append(high_res_url)

        if gallery:
            images_data["mainImage"] = gallery[0]
            images_data["galleryImages"] = gallery

        return images_data


# ============================================================================
# 3. DEDUPLICATION & TRAVERSED TRACKER (12k+ RECORD SHIELD)
# ============================================================================

class TraversedTracker:
    """
    Manages deduplication against existing database (wayfair_products.json).
    Ensures that none of the 12k+ already scraped records are repeated.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self.exact_urls: Set[str] = set()
        self.normalized_urls: Set[str] = set()
        self.skus: Set[str] = set()
        self.existing_records: List[Dict[str, Any]] = []

    def load_from_file(self, filepath: str) -> int:
        if not filepath or not os.path.exists(filepath):
            return 0

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if not content:
                    return 0
                try:
                    data = json.loads(content)
                except json.JSONDecodeError:
                    cleaned = re.sub(r',\s*([\]\}])', r'\1', content)
                    data = json.loads(cleaned)

            records = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
            loaded = 0
            with self._lock:
                for item in records:
                    if not isinstance(item, dict):
                        continue
                    if item.get("error") and not item.get("name") and not item.get("sku"):
                        continue

                    self.existing_records.append(item)
                    loaded += 1

                    for k in ["providedUrl", "provided_url", "pageUrl", "page_url"]:
                        u = item.get(k)
                        if u and isinstance(u, str):
                            u_str = u.strip()
                            self.exact_urls.add(u_str)
                            self.exact_urls.add(u_str.lower())
                            norm = normalize_pdp_url(u_str)
                            if norm:
                                self.normalized_urls.add(norm)
                                self.normalized_urls.add(norm.lower())

                    sku = item.get("sku") or item.get("SKU")
                    if sku and isinstance(sku, str):
                        self.skus.add(sku.strip().upper())

            logger.info(f"Loaded {loaded} traversed record(s) from '{filepath}'")
            return loaded
        except Exception as e:
            logger.warning(f"Could not load traversed products from '{filepath}': {e}")
            return 0

    def is_traversed(self, url: str) -> bool:
        u = url.strip()
        if not u:
            return True
        with self._lock:
            if u in self.exact_urls or u.lower() in self.exact_urls:
                return True
            norm = normalize_pdp_url(u)
            if norm and (norm in self.normalized_urls or norm.lower() in self.normalized_urls):
                return True
        return False

    def mark_traversed(self, url: str, item: Optional[Dict[str, Any]] = None):
        with self._lock:
            u_str = url.strip()
            self.exact_urls.add(u_str)
            self.exact_urls.add(u_str.lower())
            norm = normalize_pdp_url(u_str)
            if norm:
                self.normalized_urls.add(norm)
                self.normalized_urls.add(norm.lower())
            if item:
                for k in ["providedUrl", "provided_url", "pageUrl", "page_url"]:
                    pu = item.get(k)
                    if pu and isinstance(pu, str):
                        pu_str = pu.strip()
                        self.exact_urls.add(pu_str)
                        self.exact_urls.add(pu_str.lower())
                        p_norm = normalize_pdp_url(pu_str)
                        if p_norm:
                            self.normalized_urls.add(p_norm)
                            self.normalized_urls.add(p_norm.lower())
                sku = item.get("sku")
                if sku and isinstance(sku, str):
                    self.skus.add(sku.strip().upper())

    def filter_urls(self, urls: List[str]) -> Tuple[List[str], int]:
        fresh_urls = []
        seen_batch = set()
        skipped = 0

        for raw_url in urls:
            u = raw_url.strip()
            if not u or u.startswith("#"):
                continue

            if self.is_traversed(u):
                skipped += 1
                continue

            norm = normalize_pdp_url(u) or u
            norm_key = norm.lower()
            if norm_key in seen_batch:
                skipped += 1
                continue

            seen_batch.add(norm_key)
            fresh_urls.append(u)

        return fresh_urls, skipped


# ============================================================================
# 4. PROXY POOL & SCRAPY ROTATING PROXY MIDDLEWARE
# ============================================================================

def load_proxies(
    cli_proxy: Optional[str] = None,
    proxy_file: Optional[str] = None
) -> List[str]:
    """
    Loads and normalizes proxy URLs from CLI, files, and environment variables.
    Supported formats:
    - http://user:pass@host:port
    - http://host:port
    - socks5://user:pass@host:port
    - host:port:user:pass (converted automatically to http://user:pass@host:port)
    - host:port
    """
    candidates: List[str] = []

    # 1. CLI proxy argument
    if cli_proxy:
        if "," in cli_proxy:
            candidates.extend(cli_proxy.split(","))
        elif os.path.isfile(cli_proxy):
            proxy_file = cli_proxy
        else:
            candidates.append(cli_proxy)

    # 2. Proxy file (or proxies.txt in current directory)
    target_file = proxy_file or ("proxies.txt" if os.path.isfile("proxies.txt") else None)
    if target_file and os.path.isfile(target_file):
        logger.info(f"Loading proxies from file: '{target_file}'")
        with open(target_file, "r", encoding="utf-8") as f:
            for line in f:
                p = line.strip()
                if p and not p.startswith("#"):
                    candidates.append(p)

    # 3. Environment variables
    if not candidates:
        for env_var in ["PROXY_URL", "PROXIES_FILE", "ROTATING_PROXIES", "HTTP_PROXY", "HTTPS_PROXY"]:
            val = os.environ.get(env_var)
            if val:
                if os.path.isfile(val):
                    with open(val, "r", encoding="utf-8") as f:
                        for line in f:
                            p = line.strip()
                            if p and not p.startswith("#"):
                                candidates.append(p)
                else:
                    candidates.extend(re.split(r'[,\s]+', val))

    # Clean and normalize formats
    clean_proxies: List[str] = []
    seen = set()
    for raw in candidates:
        p = raw.strip().strip("'\"")
        if not p or p.startswith("#"):
            continue

        # Format: host:port:user:pass -> http://user:pass@host:port
        parts = p.split(":")
        if len(parts) == 4 and not p.startswith("http"):
            host, port, user, password = parts
            p = f"http://{user}:{password}@{host}:{port}"
        elif len(parts) == 2 and not p.startswith("http") and not p.startswith("socks"):
            p = f"http://{p}"

        if p not in seen:
            seen.add(p)
            clean_proxies.append(p)

    return clean_proxies


class RotatingProxyMiddleware:
    """
    Scrapy Downloader Middleware for rotating proxies per request with failover and cooldown.
    """
    def __init__(self, proxies: List[str], cooldown_seconds: float = 60.0):
        self.proxies = proxies
        self.cooldown_seconds = cooldown_seconds
        self.banned_cooldowns: Dict[str, float] = {}  # proxy -> expiry timestamp
        self._lock = threading.Lock()
        self._index = 0

    @classmethod
    def from_crawler(cls, crawler):
        proxies = crawler.settings.getlist("ROTATING_PROXY_LIST") or []
        cooldown = crawler.settings.getfloat("ROTATING_PROXY_COOLDOWN", 60.0)
        mw = cls(proxies=proxies, cooldown_seconds=cooldown)
        return mw

    def _get_next_proxy(self) -> Optional[str]:
        with self._lock:
            if not self.proxies:
                return None
            now = time.time()
            # Clean expired cooldowns
            for p in list(self.banned_cooldowns.keys()):
                if now >= self.banned_cooldowns[p]:
                    del self.banned_cooldowns[p]

            available = [p for p in self.proxies if p not in self.banned_cooldowns]
            if available:
                proxy = available[self._index % len(available)]
                self._index += 1
                return proxy

            # If all are cooling down, pick the one with earliest cooldown expiry
            return min(self.proxies, key=lambda p: self.banned_cooldowns.get(p, 0))

    def _mark_failed(self, proxy: str):
        if not proxy:
            return
        with self._lock:
            self.banned_cooldowns[proxy] = time.time() + self.cooldown_seconds
            avail = len(self.proxies) - len(self.banned_cooldowns)
            logger.warning(f"Proxy [{proxy}] temporarily cooling down ({self.cooldown_seconds}s). {avail} proxy(s) available.")

    def process_request(self, request, spider):
        if not self.proxies:
            return

        proxy = self._get_next_proxy()
        if proxy:
            request.meta["proxy"] = proxy
            request.meta["download_timeout"] = 35
            # For Playwright requests, attach proxy context
            if request.meta.get("playwright"):
                request.meta["playwright_context_kwargs"] = {
                    "proxy": {"server": proxy},
                    "ignore_https_errors": True,
                    "user_agent": random.choice(DEFAULT_USER_AGENTS)
                }

    def process_response(self, request, response, spider):
        proxy = request.meta.get("proxy")
        if response.status in [403, 429, 502, 503, 504]:
            if proxy:
                self._mark_failed(proxy)
            reason = response_status_message(response.status)
            logger.warning(f"HTTP {response.status} ({reason}) on {request.url} via proxy [{proxy}]. Retrying with fresh proxy...")
            retries = request.meta.get("proxy_retries", 0) + 1
            if retries <= 5:
                new_req = request.copy()
                new_req.dont_filter = True
                new_req.meta["proxy_retries"] = retries
                return new_req
        return response

    def process_exception(self, request, exception, spider):
        proxy = request.meta.get("proxy")
        if proxy:
            self._mark_failed(proxy)
        logger.warning(f"Proxy Exception on {request.url} via [{proxy}]: {exception}. Retrying...")
        retries = request.meta.get("proxy_retries", 0) + 1
        if retries <= 5:
            new_req = request.copy()
            new_req.dont_filter = True
            new_req.meta["proxy_retries"] = retries
            return new_req
        return None


# ============================================================================
# 5. SCRAPY SPIDER IMPLEMENTATION
# ============================================================================

def get_playwright_page_method(method: str, *args, **kwargs):
    """Helper creating scrapy_playwright PageMethod if installed."""
    try:
        from scrapy_playwright.page import PageMethod
        return PageMethod(method, *args, **kwargs)
    except Exception:
        return None


class WayfairProductSpider(scrapy.Spider):
    name = "wayfair_product_spider"
    handle_httpstatus_list = [403, 404, 429, 500, 502, 503, 504]

    def __init__(
        self,
        urls: List[str],
        tracker: TraversedTracker,
        use_browser: bool = False,
        results_buffer: Optional[List[Dict[str, Any]]] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.target_urls = urls
        self.tracker = tracker
        self.use_browser = use_browser
        self.results_buffer = results_buffer if results_buffer is not None else []
        self._lock = threading.Lock()

    def start_requests(self):
        for raw_url in self.target_urls:
            pdp_url = normalize_pdp_url(raw_url)
            headers = {
                "User-Agent": random.choice(DEFAULT_USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.wayfair.com/",
                "sec-ch-ua": '"Chromium";v="123", "Not:A-Brand";v="8"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "same-origin",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
            }

            meta = {
                "raw_url": raw_url,
                "pdp_url": pdp_url,
                "dont_redirect": False,
                "handle_httpstatus_list": [403, 404, 429, 500, 502, 503, 504]
            }

            if self.use_browser:
                page_methods = []
                m1 = get_playwright_page_method("wait_for_load_state", "domcontentloaded")
                if m1: page_methods.append(m1)
                m2 = get_playwright_page_method("evaluate", "window.scrollBy(0, 1000)")
                if m2: page_methods.append(m2)
                m3 = get_playwright_page_method("wait_for_timeout", 1500)
                if m3: page_methods.append(m3)
                m4 = get_playwright_page_method("evaluate", "window.scrollBy(0, 1500)")
                if m4: page_methods.append(m4)
                m5 = get_playwright_page_method("wait_for_timeout", 1500)
                if m5: page_methods.append(m5)

                meta.update({
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": page_methods
                })

            yield scrapy.Request(
                url=pdp_url,
                headers=headers,
                meta=meta,
                callback=self.parse_product,
                errback=self.handle_error,
                dont_filter=True
            )

    async def parse_product(self, response):
        raw_url = response.meta.get("raw_url")
        pdp_url = response.meta.get("pdp_url")

        # Close Playwright page if present
        page = response.meta.get("playwright_page")
        html_content = response.text
        if page:
            try:
                html_content = await page.content()
                await page.close()
            except Exception:
                pass

        if response.status not in [200, 301, 302, 304]:
            logger.warning(f"HTTP {response.status} on {pdp_url}. Scrape skipped.")
            return

        # Parse HTML & Next.js Hydration data
        parser = WayfairParser(html_content, provided_url=raw_url, page_url=pdp_url)
        item = parser.parse()

        if not item.get("name") and not item.get("sku"):
            logger.warning(f"Extracted page for {raw_url} but neither name nor SKU could be parsed.")
            return

        with self._lock:
            # Check SKU deduplication against existing database
            sku = item.get("sku")
            if sku and sku in self.tracker.skus:
                logger.info(f"Skipping duplicate product SKU {sku} ({raw_url})")
                return

            self.tracker.mark_traversed(raw_url, item)
            self.results_buffer.append(item)
            logger.info(f"[+] Scraped [{len(self.results_buffer)}]: {item.get('sku')} | {item.get('name')[:40]} | Price: {item.get('price', {}).get('currentPrice')}")

    def handle_error(self, failure):
        raw_url = failure.request.meta.get("raw_url")
        logger.error(f"Request failed for {raw_url}: {failure.value}")


# ============================================================================
# 6. ATOMIC SAVE & MERGE UTILITIES
# ============================================================================

def save_products_to_json(
    filepath: str,
    new_records: List[Dict[str, Any]],
    existing_records: Optional[List[Dict[str, Any]]] = None,
    overwrite: bool = False
):
    """
    Saves product records safely to JSON with complete deduplication.
    """
    combined: List[Dict[str, Any]] = []
    seen_identifiers = set()

    def get_key(d: Dict[str, Any]):
        sku = d.get("sku") or d.get("SKU")
        if sku and isinstance(sku, str) and sku.strip():
            return ("sku", sku.strip().upper())
        for k in ["providedUrl", "pageUrl", "url"]:
            u = d.get(k)
            if u and isinstance(u, str) and u.strip():
                norm = normalize_pdp_url(u.strip())
                if norm:
                    return ("url", norm)
        return ("obj", json.dumps(d, sort_keys=True))

    # 1. Existing database records (if appending)
    if not overwrite and existing_records:
        for it in existing_records:
            k = get_key(it)
            if k not in seen_identifiers:
                seen_identifiers.add(k)
                combined.append(it)

    # 2. Add new scraped records
    added_new = 0
    for it in new_records:
        k = get_key(it)
        if k not in seen_identifiers:
            seen_identifiers.add(k)
            combined.append(it)
            added_new += 1

    temp_file = f"{filepath}.tmp"
    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(combined, f, indent=2, ensure_ascii=False)

    os.replace(temp_file, filepath)
    logger.info(f"Successfully saved {len(combined)} total records ({added_new} newly added) to '{filepath}'")


def merge_shards_directory(merge_dir: str, output_file: str, base_file: Optional[str] = None):
    """
    Discovers all shard JSON files in merge_dir and consolidates them into output_file
    while preserving deduplication against existing database records.
    """
    logger.info(f"Merging shard files from directory: '{merge_dir}' -> '{output_file}'")
    tracker = TraversedTracker()

    if base_file and os.path.exists(base_file):
        tracker.load_from_file(base_file)
    elif os.path.exists(output_file):
        tracker.load_from_file(output_file)

    collected_records: List[Dict[str, Any]] = []
    for root, _, files in os.walk(merge_dir):
        for f in sorted(files):
            if f.endswith(".json") and f != os.path.basename(output_file):
                fpath = os.path.join(root, f)
                try:
                    with open(fpath, "r", encoding="utf-8") as jf:
                        content = jf.read().strip()
                        if not content:
                            continue
                        data = json.loads(content)
                        records = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
                        logger.info(f"Loaded {len(records)} record(s) from shard: '{fpath}'")
                        collected_records.extend(records)
                except Exception as e:
                    logger.warning(f"Error reading shard '{fpath}': {e}")

    save_products_to_json(
        filepath=output_file,
        new_records=collected_records,
        existing_records=tracker.existing_records,
        overwrite=False
    )


# ============================================================================
# 7. MAIN RUNNER & CLI INTERFACE
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Wayfair Scrapy Scraper with Rotating Proxies and Traversed URL Deduplication."
    )
    parser.add_argument("-i", "--input", default="urls.txt", help="Input file with product URLs (default: urls.txt)")
    parser.add_argument("-o", "--output", default="wayfair_products.json", help="Output JSON file (default: wayfair_products.json)")
    parser.add_argument("-u", "--url", action="append", help="Single/multiple product URL(s) to scrape")
    parser.add_argument("--ignore-files", nargs="*", default=["wayfair_products.json"],
                        help="JSON file(s) containing already scraped products to avoid duplicating (default: wayfair_products.json)")
    
    # Proxy options
    parser.add_argument("--proxy", help="Single proxy URL or comma-separated list (e.g. http://user:pass@host:port)")
    parser.add_argument("--proxy-file", help="Path to text file containing proxy list (default: proxies.txt if exists)")
    
    # Engine & Playwright options
    parser.add_argument("--use-browser", action="store_true", help="Enable Playwright browser for dynamic JS rendering & lazy-load scrolling")
    parser.add_argument("--concurrency", type=int, default=4, help="Concurrent requests per runner (default: 4)")
    parser.add_argument("--download-delay", type=float, default=0.5, help="Download delay in seconds between requests (default: 0.5)")
    parser.add_argument("--max-urls", type=int, default=0, help="Max URLs to scrape (0 for all)")
    
    # Parallel Sharding & Merging options
    parser.add_argument("--shard-index", type=int, default=0, help="0-based index of this shard runner")
    parser.add_argument("--shard-total", type=int, default=1, help="Total number of parallel runner shards")
    parser.add_argument("--merge-dir", help="Directory containing shard JSON files to merge into --output")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output file rather than appending")

    args = parser.parse_args()

    # 1. Check Merge Mode
    if args.merge_dir:
        merge_shards_directory(args.merge_dir, args.output, base_file="wayfair_products.json" if not args.overwrite else None)
        return

    # 2. Initialize Traversed Tracker and Load Existing Database (12k+ items)
    tracker = TraversedTracker()
    files_to_check = []
    if args.ignore_files:
        for f in args.ignore_files:
            if f and os.path.exists(f) and f not in files_to_check:
                files_to_check.append(f)

    if os.path.exists("wayfair_products.json") and "wayfair_products.json" not in files_to_check:
        files_to_check.append("wayfair_products.json")

    for fpath in files_to_check:
        tracker.load_from_file(fpath)

    # 3. Collect Candidate URLs
    candidate_urls: List[str] = []
    if args.url:
        candidate_urls.extend(args.url)
    elif os.path.exists(args.input):
        with open(args.input, "r", encoding="utf-8") as f:
            candidate_urls.extend([line.strip() for line in f if line.strip() and not line.strip().startswith("#")])
    else:
        candidate_urls = [
            "https://www.wayfair.com/ACME-Furniture--ACME-Ireland-Dresser-ACM-L13-K~CMU19772.html?PiID%5B%5D=119976344"
        ]
        logger.info("No input file found; using default sample URL.")

    if not candidate_urls:
        logger.error("No valid candidate URLs to scrape.")
        sys.exit(1)

    logger.info(f"Total candidate URLs loaded: {len(candidate_urls)}")

    # 4. Pre-filter candidate URLs against already traversed products (Shield the 12k+)
    fresh_urls, skipped_count = tracker.filter_urls(candidate_urls)
    logger.info(f"Deduplication: {skipped_count} URL(s) already scraped/duplicate in database. {len(fresh_urls)} fresh URLs remaining.")

    if not fresh_urls:
        logger.info("All URLs have already been scraped! No work remaining for this run.")
        return

    # 5. Apply Parallel Sharding
    if args.shard_total > 1:
        total_before = len(fresh_urls)
        fresh_urls = [url for idx, url in enumerate(fresh_urls) if idx % args.shard_total == args.shard_index]
        logger.info(f"Sharding enabled: Shard [{args.shard_index + 1}/{args.shard_total}] assigned {len(fresh_urls)} of {total_before} fresh URLs.")

    if args.max_urls > 0:
        fresh_urls = fresh_urls[:args.max_urls]
        logger.info(f"Limited to first {len(fresh_urls)} fresh URLs by --max-urls flag.")

    # 6. Load & Configure Proxies
    proxies = load_proxies(cli_proxy=args.proxy, proxy_file=args.proxy_file)
    logger.info(f"Proxies configured: {len(proxies)} proxy server(s) active.")

    # 7. Configure Scrapy Settings
    settings_dict = {
        "LOG_LEVEL": "INFO",
        "CONCURRENT_REQUESTS": args.concurrency,
        "CONCURRENT_REQUESTS_PER_DOMAIN": args.concurrency,
        "DOWNLOAD_DELAY": args.download_delay,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "COOKIES_ENABLED": True,
        "RETRY_TIMES": 4,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 429, 403],
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
            "__main__.RotatingProxyMiddleware": 750,
        },
        "ROTATING_PROXY_LIST": proxies,
        "ROTATING_PROXY_COOLDOWN": 60.0,
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    }

    if args.use_browser:
        settings_dict.update({
            "DOWNLOAD_HANDLERS": {
                "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
                "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            },
            "PLAYWRIGHT_BROWSER_TYPE": "chromium",
            "PLAYWRIGHT_LAUNCH_OPTIONS": {
                "headless": True,
                "args": ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
            },
            "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 60000,
        })

    # 8. Run Scrapy Crawler
    results_buffer: List[Dict[str, Any]] = []
    process = CrawlerProcess(settings=settings_dict)
    process.crawl(
        WayfairProductSpider,
        urls=fresh_urls,
        tracker=tracker,
        use_browser=args.use_browser,
        results_buffer=results_buffer
    )

    logger.info("Starting Scrapy crawler...")
    process.start()
    logger.info(f"Crawling complete. Total new items scraped in this session: {len(results_buffer)}")

    # 9. Save output
    is_existing_output = os.path.exists(args.output) and (os.path.abspath(args.output) in [os.path.abspath(f) for f in files_to_check])
    append_existing = is_existing_output and (not args.overwrite)

    save_products_to_json(
        filepath=args.output,
        new_records=results_buffer,
        existing_records=tracker.existing_records if append_existing else None,
        overwrite=args.overwrite
    )


if __name__ == "__main__":
    main()
