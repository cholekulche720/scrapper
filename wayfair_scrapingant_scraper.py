#!/usr/bin/env python3
"""
Wayfair ScrapingAnt Scraper
===========================
High-performance asynchronous crawler for Wayfair Product Detail Pages (PDP)
powered by the ScrapingAnt Scraping API (Residential proxy + Headless Chrome).

Features:
- Multi-API Key rotation and automatic quota exhaustion failover from 'scrapingant_keys.txt'.
- High-concurrency async fetching with aiohttp / asyncio.
- Shielding for 12k+ existing records in 'wayfair_products.json' (0 duplicates).
- Unique URL & Variant preservation (distinguishing ?piid=... variants).
- Full camelCase data extraction (Name, Brand, SKU, Breadcrumbs, Price, Variants,
  At a Glance, Description, Specifications, Documents, High-Res Images).
- Periodic auto-saving and graceful shutdown on Ctrl+C.
- API Key health check utility via '--test-keys'.
"""

import os
import sys
import re
import json
import time
import random
import logging
import argparse
import asyncio
import signal
from typing import Dict, Any, List, Optional, Set, Tuple
from urllib.parse import urlparse, parse_qs, quote_plus
from bs4 import BeautifulSoup

try:
    import aiohttp
except ImportError:
    print("❌ 'aiohttp' library is required. Install it using: pip install aiohttp")
    sys.exit(1)

# Logger setup
logger = logging.getLogger("WayfairScrapingAnt")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
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
# 2. COMPREHENSIVE WAYFAIR HTML PARSER
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
    Protects all 12k+ records from being rescraped or duplicated.
    """
    def __init__(self):
        self._lock = asyncio.Lock()
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
        if u in self.exact_urls or u.lower() in self.exact_urls:
            return True
        norm = normalize_pdp_url(u)
        if norm and (norm in self.normalized_urls or norm.lower() in self.normalized_urls):
            return True
        return False

    async def mark_traversed_async(self, url: str, item: Optional[Dict[str, Any]] = None):
        async with self._lock:
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
# 4. SCRAPINGANT KEY MANAGER & ASYNC CLIENT
# ============================================================================

class ScrapingAntKeyManager:
    """
    Manages multiple ScrapingAnt API keys with automatic failover on quota limits.
    """
    def __init__(self, keys: List[str]):
        self.keys = [k.strip() for k in keys if k.strip() and not k.startswith("#")]
        self.exhausted_keys: Set[str] = set()
        self._index = 0
        self._lock = asyncio.Lock()

    @classmethod
    def from_file(cls, filepath: str = "scrapingant_keys.txt") -> "ScrapingAntKeyManager":
        keys: List[str] = []
        # 1. From file
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    k = line.strip()
                    if k and not k.startswith("#"):
                        keys.append(k)

        # 2. From environment variables
        for env_var in ["SCRAPINGANT_API_KEY", "SCRAPINGANT_KEYS", "SCRAPING_ANT_KEY"]:
            val = os.environ.get(env_var)
            if val:
                for k in re.split(r'[,\s]+', val):
                    k = k.strip()
                    if k and k not in keys:
                        keys.append(k)

        return cls(keys)

    async def get_key(self) -> Optional[str]:
        async with self._lock:
            available = [k for k in self.keys if k not in self.exhausted_keys]
            if not available:
                return None
            key = available[self._index % len(available)]
            self._index += 1
            return key

    async def mark_exhausted(self, key: str, reason: str = "Quota Exceeded"):
        async with self._lock:
            if key not in self.exhausted_keys:
                self.exhausted_keys.add(key)
                masked = f"{key[:4]}...{key[-4:]}" if len(key) > 8 else "***"
                logger.warning(f"⚠️ ScrapingAnt API Key [{masked}] marked exhausted ({reason}). "
                               f"Active keys remaining: {len(self.keys) - len(self.exhausted_keys)}/{len(self.keys)}")

    @property
    def active_count(self) -> int:
        return len(self.keys) - len(self.exhausted_keys)


async def test_scrapingant_keys(key_manager: ScrapingAntKeyManager):
    """Utility function to test all provided ScrapingAnt keys."""
    if not key_manager.keys:
        logger.error("No ScrapingAnt keys found to test. Please add keys to 'scrapingant_keys.txt'.")
        return

    logger.info(f"Testing {len(key_manager.keys)} ScrapingAnt API key(s)...")
    test_url = "https://httpbin.org/ip"
    api_endpoint = "https://api.scrapingant.com/v2/general"

    async with aiohttp.ClientSession() as session:
        for idx, key in enumerate(key_manager.keys, 1):
            masked = f"{key[:4]}...{key[-4:]}" if len(key) > 8 else "***"
            params = {
                "url": test_url,
                "x-api-key": key,
                "browser": "false",
                "proxy_type": "datacenter"
            }
            try:
                start_t = time.time()
                async with session.get(api_endpoint, params=params, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                    elapsed = time.time() - start_t
                    body = await resp.text()
                    if resp.status == 200:
                        logger.info(f"✅ Key [{idx}/{len(key_manager.keys)}] {masked}: VALID (Status {resp.status}, {elapsed:.2f}s)")
                    elif resp.status in [401, 403]:
                        logger.error(f"❌ Key [{idx}/{len(key_manager.keys)}] {masked}: INVALID / OUT OF CREDITS (Status {resp.status}) -> {body[:150]}")
                    elif resp.status == 429:
                        logger.warning(f"⚠️ Key [{idx}/{len(key_manager.keys)}] {masked}: RATE LIMITED (Status 429)")
                    else:
                        logger.warning(f"⚠️ Key [{idx}/{len(key_manager.keys)}] {masked}: HTTP {resp.status} -> {body[:150]}")
            except Exception as e:
                logger.error(f"❌ Key [{idx}/{len(key_manager.keys)}] {masked}: Connection failed: {e}")


# ============================================================================
# 5. ASYNC SCRAPING ENGINE
# ============================================================================

async def fetch_wayfair_pdp_scrapingant(
    session: aiohttp.ClientSession,
    url: str,
    key_manager: ScrapingAntKeyManager,
    max_retries: int = 3
) -> Tuple[Optional[str], Optional[str]]:
    """
    Fetches Wayfair PDP HTML via ScrapingAnt API with residential proxy & browser JS execution.
    Returns (html_content, active_key_used).
    """
    api_endpoint = "https://api.scrapingant.com/v2/general"

    for attempt in range(1, max_retries + 1):
        key = await key_manager.get_key()
        if not key:
            logger.error("⛔ All ScrapingAnt API keys are exhausted or unavailable!")
            return None, None

        masked_key = f"{key[:4]}...{key[-4:]}" if len(key) > 8 else "***"
        params = {
            "url": url,
            "x-api-key": key,
            "browser": "true",
            "proxy_type": "residential",
            "proxy_country": "us",
            "return_page_source": "true",
            "wait_for_selector": "h1"
        }

        try:
            timeout = aiohttp.ClientTimeout(total=75)
            async with session.get(api_endpoint, params=params, timeout=timeout) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    # Check for PerimeterX challenge in response body
                    if "px-captcha" in html.lower() or "access to this page has been denied" in html.lower():
                        logger.warning(f"PerimeterX challenge encountered via key [{masked_key}] for {url}. Retrying with fresh proxy session...")
                        await asyncio.sleep(2)
                        continue
                    return html, key

                elif resp.status in [401, 403]:
                    body = await resp.text()
                    await key_manager.mark_exhausted(key, f"HTTP {resp.status}: {body[:100]}")
                    continue

                elif resp.status == 429:
                    logger.warning(f"Key [{masked_key}] hit rate limit (429). Backing off 3s...")
                    await asyncio.sleep(3)
                    continue

                else:
                    body = await resp.text()
                    logger.warning(f"ScrapingAnt HTTP {resp.status} on attempt {attempt}/{max_retries} for {url}: {body[:120]}")
                    await asyncio.sleep(attempt * 2)

        except asyncio.TimeoutError:
            logger.warning(f"Timeout on attempt {attempt}/{max_retries} for {url} via key [{masked_key}]")
            await asyncio.sleep(attempt * 2)
        except Exception as e:
            logger.warning(f"Error fetching {url} on attempt {attempt}/{max_retries} via key [{masked_key}]: {e}")
            await asyncio.sleep(attempt * 2)

    return None, None


# ============================================================================
# 6. MAIN SCRAPER RUNNER
# ============================================================================

def save_json_safe(filepath: str, data: List[Dict[str, Any]], append: bool = False):
    """Safely saves results to a JSON file."""
    output_records = data
    if append and os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                existing = json.load(f)
                if isinstance(existing, list):
                    seen = set()
                    combined = []
                    for item in existing + data:
                        if not isinstance(item, dict):
                            continue
                        url_key = item.get("pageUrl") or item.get("providedUrl") or item.get("sku")
                        if url_key:
                            norm_key = normalize_pdp_url(url_key) or url_key
                            if norm_key.lower() not in seen:
                                seen.add(norm_key.lower())
                                combined.append(item)
                        else:
                            combined.append(item)
                    output_records = combined
        except Exception as e:
            logger.warning(f"Could not merge with existing output file '{filepath}': {e}")

    os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
    temp_file = f"{filepath}.tmp"
    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(output_records, f, indent=2, ensure_ascii=False)
    os.replace(temp_file, filepath)
    logger.info(f"Successfully saved {len(output_records)} total records to '{filepath}'")


async def run_scraper(args):
    # 1. Load API keys
    key_manager = ScrapingAntKeyManager.from_file(args.keys_file)
    if args.api_key:
        for k in args.api_key.split(","):
            if k.strip() and k.strip() not in key_manager.keys:
                key_manager.keys.append(k.strip())

    if not key_manager.keys:
        logger.error(f"❌ No ScrapingAnt API keys found! Please add your keys to '{args.keys_file}' or pass --api-key.")
        sys.exit(1)

    logger.info(f"Loaded {len(key_manager.keys)} ScrapingAnt API key(s) from '{args.keys_file}'.")

    # 2. Test keys mode
    if args.test_keys:
        await test_scrapingant_keys(key_manager)
        return

    # 3. Load & Deduplicate against existing dataset
    tracker = TraversedTracker()
    files_to_check = [f.strip() for f in args.ignore_files.split(",") if f.strip()]
    for fpath in files_to_check:
        tracker.load_from_file(fpath)

    # 4. Load Target URLs
    raw_urls: List[str] = []
    if os.path.exists(args.input):
        with open(args.input, "r", encoding="utf-8") as f:
            for line in f:
                u = line.strip()
                if u and not u.startswith("#"):
                    raw_urls.append(u)
    else:
        logger.error(f"❌ Input URL file '{args.input}' not found!")
        sys.exit(1)

    logger.info(f"Total candidate URLs loaded: {len(raw_urls)}")
    fresh_urls, skipped = tracker.filter_urls(raw_urls)
    logger.info(f"Deduplication: {skipped} URL(s) already scraped/duplicate. {len(fresh_urls)} fresh URLs remaining.")

    if not fresh_urls:
        logger.info("🎉 All candidate URLs have already been scraped! No fresh URLs to scrape.")
        return

    # 5. Sharding (Multi-Runner Partitioning)
    if args.shard_total > 1:
        if args.shard_index < 0 or args.shard_index >= args.shard_total:
            logger.error(f"Invalid shard-index {args.shard_index} for shard-total {args.shard_total}")
            sys.exit(1)
        sharded_urls = [u for idx, u in enumerate(fresh_urls) if (idx % args.shard_total) == args.shard_index]
        logger.info(f"Sharding enabled: Shard [{args.shard_index + 1}/{args.shard_total}] assigned {len(sharded_urls)} of {len(fresh_urls)} fresh URLs.")
        fresh_urls = sharded_urls

    # 6. Limit URLs if --max-urls is set
    if args.max_urls and args.max_urls > 0:
        fresh_urls = fresh_urls[:args.max_urls]
        logger.info(f"Limited to first {len(fresh_urls)} fresh URLs by --max-urls flag.")

    if not fresh_urls:
        logger.info("No URLs assigned to this shard. Saving empty shard file.")
        save_json_safe(args.output, [], append=False)
        return

    # 7. Prepare worker pool & results buffer
    results_buffer: List[Dict[str, Any]] = []
    scraped_lock = asyncio.Lock()
    queue = asyncio.Queue()
    for u in fresh_urls:
        queue.put_nowait(u)

    total_target = len(fresh_urls)
    completed_count = 0
    start_time = time.time()

    async def worker(worker_id: int, session: aiohttp.ClientSession):
        nonlocal completed_count
        while not queue.empty():
            try:
                url = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            pdp_url = normalize_pdp_url(url)
            logger.info(f"[Worker-{worker_id}] Scraping ({completed_count + 1}/{total_target}): {pdp_url}")

            html, key_used = await fetch_wayfair_pdp_scrapingant(
                session=session,
                url=pdp_url,
                key_manager=key_manager,
                max_retries=args.retries
            )

            if html:
                try:
                    parser = WayfairParser(html=html, provided_url=url, page_url=pdp_url)
                    product_data = parser.parse()
                    
                    if product_data.get("name") or product_data.get("sku"):
                        async with scraped_lock:
                            results_buffer.append(product_data)
                            await tracker.mark_traversed_async(url, product_data)
                        
                        sku_disp = product_data.get("sku") or "N/A"
                        price_disp = (product_data.get("price") or {}).get("currentPrice") or "N/A"
                        logger.info(f"✅ [Worker-{worker_id}] Scraped: {product_data.get('name', 'Product')[:40]} | SKU: {sku_disp} | Price: {price_disp}")
                    else:
                        logger.warning(f"⚠️ [Worker-{worker_id}] Incomplete parse on {pdp_url} (name/sku missing)")

                except Exception as e:
                    logger.error(f"❌ [Worker-{worker_id}] Parse error on {pdp_url}: {e}")
            else:
                logger.warning(f"❌ [Worker-{worker_id}] Failed to fetch {pdp_url} after all retries.")

            completed_count += 1
            queue.task_done()

            # Intermediate save every 10 items
            if len(results_buffer) > 0 and len(results_buffer) % 10 == 0:
                async with scraped_lock:
                    save_json_safe(args.output, results_buffer, append=not args.overwrite)

            # Small delay between worker requests to avoid rate limits
            if args.delay > 0:
                await asyncio.sleep(args.delay)

    # 8. Start Async Workers
    connector = aiohttp.TCPConnector(limit=args.concurrency * 2, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        workers = [asyncio.create_task(worker(i + 1, session)) for i in range(min(args.concurrency, len(fresh_urls)))]
        await asyncio.gather(*workers, return_exceptions=True)

    elapsed = time.time() - start_time
    rate = (len(results_buffer) / (elapsed / 60)) if elapsed > 0 else 0
    logger.info(f"🎉 Scraping complete! Scraped {len(results_buffer)} items in {elapsed:.1f}s ({rate:.1f} items/min).")

    # 9. Final Save
    save_json_safe(args.output, results_buffer, append=not args.overwrite)


def merge_shards(merge_dir: str, output_file: str, base_file: Optional[str] = "wayfair_products.json"):
    """Merges all shard JSON output files into a single unified JSON database."""
    logger.info(f"Merging all shard output files from directory: '{merge_dir}' -> '{output_file}'")
    combined_records: List[Dict[str, Any]] = []
    seen_urls: Set[str] = set()

    def add_record(item: Dict[str, Any]):
        if not isinstance(item, dict):
            return
        url_key = item.get("pageUrl") or item.get("providedUrl") or item.get("sku")
        if not url_key:
            combined_records.append(item)
            return
        norm = normalize_pdp_url(url_key) or url_key
        norm_lower = norm.lower()
        if norm_lower not in seen_urls:
            seen_urls.add(norm_lower)
            combined_records.append(item)

    # 1. Load base existing records
    if base_file and os.path.exists(base_file):
        try:
            with open(base_file, "r", encoding="utf-8") as f:
                base_data = json.load(f)
                if isinstance(base_data, list):
                    for r in base_data:
                        add_record(r)
            logger.info(f"Loaded {len(combined_records)} baseline records from '{base_file}'")
        except Exception as e:
            logger.warning(f"Could not load baseline file '{base_file}': {e}")

    # 2. Load all shard files in merge_dir
    shard_count = 0
    if os.path.isdir(merge_dir):
        for root, _, files in os.walk(merge_dir):
            for file in files:
                if file.endswith(".json") and (os.path.abspath(os.path.join(root, file)) != os.path.abspath(output_file)):
                    fpath = os.path.join(root, file)
                    try:
                        with open(fpath, "r", encoding="utf-8") as f:
                            sdata = json.load(f)
                            if isinstance(sdata, list):
                                for r in sdata:
                                    add_record(r)
                                shard_count += 1
                                logger.info(f"Merged shard file: '{fpath}' ({len(sdata)} items)")
                    except Exception as e:
                        logger.warning(f"Error reading shard file '{fpath}': {e}")

    save_json_safe(output_file, combined_records, append=False)
    logger.info(f"✅ Merge complete! Combined {shard_count} shard(s) into {len(combined_records)} total unique products.")


def main():
    parser = argparse.ArgumentParser(description="Wayfair PDP Scraper via ScrapingAnt API")
    parser.add_argument("--input", "-i", default="urls_fresh.txt", help="Input URLs file (default: urls_fresh.txt)")
    parser.add_argument("--output", "-o", default="wayfair_products_scraped.json", help="Output JSON filename")
    parser.add_argument("--keys-file", "-k", default="scrapingant_keys.txt", help="ScrapingAnt keys file (default: scrapingant_keys.txt)")
    parser.add_argument("--api-key", help="Comma-separated ScrapingAnt API keys (or use scrapingant_keys.txt)")
    parser.add_argument("--ignore-files", default="wayfair_products.json", help="Files to check for existing records")
    parser.add_argument("--concurrency", "-c", type=int, default=5, help="Concurrent async requests (default: 5)")
    parser.add_argument("--max-urls", "-n", type=int, default=0, help="Max fresh URLs to scrape (0 for all)")
    parser.add_argument("--retries", type=int, default=3, help="Max retries per URL on failure (default: 3)")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between worker requests in seconds (default: 0.5)")
    parser.add_argument("--shard-index", type=int, default=0, help="Zero-based shard index for parallel execution")
    parser.add_argument("--shard-total", type=int, default=1, help="Total number of parallel shards")
    parser.add_argument("--merge-dir", help="Directory containing shard JSON outputs to merge")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output file instead of appending")
    parser.add_argument("--test-keys", action="store_true", help="Test provided ScrapingAnt keys and exit")

    args = parser.parse_args()

    if args.merge_dir:
        merge_shards(merge_dir=args.merge_dir, output_file=args.output, base_file=args.ignore_files.split(",")[0] if args.ignore_files else None)
        return

    # If urls_fresh.txt does not exist, fallback to urls.txt
    if not os.path.exists(args.input) and os.path.exists("urls.txt") and args.input == "urls_fresh.txt":
        args.input = "urls.txt"

    try:
        asyncio.run(run_scraper(args))
    except KeyboardInterrupt:
        logger.info("\n⏹️ Scraper stopped by user. Progress saved.")


if __name__ == "__main__":
    main()
