#!/usr/bin/env python3
"""
Wayfair PDP Multithreaded Scraper
Extracts comprehensive multi-level JSON product data using Zyte API.
Designed for both local execution and GitHub Actions.
"""

import os
import sys
import subprocess

# Auto-install dependencies if missing before importing them
def ensure_dependencies():
    required = ["requests", "bs4", "urllib3"]
    missing = []
    for mod in required:
        try:
            __import__(mod)
        except ImportError:
            missing.append(mod)
    if missing:
        print(f"[WayfairScraper] Missing dependencies detected: {missing}. Automatically installing requirements...")
        req_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "requirements.txt")
        if os.path.exists(req_file):
            cmd = [sys.executable, "-m", "pip", "install", "-r", req_file]
        else:
            cmd = [sys.executable, "-m", "pip", "install", "requests>=2.31.0", "beautifulsoup4>=4.12.0", "urllib3>=2.0.0"]
        try:
            subprocess.check_call(cmd)
        except subprocess.CalledProcessError:
            print("[WayfairScraper] Retrying installation with --user flag...")
            subprocess.check_call(cmd + ["--user"])
        print("[WayfairScraper] All dependencies installed successfully.\n")

ensure_dependencies()

import re
import json
import base64
import logging
import argparse
import time
import threading
from typing import Dict, Any, List, Optional, Set, Tuple
from urllib.parse import urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("WayfairScraper")

DEFAULT_ZYTE_KEY = "a047fc55b62d47be8d44157df9c78c4a"
ZYTE_API_EXTRACT_ENDPOINT = "https://api.zyte.com/v1/extract"


def mask_key(key: str) -> str:
    """Masks an API key for safe logging (e.g., 'a047...8c4a')."""
    if not key:
        return "<EMPTY>"
    key = str(key).strip()
    if len(key) <= 8:
        return "****"
    return f"{key[:4]}...{key[-4:]}"


def create_session() -> requests.Session:
    """Creates a requests session configured with retries for robust scraping."""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1.5,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["POST", "GET"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


class ZyteKeyResolver:
    """
    Resolves the active Zyte API key.
    If provided with a Scrapinghub Master/Account Key, it automatically interacts
    with the Zyte Management API to fetch the active organization Zyte API token.
    Caches resolved keys per input key in memory.
    """
    _cache: Dict[str, str] = {}
    _lock = threading.Lock()

    @classmethod
    def resolve_key(cls, user_key: str) -> str:
        key = user_key.strip()
        if not key:
            return ""

        with cls._lock:
            if key in cls._cache:
                return cls._cache[key]

        masked = mask_key(key)

        # Check if the key works directly on the Zyte API extract endpoint
        try:
            test_resp = requests.post(
                ZYTE_API_EXTRACT_ENDPOINT,
                auth=(key, ""),
                json={"url": "https://httpbin.org/ip", "httpResponseBody": True},
                timeout=8
            )
            if test_resp.status_code == 200:
                logger.info(f"Direct Zyte API key [{masked}] validated successfully.")
                with cls._lock:
                    cls._cache[key] = key
                return key
        except Exception:
            pass

        # If not, resolve it via Scrapinghub / Zyte management API
        logger.info(f"Attempting to resolve Zyte service credentials for [{masked}] via Zyte Account API...")
        try:
            token = base64.b64encode(f"{key}:".encode()).decode()
            headers = {"Authorization": f"Basic {token}"}
            
            # Fetch organization
            org_res = requests.get("https://app.zyte.com/api/v2/organizations", headers=headers, timeout=10)
            if org_res.status_code == 200:
                results = org_res.json().get("results", [])
                if results:
                    org_id = results[0].get("id")
                    services_res = requests.get(
                        f"https://app.zyte.com/api/v2/zyteapi?organization={org_id}",
                        headers=headers,
                        timeout=10
                    )
                    if services_res.status_code == 200:
                        s_results = services_res.json().get("results", [])
                        for service in s_results:
                            sid = service.get("id")
                            cred_res = requests.get(
                                f"https://app.zyte.com/api/v2/zyteapi/{sid}/credentials?organization={org_id}",
                                headers=headers,
                                timeout=10
                            )
                            if cred_res.status_code == 200:
                                apikey = cred_res.json().get("apikey")
                                if apikey:
                                    logger.info(f"Resolved Zyte API Key for '{service.get('name')}' (Service ID: {sid}): [{mask_key(apikey)}]")
                                    with cls._lock:
                                        cls._cache[key] = apikey
                                    return apikey
        except Exception as e:
            logger.warning(f"Could not auto-resolve Zyte key [{masked}] via management API: {e}")

        with cls._lock:
            cls._cache[key] = key
        return key


class ZyteKeyPool:
    """
    Thread-safe pool managing multiple Zyte API keys.
    Provides round-robin rotation, automatic failover when a key hits rate limits (429)
    or quota exhaustion / invalid credentials (401, 402, 403), and recovery tracking.
    """
    def __init__(self, raw_keys: List[str]):
        self._lock = threading.Lock()
        self._keys: List[str] = []
        self._inactive_keys: Set[str] = set()
        self._rate_limited: Dict[str, float] = {}  # key -> cooldown end timestamp
        self._index: int = 0

        # Resolve all provided keys
        seen = set()
        for rk in raw_keys:
            rk = rk.strip()
            if not rk or rk.startswith("#"):
                continue
            resolved = ZyteKeyResolver.resolve_key(rk)
            if resolved and resolved not in seen:
                seen.add(resolved)
                self._keys.append(resolved)

        if not self._keys:
            resolved_default = ZyteKeyResolver.resolve_key(DEFAULT_ZYTE_KEY)
            self._keys = [resolved_default]

        logger.info(
            f"ZyteKeyPool initialized with {len(self._keys)} active key(s): "
            f"{[mask_key(k) for k in self._keys]}"
        )

    def get_next_key(self) -> Optional[str]:
        """
        Retrieves the next active key using thread-safe round-robin allocation.
        Skips inactive (exhausted) and currently rate-limited keys if alternatives exist.
        """
        with self._lock:
            if not self._keys:
                return None

            now = time.time()
            total = len(self._keys)

            # Check if any rate-limited keys have completed cooldown
            for k in list(self._rate_limited.keys()):
                if now >= self._rate_limited[k]:
                    del self._rate_limited[k]

            # 1. Search for an available active key that is not rate-limited
            for _ in range(total):
                key = self._keys[self._index % total]
                self._index += 1
                if key not in self._inactive_keys and key not in self._rate_limited:
                    return key

            # 2. If all active keys are currently rate-limited, pick the one with earliest cooldown expiry
            active_keys = [k for k in self._keys if k not in self._inactive_keys]
            if not active_keys:
                return None  # All keys permanently exhausted

            # Find key with earliest cooldown
            best_key = min(active_keys, key=lambda k: self._rate_limited.get(k, 0))
            wait_time = max(0.0, self._rate_limited.get(best_key, now) - now)
            if wait_time > 0 and wait_time <= 10.0:
                time.sleep(wait_time)
                self._rate_limited.pop(best_key, None)
            return best_key

    def mark_key_status(self, key: str, status_code: int):
        """
        Updates the health status of a key based on HTTP response.
        401 / 402 / 403: Permanent auth failure or out of credits -> Inactive.
        429: Temporary rate limit -> 30s cooldown.
        """
        with self._lock:
            masked = mask_key(key)
            if status_code in (401, 402, 403):
                if key not in self._inactive_keys:
                    self._inactive_keys.add(key)
                    remaining = len(self._keys) - len(self._inactive_keys)
                    logger.warning(
                        f"Zyte API key [{masked}] marked INACTIVE/EXHAUSTED (HTTP {status_code}). "
                        f"{remaining} active key(s) remaining in pool."
                    )
            elif status_code == 429:
                self._rate_limited[key] = time.time() + 30.0
                logger.warning(
                    f"Zyte API key [{masked}] marked rate-limited (HTTP 429). "
                    f"Cooldown for 30s. Other keys will take over."
                )

    def get_active_count(self) -> int:
        with self._lock:
            return max(0, len(self._keys) - len(self._inactive_keys))

    def get_total_count(self) -> int:
        return len(self._keys)


def normalize_pdp_url(url: str, shop_product_type: str = "furniture") -> str:
    """
    Transforms any Wayfair product URL (legacy, affiliate, category-level)
    into the canonical PDP URL format:
    https://www.wayfair.com/<shop-product-type>/pdp/<slug>.html?piid=<piid>
    
    Rules applied:
    1. Replaces special characters with hyphen (-).
    2. Collapses multiple hyphens into a single hyphen.
    3. Retains piid query parameter if present and removes tracking clutter.
    """
    url = url.strip()
    if not url:
        return ""
    
    parsed = urlparse(url)
    domain = f"{parsed.scheme}://{parsed.netloc}" if parsed.netloc else "https://www.wayfair.com"
    path = parsed.path
    qs = parse_qs(parsed.query)

    # Extract piid if present
    piid = None
    for k, v in qs.items():
        if k.lower() in ["piid", "piid[]", "piid%5b%5d"]:
            if v and v[0]:
                piid = v[0]
                break

    # If the URL is already in /<type>/pdp/<slug>.html format
    if "/pdp/" in path:
        clean_path = path.lower()
        clean_path = re.sub(r'-+', '-', clean_path)
        clean_url = f"{domain}{clean_path}"
        if piid:
            clean_url += f"?piid={piid}"
        return clean_url

    # Extract filename / slug portion
    filename = path.strip("/").split("/")[-1]
    if filename.lower().endswith(".html"):
        name_part = filename[:-5]
    else:
        name_part = filename

    # Check for SKU pattern after ~ (e.g., ~CMU19772)
    m_sku = re.search(r'~(CMU[0-9]+|[A-Za-z0-9]+)$', name_part, re.I)
    if m_sku:
        sku = m_sku.group(1).lower()
        title_part = name_part[:m_sku.start()]
        # Strip trailing model code like -ACM-L13-K
        title_part = re.sub(r'-[A-Za-z0-9]+-[A-Za-z0-9]+-[A-Za-z0-9]+$', '', title_part)
        slug_raw = f"{title_part}-{sku}"
    else:
        slug_raw = name_part

    # Replace special characters with hyphen
    slug = re.sub(r'[^a-zA-Z0-9]', '-', slug_raw)
    # Replace multiple hyphens with single hyphen
    slug = re.sub(r'-+', '-', slug).strip('-').lower()

    new_path = f"/{shop_product_type}/pdp/{slug}.html"
    new_url = f"{domain}{new_path}"
    if piid:
        new_url += f"?piid={piid}"
    return new_url


def to_camel_case(s: str) -> str:
    """Converts a string (snake_case, hyphen-case, or space-separated) to camelCase."""
    s = re.sub(r'[\s\-_]+', ' ', s).strip()
    if not s:
        return ""
    words = s.split(' ')
    return words[0].lower() + ''.join(w.capitalize() for w in words[1:])


class WayfairParser:
    """
    Parses full PDP data from Wayfair HTML into a comprehensive camelCase dictionary.
    """

    def __init__(self, html: str, provided_url: str, page_url: str):
        self.soup = BeautifulSoup(html, "html.parser")
        self.html = html
        self.provided_url = provided_url
        self.page_url = page_url

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

        # 1. Primary: data-test-id="PricingFull-leadPrice" (Wayfair's canonical main PDP price)
        lead_price_elem = self.soup.find(attrs={"data-test-id": "PricingFull-leadPrice"})
        if lead_price_elem:
            price_text = lead_price_elem.get_text(strip=True)
            m = re.search(r"\$\d+(?:,\d+)*(?:\.\d{2})?", price_text)
            if m:
                price_data["currentPrice"] = m.group(0)

            # Look for original / was price and discount inside the PricingFull block
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

        # 2. Fallback: Search within the Buy Box container around Add to Cart
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

        # 3. Fallback: Generic search if still not found
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

        # Financing info ($/mo.)
        mo_span = self.soup.find("span", string=re.compile(r"/mo", re.I))
        if mo_span:
            mo_box = mo_span.find_parent(lambda t: t.name in ["div", "p"] and len(t.get_text(strip=True)) < 250)
            if mo_box:
                cleaned_soup = BeautifulSoup(str(mo_box), "html.parser")
                for sr in cleaned_soup.find_all(attrs={"data-test-id": re.compile(r"ScreenReaderText", re.I)}):
                    sr.decompose()
                txt = cleaned_soup.get_text(" ", strip=True)
                txt = re.sub(r"\{[A-Za-z0-9_]+\}", "", txt)
                txt = re.sub(r"\b(\w+(\s+\w+){0,3})\s+\1\b", r"\1", txt, flags=re.I)
                txt = re.sub(r"\s+", " ", txt).strip()
                price_data["financingDetails"] = txt

        # Rewards offer
        rew = self.soup.find(string=re.compile(r"Earn\s+\$\d+.*in\s+rewards", re.I))
        if rew:
            p_rew = rew.find_parent(lambda t: t.name in ["div", "p"] and len(t.get_text(strip=True)) < 250)
            if p_rew:
                cleaned_soup = BeautifulSoup(str(p_rew), "html.parser")
                for sr in cleaned_soup.find_all(attrs={"data-test-id": re.compile(r"ScreenReaderText", re.I)}):
                    sr.decompose()
                txt = cleaned_soup.get_text(" ", strip=True)
                txt = re.sub(r"\b(\w+(\s+\w+){2,6})\s+\1\b", r"\1", txt, flags=re.I)
                price_data["rewardsOffer"] = re.sub(r"\s+", " ", txt).strip()

        return price_data

    def extract_variants(self) -> Dict[str, Any]:
        variants_data: Dict[str, Any] = {
            "selectedColor": None,
            "availableOptions": []
        }

        # Selected color
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

        # Swatch options via img[alt]
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

        # About This Product
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

        # Features
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
        """Extracts specification documents (manuals, assembly instructions, spec sheets)."""
        documents = []
        seen_urls = set()

        # 1. Search specifically under "Documents" heading in specifications
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

        # 2. General fallback for any PDF / document links
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

        # 1. Product Dimensions
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

        # 2. Other Dimensions
        other_dim_p = self.soup.find(lambda t: t.name == "p" and t.get_text(strip=True) == "Other Dimensions")
        if other_dim_p:
            box = other_dim_p.find_parent("div")
            if box:
                dl = box.find_next_sibling("dl") or (box.parent and box.parent.find("dl"))
                if dl:
                    for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
                        k = to_camel_case(dt.get_text(strip=True))
                        specs["otherDimensions"][k] = dd.get_text(strip=True)

        # 3. Details / Specifications / Assembly
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
        """
        Extracts only the highest quality, unique, non-repetitive product gallery images.
        Deduplicates by image ID and filters out thumbnails and unrelated products.
        """
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

            # Skip icons, badges, ratings, placeholders, swatch filenames
            if any(bad in fn_lower for bad in ['default_name', 'logo', 'badge', 'icon', 'rating', 'star', 'swatch']):
                continue
            if fn_lower in ['black.jpg', 'white.jpg', 'espresso.jpg']:
                continue

            # Skip carousel items from other sections (sponsored, similar products)
            parent_bad = img.find_parent(lambda t: t.name in ['div', 'section'] and any(k in " ".join(t.get('class', [])).lower() for k in ['sponsored', 'similar', 'crosssell', 'browsegrid']))
            if parent_bad:
                continue

            # Verify image belongs to this product
            if sig_words:
                src_alt_text = f"{src.lower()} {alt.lower()}"
                matches = sum(1 for w in sig_words if w in src_alt_text)
                if matches < min(2, len(sig_words)):
                    continue

            if image_id not in seen_ids:
                seen_ids.add(image_id)
                # Upgrade to highest resolution (800x800 r85)
                high_res_url = f"https://assets.wfcdn.com/im/{im_hash}/resize-h800-w800%5Ecompr-r85/{subfolder}/{image_id}/{filename}"
                gallery.append(high_res_url)

        if gallery:
            images_data["mainImage"] = gallery[0]
            images_data["galleryImages"] = gallery

        return images_data


class TraversedTracker:
    """
    Tracks already traversed products and URLs to avoid duplicate scraping
    and conserve Zyte API resources.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self.exact_urls: Set[str] = set()
        self.normalized_urls: Set[str] = set()
        self.skus: Set[str] = set()
        self.existing_records: List[Dict[str, Any]] = []

    def load_from_file(self, filepath: str) -> int:
        """
        Loads existing product records from a JSON file.
        Extracts providedUrl, pageUrl, normalized PDP URL, and SKU.
        Handles trailing commas and malformed JSON safely.
        """
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
                    # Clean trailing commas before ] or }
                    cleaned = re.sub(r',\s*([\]\}])', r'\1', content)
                    data = json.loads(cleaned)

            records = data if isinstance(data, list) else [data] if isinstance(data, dict) else []

            loaded = 0
            with self._lock:
                for item in records:
                    if not isinstance(item, dict):
                        continue
                    # Ignore empty/failed error records without scraped data
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

                    sku = item.get("sku")
                    if sku and isinstance(sku, str):
                        self.skus.add(sku.strip().upper())

            logger.info(f"Loaded {loaded} traversed product(s) from '{filepath}'")
            return loaded
        except Exception as e:
            logger.warning(f"Could not load traversed products from '{filepath}': {e}")
            return 0

    def is_traversed(self, url: str) -> bool:
        """
        Returns True if the URL (or its normalized PDP form) has already been scraped.
        """
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
        """
        Dynamically marks a URL/product as traversed.
        """
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
        """
        Filters out already traversed URLs from candidate list and deduplicates in-batch.
        Returns (fresh_urls, skipped_count).
        """
        fresh_urls = []
        seen_batch = set()
        skipped = 0

        for raw_url in urls:
            u = raw_url.strip()
            if not u or u.startswith("#"):
                continue

            # Check if traversed in existing database
            if self.is_traversed(u):
                skipped += 1
                continue

            # Check in-batch duplicates
            norm = normalize_pdp_url(u) or u
            norm_key = norm.lower()
            if norm_key in seen_batch:
                skipped += 1
                continue

            seen_batch.add(norm_key)
            fresh_urls.append(u)

        return fresh_urls, skipped


def collect_zyte_keys(
    cli_keys: Optional[List[str]] = None,
    keys_file: Optional[str] = None
) -> List[str]:
    """
    Aggregates Zyte API keys with strict priority:
    1. Keys file (defaults to 'zyte_keys.txt' if present in the workspace)
    2. CLI flags (-k / --api-key / --api-keys)
    3. Environment variables (ZYTE_API_KEYS_FILE, ZYTE_API_KEYS, ZYTE_API_KEY)
    4. Built-in default key fallback
    """
    raw_candidates: List[str] = []

    # Priority 1: Check zyte_keys.txt file (or custom keys_file if provided)
    default_keys_file = "zyte_keys.txt"
    target_keys_file = keys_file or (default_keys_file if os.path.isfile(default_keys_file) else None)
    if target_keys_file and os.path.isfile(target_keys_file):
        logger.info(f"Loading Zyte API keys from file: '{target_keys_file}'")
        with open(target_keys_file, "r", encoding="utf-8") as f:
            for line in f:
                k = line.strip().strip("'\"")
                if k and not k.startswith("#"):
                    raw_candidates.append(k)

    # Priority 2: From CLI keys arguments
    if cli_keys:
        for item in cli_keys:
            if not item:
                continue
            item = item.strip()
            if os.path.isfile(item):
                with open(item, "r", encoding="utf-8") as f:
                    for line in f:
                        k = line.strip().strip("'\"")
                        if k and not k.startswith("#"):
                            raw_candidates.append(k)
            elif "," in item:
                raw_candidates.extend(item.split(","))
            else:
                raw_candidates.append(item)

    # Priority 3: From environment variables (only if no keys loaded from file or CLI)
    if not raw_candidates:
        env_keys_file = os.environ.get("ZYTE_API_KEYS_FILE")
        if env_keys_file and os.path.isfile(env_keys_file):
            with open(env_keys_file, "r", encoding="utf-8") as f:
                for line in f:
                    k = line.strip().strip("'\"")
                    if k and not k.startswith("#"):
                        raw_candidates.append(k)

        env_multi = os.environ.get("ZYTE_API_KEYS")
        if env_multi:
            raw_candidates.extend(re.split(r'[,\s]+', env_multi))

        env_single = os.environ.get("ZYTE_API_KEY")
        if env_single:
            raw_candidates.extend(re.split(r'[,\s]+', env_single))

    # Clean, validate, and deduplicate preserving order
    clean_keys: List[str] = []
    seen = set()
    for k in raw_candidates:
        stripped = k.strip().strip("'\"")
        if stripped and not stripped.startswith("#") and stripped not in seen:
            seen.add(stripped)
            clean_keys.append(stripped)

    # Priority 4: Default fallback if empty
    if not clean_keys:
        logger.info("No external Zyte keys found in zyte_keys.txt or environment; using default fallback key.")
        clean_keys = [DEFAULT_ZYTE_KEY]

    return clean_keys


class WayfairScraperEngine:
    """
    Multithreaded scraper coordinator using Zyte API with rotating keys,
    automatic failover, and traversed URL deduplication.
    """

    def __init__(
        self,
        api_keys: List[str],
        max_workers: int = 4,
        timeout: int = 60,
        traversed_tracker: Optional[TraversedTracker] = None,
        append_existing: bool = True
    ):
        self.key_pool = ZyteKeyPool(api_keys)
        self.max_workers = max_workers
        self.timeout = timeout
        self.session = create_session()
        self.traversed_tracker = traversed_tracker or TraversedTracker()
        self.append_existing = append_existing

    def fetch_page_html(self, target_url: str) -> Optional[str]:
        """
        Fetches the target page using Zyte API with browserHtml.
        Rotates Zyte keys across requests and automatically retries the EXACT same URL with the next key
        if any key fails, is exhausted (401, 402, 403), is rate limited (429), or encounters errors.
        """
        payload = {
            "url": target_url,
            "browserHtml": True
        }

        attempts = 0
        tried_keys_for_this_url = set()

        while True:
            active_count = self.key_pool.get_active_count()
            if active_count == 0:
                logger.error(f"No active Zyte API keys remaining in key pool! All keys exhausted. Could not fetch {target_url}")
                return None

            api_key = self.key_pool.get_next_key()
            if not api_key:
                logger.error("Could not obtain an active Zyte API key from pool.")
                return None

            attempts += 1
            masked = mask_key(api_key)
            tried_keys_for_this_url.add(api_key)
            logger.info(f"Extracting page via Zyte API [{masked}] (attempt {attempts}): {target_url}")

            try:
                resp = self.session.post(
                    ZYTE_API_EXTRACT_ENDPOINT,
                    auth=(api_key, ""),
                    json=payload,
                    timeout=self.timeout
                )

                if resp.status_code == 200:
                    data = resp.json()
                    html = data.get("browserHtml", "")
                    if html:
                        return html
                    logger.warning(f"Zyte API returned 200 but empty browserHtml with key [{masked}] for {target_url}. Retrying with next key...")
                    if len(tried_keys_for_this_url) >= self.key_pool.get_total_count():
                        return None
                    time.sleep(1)
                    continue

                elif resp.status_code in (401, 402, 403):
                    status_desc = "Out of credits / Quota exceeded" if resp.status_code == 402 else "Unauthorized / Forbidden"
                    logger.warning(
                        f"Zyte API key [{masked}] failed with HTTP {resp.status_code} ({status_desc}). "
                        f"Deactivating key and retrying same URL with next available key..."
                    )
                    self.key_pool.mark_key_status(api_key, resp.status_code)
                    time.sleep(1)
                    continue

                elif resp.status_code == 429:
                    logger.warning(
                        f"Zyte API key [{masked}] hit HTTP 429 (Rate Limit). "
                        f"Putting key in cooldown and retrying same URL with next key..."
                    )
                    self.key_pool.mark_key_status(api_key, 429)
                    time.sleep(1.5)
                    continue

                else:
                    logger.error(f"Zyte API error HTTP {resp.status_code} with key [{masked}]: {resp.text[:200]}")
                    if len(tried_keys_for_this_url) < self.key_pool.get_total_count():
                        logger.info(f"Retrying {target_url} with next key in pool...")
                        time.sleep(1.5)
                        continue
                    return None

            except requests.exceptions.RequestException as e:
                logger.error(f"Network error with Zyte key [{masked}]: {e}")
                if len(tried_keys_for_this_url) < self.key_pool.get_total_count():
                    logger.info(f"Retrying {target_url} with next key in pool...")
                    time.sleep(1.5)
                    continue
                return None

        return None

    def scrape_single_url(self, raw_url: str) -> Optional[Dict[str, Any]]:
        raw_url = raw_url.strip()
        if not raw_url or raw_url.startswith("#"):
            return None

        # Double check if traversed
        if self.traversed_tracker.is_traversed(raw_url):
            logger.info(f"Skipping already traversed URL: {raw_url}")
            return None

        # 1. Transform URL to canonical PDP format
        pdp_url = normalize_pdp_url(raw_url)
        logger.info(f"Processing URL: {raw_url} -> Canonical PDP: {pdp_url}")

        # 2. Fetch HTML via Zyte (with key rotation and auto-failover)
        html = self.fetch_page_html(pdp_url)
        if not html and pdp_url != raw_url:
            logger.warning(f"Could not fetch canonical PDP URL: {pdp_url}. Retrying with original URL: {raw_url}")
            html = self.fetch_page_html(raw_url)

        if not html:
            logger.error(f"Failed to fetch content for {raw_url} across available Zyte keys.")
            return None

        # 3. Parse HTML
        parser = WayfairParser(html, provided_url=raw_url, page_url=pdp_url)
        parsed_data = parser.parse()

        # Verify parsed data contains a valid product
        if not parsed_data.get("name") and not parsed_data.get("sku"):
            logger.warning(f"Extracted page for {raw_url} but neither name nor SKU could be parsed. Will not mark traversed.")
            return None

        self.traversed_tracker.mark_traversed(raw_url, parsed_data)
        logger.info(f"Successfully scraped: '{parsed_data.get('name')}' (SKU: {parsed_data.get('sku')})")
        return parsed_data

    def run(self, urls: List[str], output_file: Optional[str] = None) -> List[Dict[str, Any]]:
        # Filter candidate URLs against traversed tracker
        filtered_urls, skipped = self.traversed_tracker.filter_urls(urls)
        logger.info(
            f"Traversed URL Filter: {skipped} URL(s) skipped (already scraped or duplicate in batch). "
            f"{len(filtered_urls)} fresh URL(s) queued for scraping."
        )

        def write_output(records: List[Dict[str, Any]], target_path: str):
            temp_path = f"{target_path}.tmp"
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=2, ensure_ascii=False)
            os.replace(temp_path, target_path)

        def get_current_combined(new_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            if self.append_existing and self.traversed_tracker.existing_records:
                combined = list(self.traversed_tracker.existing_records)
                combined.extend(new_items)
                return combined
            return new_items

        if not filtered_urls:
            logger.info("All provided URLs have already been scraped! No Zyte requests needed.")
            if output_file and self.append_existing and self.traversed_tracker.existing_records:
                if not os.path.exists(output_file):
                    write_output(self.traversed_tracker.existing_records, output_file)
            return self.traversed_tracker.existing_records

        total = len(filtered_urls)
        logger.info(
            f"Starting multithreaded scraping for {total} fresh URLs with {self.max_workers} threads "
            f"across {self.key_pool.get_active_count()} Zyte API key(s)..."
        )

        new_results: List[Dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="ScraperWorker") as executor:
            future_to_url = {executor.submit(self.scrape_single_url, url): url for url in filtered_urls}
            completed = 0
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                completed += 1
                try:
                    data = future.result()
                    if data:
                        new_results.append(data)
                        logger.info(f"Progress: [{completed}/{total}] completed.")
                    else:
                        logger.warning(f"URL could not be extracted (will remain un-traversed to retry on next run): {url}")
                except Exception as exc:
                    logger.error(f"Exception scraping {url}: {exc}")

                # Checkpoint save every 25 records to prevent data loss on long jobs
                if output_file and completed % 25 == 0:
                    try:
                        write_output(get_current_combined(new_results), output_file)
                    except Exception as err:
                        logger.warning(f"Checkpoint save error: {err}")

        # Final save
        if output_file:
            final_data = get_current_combined(new_results)
            write_output(final_data, output_file)
            logger.info(f"Saved total of {len(final_data)} records ({len(new_results)} newly scraped) to {output_file}")

        return new_results


def main():
    parser = argparse.ArgumentParser(description="Wayfair PDP Multithreaded Scraper")
    parser.add_argument("-i", "--input", default="urls.txt", help="Input file containing Wayfair product URLs (one per line)")
    parser.add_argument("-o", "--output", default="wayfair_products.json", help="Output JSON filename (default: wayfair_products.json)")
    parser.add_argument("-t", "--threads", type=int, default=4, help="Number of concurrent worker threads (default: 4)")
    parser.add_argument("-k", "--api-key", "--api-keys", dest="api_keys", action="append", default=None,
                        help="Zyte API key(s) or Account key(s). Can be specified multiple times, comma-separated, or as a path to a keys file")
    parser.add_argument("--api-keys-file", default=None, help="Path to text file containing Zyte API keys (one per line)")
    parser.add_argument("-u", "--url", action="append", help="Single or multiple URLs passed directly via CLI")
    parser.add_argument("--existing-file", "--ignore-file", dest="ignore_files", action="append", default=None,
                        help="JSON file(s) containing already scraped products to ignore (default: wayfair_products.json)")
    parser.add_argument("--overwrite", action="store_true", default=False,
                        help="Overwrite output file instead of appending newly scraped products to existing products")
    parser.add_argument("--shard-index", type=int, default=0, help="0-based index of this parallel shard runner")
    parser.add_argument("--shard-total", type=int, default=1, help="Total number of parallel shard runners")
    parser.add_argument("--merge-dir", default=None, help="Directory containing shard JSON files to merge into --output")

    args = parser.parse_args()

    # If merge mode is requested, combine all shard JSON files
    if args.merge_dir:
        merge_path = args.merge_dir
        if not os.path.isdir(merge_path):
            logger.error(f"Merge directory not found: {merge_path}")
            sys.exit(1)
        merged: List[Dict[str, Any]] = []
        seen_keys = set()

        def add_record(rec: Any):
            if not isinstance(rec, dict):
                return
            u = rec.get("providedUrl") or rec.get("provided_url") or rec.get("pageUrl") or rec.get("page_url")
            key = normalize_pdp_url(u) if u else None
            if not key:
                key = u or rec.get("sku") or json.dumps(rec, sort_keys=True)
            if key and key.lower() not in seen_keys:
                seen_keys.add(key.lower())
                merged.append(rec)

        # Include existing records from output file if present
        if os.path.exists(args.output) and not args.overwrite:
            try:
                with open(args.output, "r", encoding="utf-8") as f:
                    content = json.load(f)
                    if isinstance(content, list):
                        for item in content:
                            add_record(item)
                    elif isinstance(content, dict):
                        add_record(content)
                logger.info(f"Loaded {len(merged)} existing records from {args.output} prior to merge.")
            except Exception as e:
                logger.warning(f"Could not load existing {args.output} for merge: {e}")

        out_base = os.path.basename(args.output)
        for root, _, files in os.walk(merge_path):
            for file in sorted(files):
                if file.endswith(".json") and file != out_base and not file.endswith(".tmp"):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, "r", encoding="utf-8") as f:
                            content = json.load(f)
                            before_cnt = len(merged)
                            if isinstance(content, list):
                                for item in content:
                                    add_record(item)
                            elif isinstance(content, dict):
                                add_record(content)
                            logger.info(f"Merged {len(merged) - before_cnt} new items from {file}")
                    except Exception as e:
                        logger.error(f"Failed to read {file_path}: {e}")

        temp_merge = f"{args.output}.tmp"
        with open(temp_merge, "w", encoding="utf-8") as f:
            json.dump(merged, f, indent=2, ensure_ascii=False)
        os.replace(temp_merge, args.output)
        logger.info(f"Successfully merged {len(merged)} total deduplicated records into {args.output}")
        return

    # 1. Initialize Traversed Tracker
    tracker = TraversedTracker()

    # Determine files to load existing products from
    files_to_check: List[str] = []
    if args.ignore_files:
        for f in args.ignore_files:
            if f and os.path.exists(f) and f not in files_to_check:
                files_to_check.append(f)

    # Always check default wayfair_products.json if it exists
    default_json = "wayfair_products.json"
    if os.path.exists(default_json) and default_json not in files_to_check:
        files_to_check.append(default_json)

    # Also check args.output if it exists and differs
    if args.output and os.path.exists(args.output) and args.output not in files_to_check:
        files_to_check.append(args.output)

    for fpath in files_to_check:
        tracker.load_from_file(fpath)

    # 2. Collect Candidate URLs
    urls: List[str] = []
    if args.url:
        urls.extend(args.url)
    elif os.path.exists(args.input):
        with open(args.input, "r", encoding="utf-8") as f:
            urls.extend([line.strip() for line in f if line.strip() and not line.strip().startswith("#")])
    else:
        urls = [
            "https://www.wayfair.com/ACME-Furniture--ACME-Ireland-Dresser-ACM-L13-K~CMU19772.html?PiID%5B%5D=119976344&phash=52b789&refid=FR49-CMU19772_119976344"
        ]
        logger.info("No input file found; using default sample URL.")

    if not urls:
        logger.error("No valid URLs found to scrape.")
        sys.exit(1)

    # 3. Pre-filter candidate URLs against already traversed products
    remaining_urls, skipped_count = tracker.filter_urls(urls)
    logger.info(f"URL Filtering: {skipped_count} URL(s) already scraped/duplicate. {len(remaining_urls)} remaining.")

    # 4. Apply parallel sharding on remaining fresh URLs
    if args.shard_total > 1:
        total_before = len(remaining_urls)
        remaining_urls = [url for idx, url in enumerate(remaining_urls) if idx % args.shard_total == args.shard_index]
        logger.info(f"Sharding enabled: Shard [{args.shard_index + 1}/{args.shard_total}] assigned {len(remaining_urls)} of {total_before} fresh URLs.")

    # 5. Resolve and initialize Zyte API keys
    api_keys = collect_zyte_keys(cli_keys=args.api_keys, keys_file=args.api_keys_file)

    # Determine append behavior: append if output matches existing file or if appending is not explicitly disabled
    append_existing = not args.overwrite

    # 6. Initialize Engine & Execute
    engine = WayfairScraperEngine(
        api_keys=api_keys,
        max_workers=args.threads,
        traversed_tracker=tracker,
        append_existing=append_existing
    )

    data_results = engine.run(remaining_urls, output_file=args.output)
    logger.info(f"Done! Job completed for {len(data_results)} records.")


if __name__ == "__main__":
    main()
