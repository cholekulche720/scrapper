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
from typing import Dict, Any, List, Optional
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


def create_session() -> requests.Session:
    """Creates a requests session configured with retries for robust scraping."""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
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
    Caches the resolved key in memory.
    """
    _cached_api_key: Optional[str] = None

    @classmethod
    def resolve_key(cls, user_key: str) -> str:
        if cls._cached_api_key:
            return cls._cached_api_key

        key = user_key.strip()

        # Check if the key works directly on the Zyte API extract endpoint
        try:
            test_resp = requests.post(
                ZYTE_API_EXTRACT_ENDPOINT,
                auth=(key, ""),
                json={"url": "https://httpbin.org/ip", "httpResponseBody": True},
                timeout=8
            )
            if test_resp.status_code == 200:
                logger.info("Direct Zyte API key validated successfully.")
                cls._cached_api_key = key
                return key
        except Exception:
            pass

        # If not, resolve it via Scrapinghub / Zyte management API
        logger.info("Resolving Zyte API service credentials from Zyte Account API...")
        try:
            token = base64.b64encode(f"{key}:".encode()).decode()
            headers = {"Authorization": f"Basic {token}"}
            
            # Fetch organization
            org_res = requests.get("https://app.zyte.com/api/v2/organizations", headers=headers, timeout=10)
            if org_res.status_code == 200:
                results = org_res.json().get("results", [])
                if results:
                    org_id = results[0].get("id")
                    # Fetch zyteapi services
                    services_res = requests.get(
                        f"https://app.zyte.com/api/v2/zyteapi?organization={org_id}",
                        headers=headers,
                        timeout=10
                    )
                    if services_res.status_code == 200:
                        s_results = services_res.json().get("results", [])
                        for service in s_results:
                            sid = service.get("id")
                            # Fetch credentials
                            cred_res = requests.get(
                                f"https://app.zyte.com/api/v2/zyteapi/{sid}/credentials?organization={org_id}",
                                headers=headers,
                                timeout=10
                            )
                            if cred_res.status_code == 200:
                                apikey = cred_res.json().get("apikey")
                                if apikey:
                                    logger.info(f"Resolved Zyte API Key for '{service.get('name')}' (Service ID: {sid})")
                                    cls._cached_api_key = apikey
                                    return apikey
        except Exception as e:
            logger.warning(f"Could not auto-resolve Zyte key via management API: {e}")

        cls._cached_api_key = key
        return key


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


class WayfairScraperEngine:
    """
    Multithreaded scraper coordinator using Zyte API.
    """

    def __init__(self, api_key: str, max_workers: int = 4, timeout: int = 60):
        self.raw_api_key = api_key or os.environ.get("ZYTE_API_KEY", DEFAULT_ZYTE_KEY)
        self.resolved_api_key = ZyteKeyResolver.resolve_key(self.raw_api_key)
        self.max_workers = max_workers
        self.timeout = timeout
        self.session = create_session()

    def fetch_page_html(self, target_url: str) -> Optional[str]:
        """
        Fetches the target page using Zyte API with browserHtml for complete dynamic JS rendering.
        """
        logger.info(f"Extracting page via Zyte API (browserHtml): {target_url}")
        payload = {
            "url": target_url,
            "browserHtml": True
        }
        try:
            resp = self.session.post(
                ZYTE_API_EXTRACT_ENDPOINT,
                auth=(self.resolved_api_key, ""),
                json=payload,
                timeout=self.timeout
            )
            if resp.status_code == 200:
                return resp.json().get("browserHtml", "")
            elif resp.status_code == 401:
                logger.warning("Zyte API returned 401. Re-resolving credentials...")
                self.resolved_api_key = ZyteKeyResolver.resolve_key(self.raw_api_key)
                resp = self.session.post(
                    ZYTE_API_EXTRACT_ENDPOINT,
                    auth=(self.resolved_api_key, ""),
                    json=payload,
                    timeout=self.timeout
                )
                if resp.status_code == 200:
                    return resp.json().get("browserHtml", "")
            logger.error(f"Zyte API extract failed with status {resp.status_code}: {resp.text}")
        except Exception as e:
            logger.error(f"Zyte API request failed: {e}")

        return None

    def scrape_single_url(self, raw_url: str) -> Optional[Dict[str, Any]]:
        raw_url = raw_url.strip()
        if not raw_url or raw_url.startswith("#"):
            return None

        # 1. Transform URL to canonical PDP format
        pdp_url = normalize_pdp_url(raw_url)
        logger.info(f"Processing URL: {raw_url} -> Canonical PDP: {pdp_url}")

        # 2. Fetch HTML via Zyte
        html = self.fetch_page_html(pdp_url)
        if not html:
            logger.warning(f"Could not fetch canonical PDP URL: {pdp_url}. Trying original URL...")
            html = self.fetch_page_html(raw_url)
            if not html:
                logger.error(f"Failed to fetch content for {raw_url}")
                return {
                    "provided_url": raw_url,
                    "page_url": pdp_url,
                    "error": "Failed to fetch HTML via Zyte API"
                }

        # 3. Parse HTML
        parser = WayfairParser(html, provided_url=raw_url, page_url=pdp_url)
        parsed_data = parser.parse()
        logger.info(f"Successfully scraped: '{parsed_data.get('name')}' (SKU: {parsed_data.get('sku')})")
        return parsed_data

    def run(self, urls: List[str], output_file: Optional[str] = None) -> List[Dict[str, Any]]:
        clean_urls = [u.strip() for u in urls if u.strip() and not u.strip().startswith("#")]
        total = len(clean_urls)
        logger.info(f"Starting multithreaded scraping for {total} URLs with {self.max_workers} threads...")

        results: List[Dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="ScraperWorker") as executor:
            future_to_url = {executor.submit(self.scrape_single_url, url): url for url in clean_urls}
            completed = 0
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                completed += 1
                try:
                    data = future.result()
                    if data:
                        results.append(data)
                        logger.info(f"Progress: [{completed}/{total}] completed.")
                except Exception as exc:
                    logger.error(f"Error scraping {url}: {exc}")
                    results.append({
                        "provided_url": url,
                        "error": str(exc)
                    })

                # Checkpoint save every 25 records to prevent data loss on long jobs
                if output_file and completed % 25 == 0:
                    try:
                        temp_file = f"{output_file}.tmp"
                        with open(temp_file, "w", encoding="utf-8") as f:
                            json.dump(results, f, indent=2, ensure_ascii=False)
                        os.replace(temp_file, output_file)
                    except Exception as err:
                        logger.warning(f"Checkpoint save error: {err}")

        # Final save
        if output_file:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2, ensure_ascii=False)

        return results


def main():
    parser = argparse.ArgumentParser(description="Wayfair PDP Multithreaded Scraper")
    parser.add_argument("-i", "--input", default="urls.txt", help="Input file containing Wayfair product URLs (one per line)")
    parser.add_argument("-o", "--output", default="wayfair_products.json", help="Output JSON filename (default: wayfair_products.json)")
    parser.add_argument("-t", "--threads", type=int, default=4, help="Number of concurrent worker threads (default: 4)")
    parser.add_argument("-k", "--api-key", default=None, help="Zyte API key or Account key (default: from ZYTE_API_KEY env or builtin)")
    parser.add_argument("-u", "--url", action="append", help="Single or multiple URLs passed directly via CLI")
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
        merged = []
        out_base = os.path.basename(args.output)
        for root, _, files in os.walk(merge_path):
            for file in sorted(files):
                if file.endswith(".json") and file != out_base and not file.endswith(".tmp"):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, "r", encoding="utf-8") as f:
                            content = json.load(f)
                            if isinstance(content, list):
                                merged.extend(content)
                            elif isinstance(content, dict):
                                merged.append(content)
                            logger.info(f"Loaded {len(content) if isinstance(content, list) else 1} items from {file}")
                    except Exception as e:
                        logger.error(f"Failed to read {file_path}: {e}")
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(merged, f, indent=2, ensure_ascii=False)
        logger.info(f"Successfully merged {len(merged)} total records into {args.output}")
        return

    # Collect URLs
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

    # Apply parallel sharding if configured
    if args.shard_total > 1:
        total_before = len(urls)
        urls = [url for idx, url in enumerate(urls) if idx % args.shard_total == args.shard_index]
        logger.info(f"Sharding enabled: Shard [{args.shard_index + 1}/{args.shard_total}] assigned {len(urls)} of {total_before} URLs.")

    # Initialize Engine
    api_key = args.api_key or os.environ.get("ZYTE_API_KEY", DEFAULT_ZYTE_KEY)
    engine = WayfairScraperEngine(api_key=api_key, max_workers=args.threads)

    # Run with checkpoint writing
    data_results = engine.run(urls, output_file=args.output)

    logger.info(f"Done! Successfully wrote {len(data_results)} records to {args.output}")


if __name__ == "__main__":
    main()
