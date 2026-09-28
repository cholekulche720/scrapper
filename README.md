# Wayfair PDP Multithreaded Scraper

A high-performance, multithreaded Python scraper for Wayfair Product Detail Pages (PDP) powered by **Zyte API**. Designed to run both locally and in automated CI/CD environments like **GitHub Actions**.

---

## Features

- **Automated URL Normalization**: Converts legacy, affiliate, or catalog URLs into canonical Wayfair PDP URLs (`/<shop-product-type>/pdp/<slug>.html?piid=<piid>`).
- **Zyte API Integration with Dynamic Rendering**: Bypasses anti-bot protection and PerimeterX using Zyte's `browserHtml` engine.
- **Auto Credential Resolver**: Automatically resolves Zyte API service credentials from Zyte Account keys.
- **Multithreaded / Parallel Scraping**: Concurrent URL processing with configurable thread pool workers (`ThreadPoolExecutor`).
- **Multi-Level JSON Output**: Extracts complete structured product data from breadcrumbs down to the return policy.
- **GitHub Actions Ready**: Includes `.github/workflows/scraper.yml` to run scrapers manually or on a schedule in GitHub.

---

## Extracted Data Points

The scraper produces a clean, multi-level JSON file (`wayfair_products.json`) using **camelCase** keys:

- **URLs**: `providedUrl` (raw input) and canonical `pageUrl` (transformed PDP)
- **Product Identification**: `name`, `brand`, and `sku`
- **Breadcrumbs**: Hierarchical category path array
- **Pricing & Offers** (`price`):
  - `currentPrice`
  - `originalPrice`
  - `discountPercentage`
  - `financingDetails` (e.g. Klarna monthly plan)
  - `rewardsOffer` (Wayfair Rewards)
- **Colors & Variants** (`colorAndVariants`):
  - `selectedColor`
  - `availableOptions` array with swatch thumbnail URL and stock status (`selected`, `out of stock`)
- **Highlights** (`atAGlance`): Bullet point highlights
- **Description** (`description`):
  - `aboutThisProduct` (Full product narrative)
  - `features` (Bullet points: drawers, glide mechanisms, construction, hardware)
- **Specifications** (`specifications`):
  - `productDimensions` (`dimensionImageUrl` and measurements)
  - `otherDimensions` (`overallDimensions`, drawer dimensions, etc.)
  - `details` (`material`, `numberOfDrawers`, `color`, `constructionFeatures`, etc.)
  - `assembly` (`assemblyRequired`, `assembly`)
- **Images** (`images`):
  - `mainImage`: Highest resolution (800x800) primary product photo URL
  - `galleryImages`: Deduplicated array of the highest resolution (800x800) product photos with zero thumbnails or unrelated products

---

## Local Setup & Usage

### 1. Installation

Ensure you have Python 3.10+ installed.

```bash
# Clone the repository
git clone <your-repo-url>
cd <your-repo-folder>

# Create and activate virtual environment
python -m venv .venv

# Windows:
.venv\Scripts\activate
# Linux/macOS:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Add URLs to Scrape

Place your URLs inside [urls.txt](file:///e:/Deven/urls.txt) (one URL per line):

```text
https://www.wayfair.com/ACME-Furniture--ACME-Ireland-Dresser-ACM-L13-K~CMU19772.html?PiID%5B%5D=119976344&phash=52b789&refid=FR49-CMU19772_119976344
```

### 3. Run the Scraper

Run with default settings (reads from `urls.txt`, 4 threads, outputs to `wayfair_products.json`):

```bash
python wayfair_scraper.py
```

#### CLI Options:

```bash
python wayfair_scraper.py \
  --input urls.txt \
  --output wayfair_products.json \
  --threads 4 \
  --api-key "YOUR_ZYTE_API_KEY"
```

Or scrape single URLs directly from the terminal:

```bash
python wayfair_scraper.py --url "https://www.wayfair.com/furniture/pdp/acme-furniture-acme-ireland-dresser-cmu19772.html?piid=119976344"
```

---

## Running in GitHub Actions

This repository includes a ready-to-use GitHub Actions workflow located at [.github/workflows/scraper.yml](file:///e:/Deven/.github/workflows/scraper.yml).

### Steps to Run on GitHub:

1. **Push your code to GitHub**:
   ```bash
   git init
   git add .
   git commit -m "Initial commit for Wayfair scraper"
   git remote add origin https://github.com/<your-username>/<your-repo-name>.git
   git push -u origin main
   ```

2. **Add Zyte Secret (Optional)**:
   - Go to your repository on GitHub.
   - Navigate to **Settings** > **Secrets and variables** > **Actions**.
   - Click **New repository secret**.
   - Name: `ZYTE_API_KEY`
   - Value: Your Zyte API key.

3. **Trigger the Workflow**:
   - Go to the **Actions** tab on your GitHub repository.
   - Select **Wayfair Multithreaded Scraper** from the left sidebar.
   - Click **Run workflow**.
   - Configure thread count and input file as desired, then click **Run workflow**.

4. **Download the Scraped Data**:
   - Once the action finishes, download `wayfair-products-data` from the **Artifacts** section at the bottom of the workflow run page.
