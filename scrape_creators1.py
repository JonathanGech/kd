import asyncio
import pandas as pd
import json
import os
import re
import requests
import logging
import random

Shop_dir = "Top_Creators"
if not os.path.exists(Shop_dir):
    os.makedirs(Shop_dir)

log_file_path = "Top_Creators/Top_Creators.log"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[
    logging.FileHandler(log_file_path),
    logging.StreamHandler()
])

output_dir = "Top_Creators/best_selling_products_images"
logo_dir = "Top_Creators/creator_logo"
trend_dir = "Top_Creators/trend_images"
os.makedirs(output_dir, exist_ok=True)
os.makedirs(logo_dir, exist_ok=True)
os.makedirs(trend_dir, exist_ok=True)

# GLOBALS TO TRACK COUNTERS ACROSS PAGES
image_counter = 1
logo_counter = 1
trend_counter = 1
rank_counter = 1

async def extract_creator_data(page, page_num):
    global image_counter, logo_counter, trend_counter, rank_counter

    # Base index starts at 1 for page 1, 51 for page 2, 101 for page 3, etc.
    base_index = 1 + (page_num - 1) * 10
    image_counter = logo_counter = trend_counter = rank_counter = base_index

    # Continue with the rest of your scraping logic...
    logging.info(f"Scraping page {page_num}...")

    await page.wait_for_selector(".ant-table-row", timeout=10000)

    rows = await page.query_selector_all(".ant-table-row")

    logging.info(f"Loaded {len(rows)} products")

    all_creators = []
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.kalodata.com"
    }

    for index, row in enumerate(rows):
        # ✅ Creator Logo
        row_key = await row.get_attribute("data-row-key") or "N/A"
        logo_el = await row.query_selector("div.Component-Image.round.w-\\[56px\\].h-\\[56px\\].w-\\[56px\\].h-\\[56px\\]")
        creator_logo_filename = "N/A"
        if logo_el:
            style = await logo_el.get_attribute("style")
            match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
            if match:
                logo_url = match.group(1)
                try:
                    response = requests.get(logo_url, headers=headers)
                    if response.status_code == 200:
                        creator_logo_filename = f"creator_logo_{rank_counter}.png"
                        with open(os.path.join(logo_dir, creator_logo_filename), "wb") as f:
                            f.write(response.content)
                        logging.info(f"Saved logo {creator_logo_filename}")
                        logo_counter += 1
                    else:
                        logging.info(f"Failed to download logo image (status {response.status_code})")
                except Exception as e:
                    logging.error(f"Error downloading logo {logo_url}: {e}")

        # Creator Profile
        creator_name_el = await row.query_selector("div.line-clamp-1:not(.text-base-999)")
        creator_name = await creator_name_el.inner_text() if creator_name_el else "N/A"

        # Creator Name
        type_el = await row.query_selector("div.text-base-999.line-clamp-1")
        type_text = await type_el.inner_text() if type_el else "N/A"

        td_elements = await row.query_selector_all("td")
        # Revenue
        rev_el = await row.query_selector("td.ant-table-cell.ant-table-column-sort")
        rev_text = await rev_el.inner_text() if rev_el else "N/A"
        # Followers
        followers = await td_elements[2].inner_text() if len(td_elements) > 2 else "N/A"
        # Content Views
        content_views = await td_elements[6].inner_text() if len(td_elements) > 6 else "N/A"

        # Revenue Trend Screenshot (5th td)
        revenue_trend_filename = "N/A"
        if len(td_elements) > 5:
            try:
                revenue_trend_filename = f"revenue_trend_{rank_counter}.png"
                await td_elements[5].screenshot(path=os.path.join(trend_dir, revenue_trend_filename))
                logging.info(f"Screenshot saved for revenue trend: {revenue_trend_filename}")
            except Exception as e:
                logging.error(f"Error capturing screenshot for revenue trend: {e}")

        # Views Trend Screenshot (7th td)
        views_trend_filename = "N/A"
        if len(td_elements) > 7:
            try:
                views_trend_filename = f"views_trend_{trend_counter}.png"
                await td_elements[7].screenshot(path=os.path.join(trend_dir, views_trend_filename))
                logging.info(f"Screenshot saved for views trend: {views_trend_filename}")
            except Exception as e:
                logging.error(f"Error capturing screenshot for views trend: {e}")

        

        # Creator Debut Time
        debut_time_el = await row.query_selector("span.Component-MemberText")
        debut_time = await debut_time_el.inner_text() if debut_time_el else "N/A"

        # Best Sellers (hover and extract image)
        best_seller_ids = []
        best_seller_images = []
        all_product_names = []
        all_product_prices = []
        image_divs = await row.query_selector_all("div.Component-Image.cover.cover")
        for image_div in image_divs:
            await image_div.hover()
            await asyncio.sleep(random.uniform(0.2, 0.5))

            style = await image_div.get_attribute("style")
            match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
            if match:
                url = match.group(1)
                id_match = re.search(r'tiktok\.product/(\d+)/', url)
                product_id = id_match.group(1) if id_match else None
                try:
                    response = requests.get(url, headers=headers)
                    if response.status_code == 200:
                        image_name = f"creator_{rank_counter}_image_{image_counter}.png"
                        image_path = os.path.join(output_dir, image_name)
                        with open(image_path, "wb") as f:
                            f.write(response.content)
                        best_seller_images.append(image_name)
                        best_seller_ids.append(product_id)  # ✅ Save product ID
                        logging.info(f"Saved {image_name} with product ID {product_id}")
                        # logging.info(f"Saved {image_name}")
                        image_counter += 1
                    else:
                        logging.info(f"Failed to download image (status {response.status_code})")
                except Exception as e:
                    logging.error(f"Error downloading {url}: {e}")
        await logo_el.hover()
        await asyncio.sleep(random.uniform(0.2, 0.5))

        # Best Seller Product Names (collected like a shared pool)
        product_elements = await page.query_selector_all("span.line-clamp-2")
        for p in product_elements:
            product_name = await p.inner_text()
            normalized_product_name = ' '.join(product_name.split())

            if normalized_product_name not in all_product_names:
                all_product_names.append(normalized_product_name)
        
        all_product_prices = []
        # Best Seller Prices (similarly, as shared pool)
        price_elements = await page.query_selector_all("div.text-\\[16px\\].min-w-\\[80px\\].h-\\[20px\\].font-medium.bg-white")
        for el in price_elements:
            price_text = await el.inner_text()
            # normalized_price = ' '.join(price_text.split())
            # if normalized_price not in all_product_prices:
            all_product_prices.append(price_text)

        creator_data = {
            "Row Key": row_key,
            "Rank": rank_counter,
            "Creator Logo": creator_logo_filename,
            "Creator Profile": creator_name,
            "Creator Name": type_text,
            "Followers": followers,
            "Best Seller IDs": best_seller_ids,
            "Best Sellers": [],
            "Best Seller Prices": [],
            "Best Seller Images": best_seller_images,
            "Revenue": rev_text,
            "Revenue Trend": revenue_trend_filename,
            "Content Views": content_views,
            "Views Trend": views_trend_filename,
            "Creator Debut Time": debut_time
        }

        all_creators.append(creator_data)
        trend_counter += 1
        rank_counter += 1

        product_idx = 0
        for creator in all_creators:
            num_images = len(creator["Best Seller Images"])
            creator["Best Sellers"] = all_product_names[product_idx:product_idx + num_images]
            # shop["Best Seller Prices"] = all_product_prices[product_idx:product_idx + num_images]
            product_idx += num_images

        # Assign Best Seller Prices based on Best Sellers
        price_idx = 0
        for creator in all_creators:
            num_products = len(creator["Best Sellers"])
            creator["Best Seller Prices"] = all_product_prices[price_idx:price_idx + num_products]
            price_idx += num_products

        
        # Display results
        for i, creator in enumerate(all_creators):
            logging.info(f"\nCreator {i + 1}:")
            for k, v in creator.items():
                logging.info(f"  {k}: {v if not isinstance(v, list) else ', '.join(v)}")

    return all_creators

async def run_creator_scraper(page, page_num):
    logging.info("Starting scraper...")
    try:
        if page_num == 1:
            logging.info("Clicking 'Creator' link inside #page_header_left...")
            await page.click("#page_header_left >> text=Creator")

            await page.click("span.ant-select-selection-item")
            await page.wait_for_selector("div.ant-select-item-option-content")
            options = await page.query_selector_all("div.ant-select-item-option-content")
            for option in options:
                text = await option.inner_text()
                if "10 / page" in text:
                    await option.click()
                    break
                
            await page.click("div.h-\\[22px\\].hover\\:bg-\\[rgb\\(238\\,246\\,253\\)\\].rounded-\\[4px\\].pl-\\[4px\\].flex.items-center.justify-between.text-\\[13px\\].whitespace-nowrap")
            await page.get_by_text("Yesterday").click()
            await page.click("span.animate-pulse-subtle")

        await asyncio.sleep(3)

        all_results = []
        semaphore = asyncio.Semaphore(5)  # Limit concurrent page processing
        

        async def process_page(page_num):
            nonlocal all_results
            async with semaphore:
                try:
                    if page_num > 1:
                        selector = f'li.ant-pagination-item >> a[rel="nofollow"]:has-text("{page_num}")'
                        try:
                            await page.click(selector)
                            await asyncio.sleep(4)
                        except Exception as e:
                            logging.error(f"Page {page_num} navigation failed: {e}")
                            return

                    page_results = await extract_creator_data(page, page_num)
                    all_results.extend(page_results)

                    # Save progressively
                    allcreators = pd.DataFrame(page_results)

                    # Save to CSV
                    df = pd.DataFrame(allcreators)
                    df["Row Key"] = df["Row Key"].apply(lambda x: f"'{x}")
                    df["Best Sellers"] = df["Best Sellers"].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
                    df["Best Seller Prices"] = df["Best Seller Prices"].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
                    df["Best Seller Images"] = df["Best Seller Images"].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

                    # Use a lock for file operations
                    async with asyncio.Lock():
                        df.to_csv("Top_Creators/top_creators_output.csv", mode="a", index=False, 
                                 header=not os.path.exists("Top_Creators/top_creators_output.csv"))
                        with open("Top_Creators/top_creators_output.json", "a", encoding="utf-8") as f:
                            json.dump(page_results, f, ensure_ascii=False, indent=4)

                except Exception as e:
                    logging.error(f"Error processing page {page_num}: {e}")
                    if "TargetClosedError" in str(e):
                        raise  # Re-raise if page was closed
        
        await process_page(page_num)

        logging.info("Scraping complete!")
        logging.info(f"Total products scraped: {len(all_results)}")

    except Exception as e:
        logging.error(f"Scraping failed: {e}")
        raise

    logging.info("Scraping complete!")
    logging.info(f"Total products scraped: {len(all_results)}")


async def creator_main(page):
    for page_num in range(1,2):
        await run_creator_scraper(page, page_num)  # You can loop this later if needed
        