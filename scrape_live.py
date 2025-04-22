import asyncio
import pandas as pd
import json
import os
import re
import requests
import logging

live_dir = "Top_Lives"
if not os.path.exists(live_dir):
    os.makedirs(live_dir)

log_file_path = "Top_Lives/Top_Lives.log"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[
    logging.FileHandler(log_file_path),
    logging.StreamHandler()
])

best_seller_dir = "Top_Lives/best_seller_images"
logo_dir = "Top_Lives/live_content_image"
os.makedirs(best_seller_dir, exist_ok=True)
os.makedirs(logo_dir, exist_ok=True)

# GLOBALS TO TRACK COUNTERS ACROSS PAGES
image_counter = 1
logo_counter = 1

async def extract_live_data(page, page_num):
    global image_counter, logo_counter

    # Base index starts at 1 for page 1, 51 for page 2, 101 for page 3, etc.
    base_index = 1 + (page_num - 1) * 10
    image_counter = logo_counter = base_index

    # Continue with the rest of your scraping logic...
    logging.info(f"Scraping page {page_num}...")

    await page.wait_for_selector(".ant-table-row.ant-table-row-level-0", timeout=10000)

    rows = await page.query_selector_all(".ant-table-row.ant-table-row-level-0")
    logging.info(f"Loaded {len(rows)} videos")

    all_lives = []
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.kalodata.com"
    }

    for index, row in enumerate(rows):
        live_logo_filename = "N/A"
        row_key = await row.get_attribute("data-row-key") or "N/A"
        
        logo_el = await row.query_selector("div.Component-Image.Layout-VideoCover.cover.Layout-VideoCover.cover")
        if logo_el:
            style = await logo_el.get_attribute("style")
            match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
            if match:
                logo_url = match.group(1)
                try:
                    response = requests.get(logo_url, headers=headers)
                    if response.status_code == 200:
                        live_logo_filename = f"live_coverpic_{logo_counter}.png"
                        with open(os.path.join(logo_dir, live_logo_filename), "wb") as f:
                            f.write(response.content)
                        logo_counter += 1
                        logging.info(f"Downloaded coverpic for live {index + 1} as {live_logo_filename}")
                except Exception as e:
                    logging.error(f"Failed to download coverpic for live {index + 1}: {e}")

        # Live Name
        live_name_el = await row.query_selector("div.group-hover\\:text-primary.line-clamp-2")
        live_name = await live_name_el.inner_text() if live_name_el else "N/A"

        # Live Profile
        live_profile = await row.query_selector("div.text-base-999")
        live_profile_text = await live_profile.inner_text() if live_profile else "N/A"

        # Revenue
        rev_el = await row.query_selector("td.ant-table-cell.ant-table-column-sort")
        rev_text = await rev_el.inner_text() if rev_el else "N/A"

        # All TDs (for trend screenshots)
        td_elements = await row.query_selector_all("td")

        # Views
        views_num = await td_elements[5].inner_text() if len(td_elements) > 5 else "N/A"
        # GPM
        gpm = await td_elements[6].inner_text() if len(td_elements) > 6 else "N/A"

        # Select the parent div inside the <td>
        time_cell = await row.query_selector("td.ant-table-cell div.flex")

        # Get the two inner divs
        date_range_el = await time_cell.query_selector("div:nth-child(1)")
        duration_el = await time_cell.query_selector("div:nth-child(2)")

        # Extract text
        date_range = await date_range_el.inner_text() if date_range_el else "N/A"
        duration = await duration_el.inner_text() if duration_el else "N/A"

        # Best Seller Images
        best_seller_images = []
        all_product_names = []
        all_product_prices = []
        # Component-Image Layout-VideoCover cover Layout-VideoCover cover

        image_divs = await row.query_selector_all("div.Component-Image.cover.cover:not(.Layout-VideoCover)")
        for image_div in image_divs:
            await image_div.hover()
            await asyncio.sleep(1)

            # Image URL
            style = await image_div.get_attribute("style")
            match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
            if match:
                url = match.group(1)
                try:
                    response = requests.get(url, headers=headers)
                    if response.status_code == 200:
                        image_name = f"live_product_{index+1}_image_{image_counter}.png"
                        image_path = os.path.join(best_seller_dir, image_name)
                        with open(image_path, "wb") as f:
                            f.write(response.content)
                        best_seller_images.append(image_name)
                        logging.info(f"Saved {image_name}")
                        image_counter += 1
                    else:
                        logging.info(f"Failed to download image (status {response.status_code})")
                except Exception as e:
                    logging.error(f"Error downloading {url}: {e}")
        
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

        live_data = {
            "Row Key": row_key,
            "Rank": index + 1,
            "Live Cover Pic": live_logo_filename,
            "Live Name": live_name,
            "Profile Name": live_profile_text,
            "Best Sellers": [],  # filled later
            "Best Seller Prices": [],  # filled later
            "Best Seller Images": best_seller_images,
            "Time Frame": date_range,
            "Duration": duration,
            "Revenue": rev_text,
            "Views": views_num,
            "GPM": gpm
        }

        all_lives.append(live_data)
        # trend_counter += 1
        
        product_idx = 0
        for live in all_lives:
            num_images = len(live["Best Seller Images"])
            live["Best Sellers"] = all_product_names[product_idx:product_idx + num_images]
            # shop["Best Seller Prices"] = all_product_prices[product_idx:product_idx + num_images]
            product_idx += num_images
                    
        # Assign Best Seller Prices based on Best Sellers
        price_idx = 0
        for live in all_lives:
            num_products = len(live["Best Seller Images"])
            live["Best Seller Prices"] = all_product_prices[price_idx:price_idx + num_products]
            price_idx += num_products

        # Display results
        for i, live in enumerate(all_lives):
            logging.info(f"\nLive {i + 1}:")
            for k, v in live.items():
                logging.info(f"  {k}: {v if not isinstance(v, list) else ', '.join(v)}")        
    
    return all_lives

async def run_live_scraper(page, page_num):
    logging.info("Starting scraper...")
    try:
        logging.info("Clicking 'Livestream' link inside #page_header_left...")
        await page.click("#page_header_left >> text=Livestream")

        await page.click("div.h-\\[22px\\].hover\\:bg-\\[rgb\\(238\\,246\\,253\\)\\].rounded-\\[4px\\].pl-\\[4px\\].flex.items-center.justify-between.text-\\[13px\\].whitespace-nowrap")
        await page.get_by_text("Yesterday").click()
        await page.click("span.animate-pulse-subtle")

        await page.click("span.ant-select-selection-item")
        await page.wait_for_selector("div.ant-select-item-option-content")
        options = await page.query_selector_all("div.ant-select-item-option-content")
        for option in options:
            text = await option.inner_text()
            if "10 / page" in text:
                await option.click()
                break

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

                    page_results = await extract_live_data(page, page_num)
                    all_results.extend(page_results)

                    # Save progressively
                    alllive = pd.DataFrame(page_results)

                    # Save to CSV
                    df = pd.DataFrame(alllive)
                    df["Best Sellers"] = df["Best Sellers"].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
                    df["Best Seller Prices"] = df["Best Seller Prices"].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
                    df["Best Seller Images"] = df["Best Seller Images"].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
                    
                    # Use a lock for file operations
                    async with asyncio.Lock():
                        df.to_csv("Top_Lives/top_lives_output.csv", mode="a", index=False, 
                                 header=not os.path.exists("Top_Lives/top_lives_output.csv"))
                        with open("Top_Lives/top_lives_output.json", "a", encoding="utf-8") as f:
                            json.dump(page_results, f, ensure_ascii=False, indent=4)

                except Exception as e:
                    logging.error(f"Error processing page {page_num}: {e}")
                    if "TargetClosedError" in str(e):
                        raise  # Re-raise if page was closed

        await process_page(page_num)

        logging.info("Scraping complete!")
        logging.info(f"Total lives scraped: {len(all_results)}")

    except Exception as e:
        logging.error(f"Scraping failed: {e}")
        raise

    logging.info("Scraping complete!")
    logging.info(f"Total lives scraped: {len(all_results)}")