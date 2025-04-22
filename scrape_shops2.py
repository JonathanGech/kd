import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import json
import os
import re
import requests
import logging

Shop_dir = "Top_Shops"
if not os.path.exists(Shop_dir):
    os.makedirs(Shop_dir)

log_file_path = "Top_Shops/Top_Shops.log"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", 
    encoding='utf-8',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)

output_dir = "Top_Shops/best_selling_products_images"
logo_dir = "Top_Shops/shop_logo"
trend_dir = "Top_Shops/trend_images"
os.makedirs(output_dir, exist_ok=True)
os.makedirs(logo_dir, exist_ok=True)
os.makedirs(trend_dir, exist_ok=True)

# GLOBALS TO TRACK COUNTERS ACROSS PAGES
image_counter = 1
logo_counter = 1
trend_counter = 1
rank_counter = 1


async def extract_shop_data(page, page_num):
    global image_counter, logo_counter, trend_counter
    global all_product_names, all_product_prices

    # Base index starts at 1 for page 1, 51 for page 2, 101 for page 3, etc.
    base_index = 1 + (page_num - 1) * 50
    image_counter = logo_counter = trend_counter = rank_counter = base_index

    # Continue with the rest of your scraping logic...
    logging.info(f"Scraping page {page_num}...")

    await page.wait_for_selector(".ant-table-row", timeout=10000)

    rows = await page.query_selector_all(".ant-table-row")
    logging.info(f"Loaded {len(rows)} products")

    all_shops = []
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.kalodata.com"
    }

    for index, row in enumerate(rows):
        # ✅ Shop Logo
        row_key = await row.get_attribute("data-row-key") or "N/A"
        logo_el = await row.query_selector("div.Component-Image.w-\\[56px\\].h-\\[56px\\].w-\\[56px\\].h-\\[56px\\]")
        shop_logo_filename = "N/A"
        if logo_el:
            style = await logo_el.get_attribute("style")
            match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
            if match:
                logo_url = match.group(1)
                try:
                    response = requests.get(logo_url, headers=headers)
                    if response.status_code == 200:
                        shop_logo_filename = f"shop_logo_{rank_counter}.png"
                        with open(os.path.join(logo_dir, shop_logo_filename), "wb") as f:
                            f.write(response.content)
                        logging.info(f"Saved shop {index + 1} logo as {shop_logo_filename}")
                        logo_counter += 1
                    else:
                        logging.error(f"Failed to download shop logo (status {response.status_code})")
                except Exception as e:
                    logging.error(f"Error downloading logo {logo_url}: {e}")

        # Shop Name
        shop_name_el = await row.query_selector("div.line-clamp-1:not(.text-base-999)")
        shop_name = await shop_name_el.inner_text() if shop_name_el else "N/A"

        # Shop Type
        type_el = await row.query_selector("div.text-base-999.line-clamp-1")
        type_text = await type_el.inner_text() if type_el else "N/A"

        # Revenue
        rev_el = await row.query_selector("td.ant-table-cell.ant-table-column-sort")
        rev_text = await rev_el.inner_text() if rev_el else "N/A"

        # All TDs (for trend screenshots)
        td_elements = await row.query_selector_all("td")

        # Average Unit Price (last td)
        avg_unitp_text = await td_elements[-1].inner_text() if td_elements else "N/A"

        # Revenue Trend Screenshot (5th td)
        revenue_trend_filename = "N/A"
        if len(td_elements) > 4:
            try:
                revenue_trend_filename = f"revenue_trend_{rank_counter}.png"
                await td_elements[4].screenshot(path=os.path.join(trend_dir, revenue_trend_filename))
                logging.info(f"Screenshot saved for revenue trend: {revenue_trend_filename}")
            except Exception as e:
                logging.error(f"Error capturing screenshot for revenue trend: {e}")

        # Best Seller Images
        best_seller_images = []
        product_elements = ""
        all_product_names = []
        price_elements = ""
        all_product_prices = []

        image_divs = await row.query_selector_all("div.Component-Image.cover.cover")
        for image_div in image_divs:
            await image_div.hover()
            await asyncio.sleep(.2)
            
            # Image URL
            style = await image_div.get_attribute("style")
            match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
            if match:
                url = match.group(1)
                try:
                    response = requests.get(url, headers=headers)
                    if response.status_code == 200:
                        image_name = f"shop_{rank_counter}_image_{image_counter}.png"
                        image_path = os.path.join(output_dir, image_name)
                        with open(image_path, "wb") as f:
                            f.write(response.content)
                        best_seller_images.append(image_name)
                        logging.info(f"Saved {image_name}")
                    
                        image_counter += 1
                    else:
                        logging.info(f"Failed to download image (status {response.status_code})")
                except Exception as e:
                    logging.error(f"Error downloading {url}: {e}")
            # asyncio.sleep(0.5)
        await logo_el.hover()
        await asyncio.sleep(.2)
        # Best Seller Product Names (collected like a shared pool)
        # all_product_names = []
        product_elements = await page.query_selector_all("span.line-clamp-2")
        for p in product_elements:
            product_name = await product_elements.inner_text()
            all_product_names.append(' '.join(product_name.split()))
        # all_product_prices = []
        # Best Seller Prices (similarly, as shared pool)
        price_elements = await page.query_selector_all("div.text-\\[16px\\].min-w-\\[80px\\].h-\\[20px\\].font-medium.bg-white")
        for el in price_elements:
            price_text = await price_elements.inner_text()
            all_product_prices.append(price_text)
                # normalized_price = ' '.join(price_text.split())
                # if normalized_price not in all_product_prices:
            
        
        # for el in all_product_prices:
        #     print(f"Price: {el}")
            

        shop_data = {
            "Row Key": str(row_key),
            "Rank": rank_counter,
            "Shop Logo": shop_logo_filename,
            "Name": shop_name,
            "Type": type_text,
            "Best Sellers": [],  # filled later
            "Best Seller Prices": [],  # filled later
            "Best Seller Images": best_seller_images,
            "Revenue": rev_text,
            "Revenue Trend": revenue_trend_filename,
            "Average Unit Price": avg_unitp_text
        }
        
        all_shops.append(shop_data)
        # image_counter += 1
        # logo_counter += 1
        rank_counter += 1
        trend_counter += 1
        # Slice product names and prices per shop using best seller images as reference
        product_idx = 0
        for shop in all_shops:
            num_images = len(shop["Best Seller Images"])
            shop["Best Sellers"] = all_product_names[product_idx:product_idx + num_images]
            # shop["Best Seller Prices"] = all_product_prices[product_idx:product_idx + num_images]
            product_idx += num_images
                    
        # Assign Best Seller Prices based on Best Sellers
        price_idx = 0
        for shop in all_shops:
            num_products = len(shop["Best Sellers"])
            shop["Best Seller Prices"] = all_product_prices[price_idx:price_idx + num_products]
            price_idx += num_products

        # Display results
        for i, shop in enumerate(all_shops):
            logging.info(f"\nShop {i + 1}:")
            for k, v in shop.items():
                logging.info(f"  {k}: {v if not isinstance(v, list) else ', '.join(v)}")        
    
    return all_shops

async def run_shop_scraper(page, page_num):
    logging.info("Starting scraper...")
    try:
        if page_num == 1 or page_num == 4 or page_num == 7:
            logging.info("Clicking 'Shop' link inside #page_header_left...")
            await page.click("#page_header_left >> text=Shop")

            await page.click("span.ant-select-selection-item")
            await page.wait_for_selector("div.ant-select-item-option-content")
            options = await page.query_selector_all("div.ant-select-item-option-content")
            for option in options:
                text = await option.inner_text()
                if "50 / page" in text:
                    await option.click()
                    break

            await page.click("div.h-\\[22px\\].hover\\:bg-\\[rgb\\(238\\,246\\,253\\)\\].rounded-\\[4px\\].pl-\\[4px\\].flex.items-center.justify-between.text-\\[13px\\].whitespace-nowrap")
            # await page.get_by_text("Yesterday").click()
            await page.locator("text=/^Yesterday/").first.click()
            # asyncio.sleep(2)
            await page.get_by_text("Submit").click()
            # await page.click("span.animate-pulse-subtle")

            

        await asyncio.sleep(3)

        all_results = []
        semaphore = asyncio.Semaphore(3)  # Limit concurrent page processing
        

        async def process_page(page_num):
            nonlocal all_results
            async with semaphore:
                try:
                    if page_num > 1:
                        selector = f'li.ant-pagination-item >> a[rel="nofollow"]:has-text("{page_num}")'
                        if page_num == 7:
                            selector = f'li.ant-pagination-item >> a[rel="nofollow"]:has-text("5")'
                            await page.click(selector)
                            await asyncio.sleep(3)
                            selector = f'li.ant-pagination-item >> a[rel="nofollow"]:has-text("{page_num}")'
                        # elif page_num == 9:
                        #     selector = f'li.ant-pagination-item >> a[rel="nofollow"]:has-text("5")'
                        #     await page.click(selector)
                        #     selector = f'li.ant-pagination-item >> a[rel="nofollow"]:has-text("7")'
                        #     await page.click(selector)
                        #     await asyncio.sleep(3)
                        #     selector = f'li.ant-pagination-item >> a[rel="nofollow"]:has-text("{page_num}")'
                        try:
                            if selector:
                                await page.click(selector)
                                await asyncio.sleep(3)
                            # else:
                                # more = await page.query_selector("span.ant-pagination-item-ellipsis")
                                # await page.click(more)
                                
                        except Exception as e:
                            logging.error(f"Page {page_num} navigation failed: {e}")
                            await page.click("span.ant-pagination-item-ellipsis")
                            await page.click(selector)
                            await asyncio.sleep(3)
                            return

                    page_results = await extract_shop_data(page, page_num)
                    all_results.extend(page_results)

                    # Save progressively
                    allshops = pd.DataFrame(page_results)
        
                    # Save to CSV
                    df = pd.DataFrame(allshops)
                    df["Best Sellers"] = df["Best Sellers"].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
                    df["Best Seller Prices"] = df["Best Seller Prices"].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
                    df["Best Seller Images"] = df["Best Seller Images"].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
                    
                    # df["Best Sellers"] = df["Best Sellers"].apply(lambda x: ', '.join(x))
                    # df["Best Seller Prices"] = df["Best Seller Prices"].apply(lambda x: ', '.join(x))
                    # df["Best Seller Images"] = df["Best Seller Images"].apply(lambda x: ', '.join(x))

                    # Use a lock for file operations
                    async with asyncio.Lock():
                        df.to_csv("Top_Shops/top_shops_output.csv", mode="a", index=False, 
                                 header=not os.path.exists("Top_Shops/top_shops_output.csv"))
                        with open("Top_Shops/top_shops_output.json", "a", encoding="utf-8") as f:
                            json.dump(page_results, f, ensure_ascii=False, indent=4)

                except Exception as e:
                    logging.error(f"Error processing page {page_num}: {e}")
                    if "TargetClosedError" in str(e):
                        raise  # Re-raise if page was closed

        # # Create tasks for all pages but process them with limited concurrency
        # tasks = [process_page(page_num) for page_num in range(1, 11)]
        # await asyncio.gather(*tasks, return_exceptions=True)
        # for page_num in range(1, 11):
        #     await process_page(page_num)
        await process_page(page_num)

        logging.info("Scraping complete!")
        logging.info(f"Total products scraped: {len(all_results)}")

    except Exception as e:
        logging.error(f"Scraping failed: {e}")
        raise

    logging.info("Scraping complete!")
    logging.info(f"Total products scraped: {len(all_results)}")


async def shop_main(page, page_range):
    for page_num in page_range:
        await run_shop_scraper(page, page_num)  # You can loop this later if needed
    await page.close()
