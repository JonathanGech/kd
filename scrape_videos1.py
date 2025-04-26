import asyncio
import pandas as pd
import json
import os
import re
import requests
import random
from p_logging import get_logger


Videos_dir = "Top_Videos"
if not os.path.exists(Videos_dir):
    os.makedirs(Videos_dir)

log_file_path = os.path.join(Videos_dir, "Top_Videos.log")
logger = get_logger('top_videos_logger', log_file_path)

logger.info("This will go ONLY to Top_Videos.log")

output_dir = "Top_Videos/video_content_images"
video_product_dir = "Top_Videos/video_product_images"
trend_dir = "Top_Videos/trend_images"
video_content_dir = "Top_Videos/video_content_videos"

os.makedirs(output_dir, exist_ok=True)
os.makedirs(video_product_dir, exist_ok=True)
os.makedirs(trend_dir, exist_ok=True)
os.makedirs(video_content_dir, exist_ok=True)

# GLOBALS TO TRACK COUNTERS ACROSS PAGES
image_counter = 1
logo_counter = 1
trend_counter = 1
rank_counter = 1


async def extract_video_data(page, page_num):
    global image_counter, logo_counter, trend_counter, rank_counter

    # Base index starts at 1 for page 1, 51 for page 2, 101 for page 3, etc.
    base_index = 1 + (page_num - 1) * 50
    image_counter = logo_counter = trend_counter = rank_counter = base_index

    # Continue with the rest of your scraping logic...
    logger.info(f"Scraping page {page_num}...")

    await page.wait_for_selector(".ant-table-row", timeout=10000)

    rows = await page.query_selector_all(".ant-table-row")
    logger.info(f"Loaded {len(rows)} videos")

    all_videos = []
    all_product_names = []
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.kalodata.com"
    }

    for index, row in enumerate(rows):
        # ✅ product Logo
        row_key = await row.get_attribute("data-row-key") or "N/A"
        product_el = await row.query_selector("div.Component-Image.cover.cover")
        product_image_filename = "N/A"
        if product_el:
            style = await product_el.get_attribute("style")
            match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
            if match:
                logo_url = match.group(1)
                try:
                    response = requests.get(logo_url, headers=headers)
                    if response.status_code == 200:
                        product_image_filename = f"product_logo_{row_key}.png"
                        with open(os.path.join(video_product_dir, product_image_filename), "wb") as f:
                            f.write(response.content)
                        logger.info(f"Saved product {product_image_filename}")
                        # logo_counter += 1
                    else:
                        logger.info(f"Failed to download product image (status {response.status_code})")
                except Exception as e:
                    logger.error(f"Error downloading product {logo_url}: {e}")

        # product Profile
        video_name_el = await row.query_selector("div.group-hover\\:text-primary")
        video_name = await video_name_el.inner_text() if video_name_el else "N/A"

        # product Name
        v_duration_el = await row.query_selector("div.text.truncate.text-\\[\\#999\\].text-\\[12px\\]")
        v_duration_text = await v_duration_el.inner_text() if v_duration_el else "N/A"

        product_name = None
        product_image_filename = None
        all_product_names = []
        product_price = None

        # For video
        image_divs = await row.query_selector_all("div.Component-Image.Layout-VideoCover.poster.Layout-VideoCover.poster")
        video_content_image = None

        for image_div in image_divs:
            # await image_div.hover()
            # await asyncio.sleep(3)
            # await asyncio.sleep(random.uniform(0.2, 0.5))
            await asyncio.sleep(.2)

            image_name = f"product_{row_key}_image_{image_counter}.png"
            image_path = os.path.join(output_dir, image_name)

            # Save thumbnail image
            style = await image_div.get_attribute("style")
            match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
            if match:
                url = match.group(1)
                try:
                    response = requests.get(url, headers=headers)
                    if response.status_code == 200:
                        with open(image_path, "wb") as f:
                            f.write(response.content)
                        video_content_image = image_name
                        if image_name not in all_product_names:
                            all_product_names.append(image_name)
                        logger.info(f"Saved {image_name}")
                        image_counter += 1
                    else:
                        logger.info(f"Failed to download image (status {response.status_code})")
                except Exception as e:
                    logger.error(f"Error downloading {url}: {e}")

            # Save video from file dialog
            # try:
            #     async with page.expect_download() as download_info:
            #         await page.click("text=Download Video (Without Watermark)")
            #     download = await download_info.value
            #     video_filename = image_name.replace(".png", ".mp4")
            #     video_path = os.path.join(video_content_dir, video_filename)
            #     await download.save_as(video_path)
            #     logger.info(f"Saved video: {video_filename}")
            # except Exception as e:
            #     logger.error(f"Error downloading video: {e}")
        


        td_elements = await row.query_selector_all("td")

        # Item Sold
        item_sold = await td_elements[3].inner_text() if len(td_elements) > 3 else "N/A"
        # Revenue
        rev_el = await row.query_selector("td.ant-table-cell.ant-table-column-sort")
        rev_text = await rev_el.inner_text() if rev_el else "N/A"
        # Average Unit Price
        views_num = await td_elements[6].inner_text() if len(td_elements) > 6 else "N/A"
        # GPM
        gpm = await td_elements[8].inner_text() if len(td_elements) > 8 else "N/A"
        # CPA
        adCPA = await td_elements[9].inner_text() if len(td_elements) > 9 else "N/A"
        # adViewRatio
        adViewRatio = await td_elements[10].inner_text() if len(td_elements) > 10 else "N/A"
        # adSpend
        adSpend = await td_elements[11].inner_text() if len(td_elements) > 11 else "N/A"
        # For Ad Roas
        adRoas = await td_elements[12].inner_text() if len(td_elements) > 12 else "N/A"

        # Revenue Trend Screenshot (5th td)
        revenue_trend_filename = "N/A"
        if len(td_elements) > 5:
            try:
                revenue_trend_filename = f"revenue_trend_{row_key}.png"
                await td_elements[5].screenshot(path=os.path.join(trend_dir, revenue_trend_filename))
                logger.info(f"Screenshot saved for revenue trend: {revenue_trend_filename}")
            except Exception as e:
                logger.error(f"Error capturing screenshot for revenue trend: {e}")

        if len(td_elements) > 7:
            try:
                views_trend_filename = f"views_trend_{row_key}.png"
                await td_elements[7].screenshot(path=os.path.join(trend_dir, views_trend_filename))
                logger.info(f"Screenshot saved for views trend: {views_trend_filename}")
            except Exception as e:
                logger.error(f"Error capturing screenshot for views trend: {e}")

    
        prod_image_div = await row.query_selector("div.Component-Image.cover.cover")
        best_seller_ids = []
        if prod_image_div:
            # await prod_image_div.hover()
            # await asyncio.sleep(.2)

                # Image URL
            style = await prod_image_div.get_attribute("style")
            match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
            if match:
                url = match.group(1)
                id_match = re.search(r'tiktok\.product/(\d+)/', url)
                product_id = id_match.group(1) if id_match else None

                logger.info(f"Extracted URL: {url}")
                logger.info(f"Extracted Product ID: {product_id}")
                try:
                    response = requests.get(url, headers=headers)
                    if response.status_code == 200:
                        image_name = f"product_{product_id}_image_{image_counter}.png"
                        image_path = os.path.join(output_dir, image_name)
                        with open(image_path, "wb") as f:
                            f.write(response.content)
                        product_image_filename = image_name
                        best_seller_ids.append(product_id)  # ✅ Save product ID
                        logger.info(f"Saved {image_name} with product ID {product_id}")
                        # logger.info(f"Saved {image_name}")
                        image_counter += 1
                    else:
                        logger.info(f"Failed to download image (status {response.status_code})")
                except Exception as e:
                    logger.error(f"Error downloading {url}: {e}")

        await row.hover()
        # await asyncio.sleep(random.uniform(0.2, 0.5))
        await asyncio.sleep(.1)

        # Product Names
        product_name = None
        product_element = await page.query_selector("span.line-clamp-2")
        product_name = await product_element.inner_text() if product_element else "N/A"
        if product_element:
            product_name_text = await product_element.inner_text()
            product_name = product_name_text
            # normalized_product_name = ' '.join(product_name.split())
            # if normalized_product_name not in all_product_names:
            #     all_product_names.append(normalized_product_name)
        
        product_price = None
        # Best Seller Prices (similarly, as shared pool)
        price_elements = await page.query_selector("div.text-\\[16px\\].min-w-\\[80px\\].h-\\[20px\\].font-medium.bg-white")
        if price_elements:
            price_text = await price_elements.inner_text()
            # normalized_price = ' '.join(price_text.split())
            # if normalized_price not in all_product_prices:
            product_price = price_text
        
        

        product_data = {
            "Row Key": row_key,
            "Rank": rank_counter,
            "Video Content": video_content_image,
            "Video Name": video_name,
            "Video Duration": v_duration_text,
            "Best Seller IDs": best_seller_ids,
            "Product Name": product_name,
            "Product Price": product_price,
            "Product Image": product_image_filename,
            "Item Sold": item_sold,
            "Revenue": rev_text,
            "Revenue Trend": revenue_trend_filename,
            "Views:": views_num,
            "Views Trend": views_trend_filename,
            "GPM": gpm,
            "Ad CPA": adCPA,
            "Ad View Ratio": adViewRatio,
            "Ad Spend": adSpend,
            "Ad Roas": adRoas,
        }

        all_videos.append(product_data)
        trend_counter += 1
        rank_counter += 1

        # Display results
        for i, shop in enumerate(all_videos):
            logger.info(f"\nVideo {i + 1}:")
            for k, v in shop.items():
                print(f"  {k}: {v if not isinstance(v, list) else ', '.join(v)}")        
    
    return all_videos

async def run_video_scraper(page, page_num):
    logger.info("Starting scraper...")
    try:
        if page_num == 1:
            logger.info("Clicking 'Video' link inside #page_header_left...")
            await page.click("#page_header_left >> text=Video & Ad")

            await page.click("span.ant-select-selection-item")
            await page.wait_for_selector("div.ant-select-item-option-content")
            options = await page.query_selector_all("div.ant-select-item-option-content")
            for option in options:
                text = await option.inner_text()
                if "50 / page" in text:
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
                            logger.error(f"Page {page_num} navigation failed: {e}")
                            return

                    page_results = await extract_video_data(page, page_num)
                    all_results.extend(page_results)

                    # Save progressively# Save progressively
                    allvideo = pd.DataFrame(page_results)
                    
                    
                    df = pd.DataFrame(allvideo)
                    df["Row Key"] = df["Row Key"].apply(lambda x: f"'{x}")
                    # Use a lock for file operations
                    async with asyncio.Lock():
                        df.to_csv("Top_Videos/top_videos_output.csv", mode="a", index=False, 
                                 header=not os.path.exists("Top_Videos/top_videos_output.csv"))
                        with open("Top_Videos/top_videos_output.json", "a", encoding="utf-8") as f:
                            json.dump(page_results, f, ensure_ascii=False, indent=4)# Use a lock for file operations
                    
                except Exception as e:
                    logger.error(f"Error processing page {page_num}: {e}")
                    if "TargetClosedError" in str(e):
                        raise  # Re-raise if page was closed
        
        await process_page(page_num)

        logger.info("Scraping complete!")
        logger.info(f"Total products scraped: {len(all_results)}")

    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        raise

    logger.info("Scraping complete!")
    logger.info(f"Total products scraped: {len(all_results)}")

async def video_main(page):
    for page_num in range(1,11):
        await run_video_scraper(page, page_num)  # You can loop this later if needed
