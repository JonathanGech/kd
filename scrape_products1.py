import asyncio
import pandas as pd
import json
import os
import re
import requests
import random
from p_logging import get_logger

Product_dir = "Top_Products"
if not os.path.exists(Product_dir):
    os.makedirs(Product_dir)

log_file_path = os.path.join(Product_dir, "Top_Products.log")
logger = get_logger('top_products_logger', log_file_path)

logger.info("This will go ONLY to Top_Products.log")


output_dir = "Top_Products/best_selling_products_images"
logo_dir = "Top_Products/product_logo"
trend_dir = "Top_Products/trend_images"
highest_revenue_dir = "Top_Products/highest_revenue_videos"

os.makedirs(output_dir, exist_ok=True)
os.makedirs(logo_dir, exist_ok=True)
os.makedirs(trend_dir, exist_ok=True)
os.makedirs(highest_revenue_dir, exist_ok=True)

# GLOBALS TO TRACK COUNTERS ACROSS PAGES
image_counter = 1
logo_counter = 1
trend_counter = 1
rank_counter = 1

async def extract_product_data(page, page_num):
    global image_counter, logo_counter, trend_counter, rank_counter

    # Base index starts at 1 for page 1, 51 for page 2, 101 for page 3, etc.
    base_index = 1 + (page_num - 1) * 10
    image_counter = logo_counter = trend_counter = rank_counter = base_index

    # Continue with the rest of your scraping logic...
    logger.info(f"Scraping page {page_num}...")

    await page.wait_for_selector(".ant-table-row.ant-table-row-level-0", timeout=15000)
    rows = await page.query_selector_all(".ant-table-row.ant-table-row-level-0")
    logger.info(f"Loaded {len(rows)} products")

    all_products = []
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.kalodata.com"
    }

    for index, row in enumerate(rows):
        product_logo_filename = "N/A"
        row_key = await row.get_attribute("data-row-key") or "N/A"
        logo_el = await row.query_selector("div.Component-Image.w-\\[72px\\].h-\\[72px\\].w-\\[72px\\].h-\\[72px\\]")
        if logo_el:
            style = await logo_el.get_attribute("style")
            match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
            if match:
                logo_url = match.group(1)
                try:
                    response = requests.get(logo_url, headers=headers)
                    if response.status_code == 200:
                        product_logo_filename = f"product_logo_{row_key}.png"
                        with open(os.path.join(logo_dir, product_logo_filename), "wb") as f:
                            f.write(response.content)
                        logo_counter += 1
                        logger.info(f"Downloaded logo for product {index + 1} as {product_logo_filename}")
                except Exception as e:
                    logger.error(f"Failed to download logo for product {index + 1}: {e}")

        product_name_el = await row.query_selector("div.line-clamp-2:not(.text-\\[13px\\]):not(.font-medium)")
        product_name = await product_name_el.inner_text() if product_name_el else "N/A"

        p_range_el = await row.query_selector("div.text-\\[13px\\].font-medium.line-clamp-2")
        p_range_text = await p_range_el.inner_text() if p_range_el else "N/A"

        image_divs = await row.query_selector_all("div.Component-Image.Layout-VideoCover.cover.Layout-VideoCover.cover")
        best_seller_images = []
        highest_revenue_videos = []
        best_seller_ids = []
        for image_div in image_divs:
            # await image_div.hover()
            # await asyncio.sleep(1)
            # await asyncio.sleep(random.uniform(0.2, 0.5))
            # await asyncio.sleep(0.2)
            

            # max_retries = 1
            # retry_count = 0
            # video_downloaded = False
            # while retry_count < max_retries and not video_downloaded:
            #     try:
            #         async with page.expect_download() as download_info:
            #             await page.click("text=Download Video (Without Watermark)")
            #             await page.wait_for_selector("div.Layout-VideoDownloading", timeout=10000)
            #             await asyncio.sleep(1.5)
            #             # Check for error message
            #             # error_element = await page.query_selector("div.ant-message-custom-content.ant-message-error")
            #             dl_failed_element = await page.query_selector("div.ant-message-notice-content")
            #             if dl_failed_element:
            #                 # error_text = await error_element.inner_text()
            #                 # Your download limit has been reached. Please go to the pricing page to purchase the recharge plan.
            #                 # if "Your download limit has been reached" in error_text:
            #                 logger.warning(f"Skipping video download: {index+1}")
            #                 retry_count += 1
            #                 break 
                        
            #         download = await download_info.value
            #         video_filename = image_name.replace(".png", ".mp4")
            #         video_path = os.path.join(highest_revenue_dir, video_filename)
            #         await download.save_as(video_path)
            #         highest_revenue_videos.append(video_filename)
            #         video_downloaded = True
            #         logger.info(f"Downloaded video {video_filename}")
            #     except Exception as e:
            #         retry_count += 1
            #         logger.error(f"Failed to download video after {retry_count} retries: {e}")
            #         await asyncio.sleep(1)
            # if not video_downloaded:
            #     logger.warning(f"Video failed to download after {max_retries} retries for image {image_name}. Proceeding to image download.")
            style = await image_div.get_attribute("style")
            match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
            image_name = "N/A"
            if match:
                url = match.group(1)
                id_match = re.search(r'tiktok\.video/(\d+)/', url)
                product_id = id_match.group(1) if id_match else None
                best_seller_ids.append(product_id) # ✅ Save product ID
                try:
                    response = requests.get(url, headers=headers)
                    if response.status_code == 200:
                        image_name = f"video_{product_id}_image_{image_counter}.png"
                        image_path = os.path.join(output_dir, image_name)
                        with open(image_path, "wb") as f:
                            f.write(response.content)
                        # best_seller_ids.append(product_id)  # ✅ Save product ID
                        best_seller_images.append(image_name)
                        logger.info(f"Saved {image_name} with product ID {product_id}")       
                        image_counter += 1
                        logger.info(f"Downloaded image {image_name}")
                except Exception as e:
                    logger.error(f"Failed to download image {image_name}: {e}")
            
            try:
                await image_div.click()
                await page.wait_for_selector("#videoplayer video", timeout=10000)  # Use ID selector

                video_element = await page.query_selector("#videoplayer video")
                video_url = await video_element.get_attribute("src") if video_element else None

                if video_url:
                    video_filename = f"video_{product_id}.mp4"
                    video_path = os.path.join(highest_revenue_dir, video_filename)

                    response = requests.get(video_url, headers=headers)
                    if response.status_code == 200:
                        with open(video_path, "wb") as f:
                            f.write(response.content)
                        highest_revenue_videos.append(video_filename)
                        logger.info(f"Downloaded video {video_filename}")
                    else:
                        logger.warning(f"Video URL responded with status {response.status_code}")
                else:
                    logger.warning("No video URL found in #videoplayer")
            except Exception as e:
                logger.error(f"Error during video extraction from image_div: {e}")
            finally:
                try:
                    close_button = await page.query_selector(
                        "//div[contains(@class, 'w-[40px]') and contains(@class, 'h-[40px]') "
                        "and contains(@class, 'rounded-full') and contains(@class, 'bg-[#999]')]"
                    )
                    if close_button:
                        await close_button.click()
                        await asyncio.sleep(0.2)
                    else:
                        logger.warning("Close button not found after video download or failure")
                except Exception as e:
                    logger.error(f"Error trying to close the video modal: {e}")

        await product_name_el.hover()
        await asyncio.sleep(0.2)
        # await asyncio.sleep(random.uniform(0.2, 0.5))

        td_elements = await row.query_selector_all("td")
        rev_el = await row.query_selector("td.ant-table-cell.ant-table-column-sort")
        rev_text = await rev_el.inner_text() if rev_el else "N/A"
        item_sold = await td_elements[4].inner_text() if len(td_elements) > 4 else "N/A"
        avg_unit_price = await td_elements[5].inner_text() if len(td_elements) > 5 else "N/A"
        commission_rate = await td_elements[6].inner_text() if len(td_elements) > 6 else "N/A"
        creator_number = await td_elements[8].inner_text() if len(td_elements) > 8 else "N/A"
        launch_date = await td_elements[9].inner_text() if len(td_elements) > 9 else "N/A"
        creator_conversion_rate = await td_elements[10].inner_text() if len(td_elements) > 10 else "N/A"

        revenue_trend_filename = "N/A"
        if len(td_elements) > 3:
            try:
                revenue_trend_filename = f"revenue_trend_{row_key}.png"
                await td_elements[3].screenshot(path=os.path.join(trend_dir, revenue_trend_filename))
                # trend_counter += 1
                logger.info(f"Captured revenue trend screenshot {revenue_trend_filename}")
            except Exception as e:
                logger.error(f"Failed to capture revenue trend: {e}")

        product_data = {
            "Row Key": row_key,
            "Rank": rank_counter,
            "Product Logo": product_logo_filename,
            "Product Name": product_name,
            "Product Range": p_range_text,
            "Revenue": rev_text,
            "Revenue Trend": revenue_trend_filename,
            "Item Sold": item_sold,
            "Average Unit Price": avg_unit_price,
            "Commission Rate": commission_rate,
            "Highest Revenue Videos IDs": best_seller_ids,
            "Highest Revenue Videos": highest_revenue_videos,
            "Highest Revenue Videos Images": best_seller_images,
            "Creator Number": creator_number,
            "Launch Date": launch_date,
            "Creator Conversion Rate": creator_conversion_rate
        }

        all_products.append(product_data)
        trend_counter += 1
        rank_counter += 1

    return all_products

async def run_product_scraper(page, page_num):
    logger.info("Starting scraper...")
    try:
        # if page_num == 1 or page_num == 4 or page_num == 7:
        if page_num == 1:
            await page.click("#page_header_left >> text=Product")

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

        await asyncio.sleep(2)

        all_results = []
        semaphore = asyncio.Semaphore(5)  # Limit concurrent page processing
        

        async def process_page(page_num):
            nonlocal all_results
            async with semaphore:
                try:
                    # if page_num > 1:
                    #     selector = f'li.ant-pagination-item >> a[rel="nofollow"]:has-text("{page_num}")'
                    #     try:
                    #         await page.click(selector)
                    #         await asyncio.sleep(3)
                    #     except Exception as e:
                    #         logger.error(f"Page {page_num} navigation failed: {e}")
                    #         return
                    if page_num > 1:
                        selector = f'li.ant-pagination-item >> a[rel="nofollow"]:has-text("{page_num}")'
                        # if page_num == 4:
                        #     selector = f'li.ant-pagination-item >> a[rel="nofollow"]:has-text("5")'
                        #     await page.click(selector)
                        #     await asyncio.sleep(3)
                        #     selector = f'li.ant-pagination-item >> a[rel="nofollow"]:has-text("{page_num}")'
                        # elif page_num == 7:
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
                            else:
                                more = await page.query_selector("span.ant-pagination-item-ellipsis")
                                await page.click(more)
                                
                        except Exception as e:
                            logger.error(f"Page {page_num} navigation failed: {e}")
                            await page.click("span.ant-pagination-item-ellipsis")
                            await page.click(selector)
                            await asyncio.sleep(3)
                            return

                    page_results = await extract_product_data(page, page_num)
                    all_results.extend(page_results)

                    # Save progressively
                    df = pd.DataFrame(page_results)
                    df["Row Key"] = df["Row Key"].apply(lambda x: f"'{x}")
                    df["Highest Revenue Videos"] = df["Highest Revenue Videos"].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
                    df["Highest Revenue Videos Images"] = df["Highest Revenue Videos Images"].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
                    
                    # Use a lock for file operations
                    async with asyncio.Lock():
                        df.to_csv("Top_Products/top_products_output.csv", mode="a", index=False, 
                                 header=not os.path.exists("Top_Products/top_products_output.csv"))
                        with open("Top_Products/top_products_output.json", "a", encoding="utf-8") as f:
                            json.dump(page_results, f, ensure_ascii=False, indent=4)

                except Exception as e:
                    logger.error(f"Error processing page {page_num}: {e}")
                    if "TargetClosedError" in str(e):
                        raise  # Re-raise if page was closed

        # # Create tasks for all pages but process them with limited concurrency
        # tasks = [process_page(page_num) for page_num in range(1, 11)]
        # await asyncio.gather(*tasks, return_exceptions=True)
        # for page_num in range(1, 11):
        #     await process_page(page_num)
        await process_page(page_num)

        logger.info("Scraping complete!")
        logger.info(f"Total products scraped: {len(all_results)}")

    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        raise

    logger.info("Scraping complete!")
    logger.info(f"Total products scraped: {len(all_results)}")

    # ant-message-custom-content ant-message-error
    # Your download limit has been reached. Please go to the pricing page to purchase the recharge plan.

async def product_main(page):
    for page_num in range(1,2):
        await run_product_scraper(page, page_num)  # You can loop this later if needed
    # for page_num in page_range:
    #     await run_product_scraper(page, page_num)  # You can loop this later if needed
    await page.close()

    # text-base-666 text-[14px] text-center