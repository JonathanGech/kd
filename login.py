# main.py (your original script)

import asyncio
from playwright.async_api import async_playwright
# from playwright_stealth import stealth_async
from config import ConfigManager
from scrape_shops1 import shop_main
# from scrape_shops import extract_shop_data
from scrape_creators1 import creator_main
from scrape_products1 import product_main
from scrape_videos1 import video_main
from scrape_videos import extract_video_data
from scrape_category1 import category_main
from scrape_live1 import live_main

config_manager = ConfigManager()
url = config_manager.url
email = config_manager.email
password = config_manager.password

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        # await stealth_async(page)

        print("Opening Kalodata...")
        await page.goto("https://www.kalodata.com/")

        print("Clicking 'Log in'...")
        await page.click("text=Log in")

        print("Log-in using credentials...")
        # Click the country code dropdown
        await page.wait_for_selector("span.ant-input-group-addon", timeout=10000)
        await page.click("span.ant-input-group-addon")

        # Wait for the country code dropdown to appear
        await page.wait_for_selector(".select-wrapper", timeout=10000)

        # Find all select-wrapper elements
        country_elements = await page.query_selector_all(".select-wrapper")
        for el in country_elements:
            text_content = await el.inner_text()
            if "US +1" in text_content:
                await el.click()
                break

        print("Entering email...")
        await page.wait_for_selector("#register_phone", timeout=10000)
        await page.fill("#register_phone", email)

        print("Entering password...")
        await page.wait_for_selector("#register_password", timeout=10000)
        await page.fill("#register_password", password)


        print("Clicking 'Log in' button...")
        await page.wait_for_selector("button:text('Log in')", timeout=10000)
        await page.click("button:text('Log in')")

        print("Login successful!")

        # Scrape Shop Data
        print("Navigating to shop data...")
        # await shop_main(page)

        # Scrape Creator Data

        print("Navigating to creator data...")
        # await run_creator_scraper(page, page_num)
        # await creator_main(page)

        # Scrape Product Data
        print("Navigating to product data...")
        await product_main(page)
        
        # Scrape Video Data
        print("Navigating to video data...")
        # await extract_video_data(page)
        # await video_main(page)

        # Scrape Category Data
        print("Navigating to category data...")
        # await category_main(page)

        # Scrape Live Data
        print("Navigating to live data...")
        # await live_main(page)

        await browser.close()
        await asyncio.sleep(3)

# for page_num in range(1, 11):
    # Run the script
    # asyncio.run(run(page_num))
asyncio.run(run())

# ant-message-custom-content ant-message-error