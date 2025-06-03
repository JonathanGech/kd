# import asyncio
# from playwright.async_api import async_playwright
# # from playwright_stealth import stealth_async
# from config import ConfigManager
# from scrape_shops2 import shop_main

# config_manager = ConfigManager()
# url = config_manager.url
# email = config_manager.email
# password = config_manager.password

# async def login_kalodata(page):
#     print("Opening Kalodata...")
#     await page.goto("https://www.kalodata.com/")
#     await page.click("text=Log in")

#     print("Logging in...")
#     await page.wait_for_selector("span.ant-input-group-addon", timeout=10000)
#     await page.click("span.ant-input-group-addon")
#     await page.wait_for_selector(".select-wrapper", timeout=10000)

#     country_elements = await page.query_selector_all(".select-wrapper")
#     for el in country_elements:
#         text_content = await el.inner_text()
#         if "US +1" in text_content:
#             await el.click()
#             break

#     await page.wait_for_selector("#register_phone", timeout=10000)
#     await page.fill("#register_phone", email)
#     await page.wait_for_selector("#register_password", timeout=10000)
#     await page.fill("#register_password", password)
#     await page.wait_for_selector("button:text('Log in')", timeout=10000)
#     await page.click("button:text('Log in')")

#     print("Login successful on this page!")

# async def run():
#     async with async_playwright() as p:
#         browser = await p.chromium.launch(headless=False)
#         # context = await browser.new_context()

#         # Create 3 tabs (pages)
#         page1 = await browser.new_page()
#         # await stealth_async(page1)
#         page2 = await browser.new_page()
#         # await stealth_async(page2)
#         page3 = await browser.new_page()
#         # await stealth_async(page3)
#         # page4 = await browser.new_page()
#         # page5 = await browser.new_page()

#         # Log in on all pages
#         await asyncio.gather(
#             login_kalodata(page1),
#             login_kalodata(page2),
#             login_kalodata(page3)
#             # login_kalodata(page4),
#             # login_kalodata(page5)
#         )

#         # Run shop_main on each page in parallel
#         await asyncio.gather(
#             shop_main(page1, range(1, 4)),  # Pages 1–3
#             shop_main(page2, range(4, 7)),   # Pages 4–6
#             shop_main(page3, range(7, 11)) # Pages 7–9
#             # shop_main(page4, range(7, 9)),   # Pages 10–12
#             # shop_main(page5, range(9, 11))    # Pages 13–15
#         )

#         await browser.close()
#         await asyncio.sleep(10)

# asyncio.run(run())
import asyncio
import random
from playwright.async_api import async_playwright
from config import ConfigManager
from scrape_shops2 import shop_main
from scrape_videos1 import video_main
from scrape_category1 import category_main
from scrape_creators1 import creator_main
from scrape_products1 import product_main
from scrape_live1 import live_main

config_manager = ConfigManager()
url = config_manager.url
email = config_manager.email
password = config_manager.password

# SCRAPE_FUNCTIONS = [shop_main, video_main, category_main, creator_main, product_main, live_main]
# SCRAPE_FUNCTIONS = [category_main, creator_main]
SCRAPE_FUNCTIONS = [video_main, live_main, product_main]
PAGE_RANGES = [range(1, 4), range(4, 7), range(7, 11)]
# PAGE_RANGES = [range(1, 3), range(3, 5), range(5, 7), range(7, 9), range(9, 11)]

async def login_kalodata(page):
    print("Opening Kalodata...")
    await page.goto("https://www.kalodata.com/")
    await page.click("text=Log in")

    print("Logging in...")
    await page.wait_for_selector("span.ant-input-group-addon", timeout=10000)
    await page.click("span.ant-input-group-addon")
    await page.wait_for_selector(".select-wrapper", timeout=10000)

    country_elements = await page.query_selector_all(".select-wrapper")
    for el in country_elements:
        text_content = await el.inner_text()
        if "US +1" in text_content:
            await el.click()
            break

    await page.wait_for_selector("#register_phone", timeout=10000)
    await page.fill("#register_phone", email)
    await page.wait_for_selector("#register_password", timeout=10000)
    await page.fill("#register_password", password)
    await page.wait_for_selector("button:text('Log in')", timeout=10000)
    await page.click("button:text('Log in')")

    # await page.click("div.btn.mb-\\[16px\\]", timeout=10000)
    # # await page.wait_for_selector("button:text('OK')", timeout=10000)
    # # await page.click("button:text('OK')")
    # print("Login successful on this page!")
    
async def open_tab_and_scrape(browser, page_range, scrape_callback):
    page = await browser.new_page()
    await login_kalodata(page)
    await scrape_callback(page, page_range)

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)

        # Define ranges for each page/tab
        # tasks = [
        #     (range(1, 4)),
        #     (range(4, 7)),
        #     (range(7, 11))
        #     # Add more ranges if needed
        # ]

        for scrape_fn in SCRAPE_FUNCTIONS:
            for page_range in PAGE_RANGES:
                asyncio.create_task(open_tab_and_scrape(browser, page_range, scrape_fn))
                delay = random.randint(180, 300)  # 5 to 7 minutes in seconds
                print(f"Waiting {delay // 60} minutes before opening the next tab...")
                await asyncio.sleep(delay)
            # await asyncio.sleep(120)

        # Wait enough time for all scraping to complete
        await asyncio.sleep(3600)  # Adjust as needed based on expected total run time
        await browser.close()

asyncio.run(run())

# btn mb-[16px]