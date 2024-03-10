import asyncio
from pyppeteer import launch
from pyppeteer.errors import PageError
from urllib.parse import unquote
import mysql.connector
import pathes


async def get_element_text(page, xpath):
    try:
        return await page.evaluate('''(xpath) => {
            const element = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
            return element ? element.textContent : '';
        }''', xpath)
    except Exception as e:
        print(e)
        return False


async def get_element(review_block, xpath):
    try:
        name = await review_block.evaluate(f'(element) => {{ return element.querySelector("{xpath}").textContent; }}')
        return name.strip() if name else ''
    except Exception as e:
        print(f"Ошибка при получении текста элемента: {e}")
        return ''


async def element_click(page, xpath):
    try:
        result = await page.evaluate('''(xpath) => {
            const element = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
            if (element) {
                element.click();
                return true;
            }
            return false;
        }''', xpath)
        return result
    except Exception as e:
        print(f"Ошибка при клике по элементу: {e}")
        return False


async def el_click(page, path):
    try:
        element = await page.waitForXPath(path)
        await element.click()
        return True
    except Exception as e:
        return False


async def save_reviews_content(page):
    reviews_content = []
    await page.waitForXPath(pathes.scroll)
    full_elements = await page.xpath(pathes.scroll)

    review_blocks = await page.xpath(
        '//div[starts-with(@class, "_11gvyqv")]')
    total_count = len(review_blocks)

    last_count = 0
    last_review_block_index = 0
    while True:
        temp = 0
        review_blocks = await page.xpath(
            '//div[starts-with(@class, "_11gvyqv")]')

        for idx, review_block in enumerate(review_blocks[last_review_block_index:], start=1):
            temp += 1
            await page.evaluate('(element) => element.scrollIntoView()', review_block)
            await asyncio.sleep(0.1)
            try:
                name_element = await review_block.querySelector('span[class^="_16s5yj36"]')
                name = await page.evaluate('(element) => element.getAttribute("title")', name_element)
            except Exception as e:
                name = ''
            try:
                date_element = await review_block.querySelector('div[class="_4mwq3d"]')
                date = await page.evaluate('(element) => element.textContent.trim().split(",")[0]', date_element)
            except Exception as e:
                date = ''
            try:
                review_text_element = await review_block.querySelector('div[class="_49x36f"] > a')
                review_text = await page.evaluate('(element) => element.textContent.trim()', review_text_element)
            except Exception as e:
                review_text = ''

            ratings = {
                '50px': 5,
                '40px': 4,
                '30px': 3,
                '20px': 2,
                '10px': 1
            }

            rating_elements = await review_block.xpath(
                './/div[@class="_1fkin5c"]')
            rating = 0
            for element in rating_elements:
                width = await page.evaluate('(element) => getComputedStyle(element).width', element)
                rating += ratings.get(width, 0)

            reviews_content.append({'Имя': name, 'Отзыв': review_text, 'Оценка': rating, 'Дата': date})

            last_review_block_index += 1

        if temp == last_count:
            prev_height = await page.evaluate('(element) => element.scrollHeight', full_elements[
                0])
            # eshkere
            await page.evaluate('(element) => element.scrollTo(0, element.scrollHeight)',
                                full_elements[0])
            await asyncio.sleep(0.5)
            new_height = await page.evaluate('(element) => element.scrollHeight', full_elements[
                0])
            if new_height == prev_height:
                break
            else:
                last_count = temp
        else:
            last_count = temp

    return reviews_content, total_count


async def main():
    search_query = 'Вкусно и точка'
    url = f'https://2gis.ru/ufa/search/{search_query}'
    browser = await launch(
        {'executablePath': 'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe', 'headless': False})

    page = await browser.newPage()
    await page.goto(url)
    await element_click(page, pathes.cookie_banner)
    await asyncio.sleep(0.5)
    count_all_items = await get_element_text(page, pathes.items_count)
    pages = round(int(count_all_items) / 12 + 0.5)
    for _ in range(pages):
        main_block = await page.waitForXPath(pathes.main_block)
        items = await page.xpath(f'{pathes.main_block}/div')
        count_items = len(items)
        for item in range(1, count_items + 1):
            elements = await main_block.xpath(f'div[{item}]')
            if elements:
                class_attribute = await page.evaluate('(element) => element.getAttribute("class")', elements[0])
                if class_attribute:
                    continue
            element_xpath = f'/html/body/div[2]/div/div/div[1]/div[1]/div[2]/div[1]/div/div[2]/div/div/div/div[2]/div[2]/div[1]/div/div/div/div[2]/div/div[{item}]'
            element = await page.waitForXPath(element_xpath)
            await page.evaluate('(element) => { element.scrollIntoView(); }', element)
            await el_click(page, element_xpath)
            await asyncio.sleep(0.5)
            try:
                address = await get_element_text(page, './/span[@class="_er2xx9"]/a')
                address = address.strip()
            except PageError:
                address = None
            try:
                phone_number = await get_element_text(page, '//div[@class="_b0ke8"]/a')
                phone_number = phone_number.replace('tel:', '')
            except PageError:
                phone_number = 'None'
            if not await element_click(page, pathes.btnreviews1):
                await element_click(page, pathes.btnreviews2)
            await asyncio.sleep(2)
            try:
                place_rating = await get_element_text(page, '//div[@class="_10fd7sv"]')
            except PageError:
                place_rating = 'None'
            count_rating = await get_element_text(page, pathes.count_rating1) or await get_element_text(page,
                                                                                                        pathes.count_rating2)
            link = unquote(page.url)
            reviews_content, _ = await save_reviews_content(page)
            await save_to_database(reviews_content, link, address, place_rating, phone_number, count_rating)
        await page.evaluate('window.scrollTo(0, document.body.scrollHeight);')
        await element_click(page, pathes.next_page_btn)
        await asyncio.sleep(0.5)
    await browser.close()


async def save_to_database(data, link, address, place_rating, phone_number, count_rating):
    conn = mysql.connector.connect(
        database="parsing",
        user="root",
        password="1234",
        host="127.0.0.1",
        port="3306"
    )
    cursor = conn.cursor()

    try:
        for review in data:
            check_place_query = """
            SELECT place_id FROM places WHERE link = %s
            """
            check_place_values = (link,)
            cursor.execute(check_place_query, check_place_values)
            place_record = cursor.fetchone()

            if place_record:
                place_id = place_record[0]
            else:
                add_place_query = """
                INSERT INTO places (link, address, place_rating, phone_number, count_rating)
                VALUES (%s, %s, %s, %s, %s)
                """
                add_place_values = (link, address, place_rating, phone_number, count_rating)
                cursor.execute(add_place_query, add_place_values)
                place_id = cursor.lastrowid

            add_or_update_review_query = """
            INSERT INTO reviews (place_id, reviews_text, rating, date, user_name)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE reviews_text=VALUES(reviews_text), rating=VALUES(rating), date=VALUES(date), user_name=VALUES(user_name)
            """
            add_review_values = (place_id, review['Отзыв'], review['Оценка'], review['Дата'], review['Имя'])
            cursor.execute(add_or_update_review_query, add_review_values)

        conn.commit()
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
