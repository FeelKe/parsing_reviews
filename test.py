from __future__ import annotations

import time
from time import sleep
from urllib.parse import unquote

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
import mysql.connector
import pathes

from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains

TABLE_COLUMNS = ['Название', 'Имя', 'Отзыв', 'Оценка', 'Адрес', 'Ссылка']
TABLE = {column: [] for column in TABLE_COLUMNS}


def get_element_text(driver: WebDriver, path: str) -> str:
    try:
        return driver.find_element(By.XPATH, path).text
    except NoSuchElementException:
        return ''


def move_to_element(driver: WebDriver, element: WebElement | WebDriver) -> None:
    try:
        webdriver.ActionChains(driver).move_to_element(element).perform()
    except StaleElementReferenceException:
        pass


def element_click(driver: WebDriver | WebElement, path: str) -> bool:
    try:
        driver.find_element(By.XPATH, path).click()
        return True
    except:
        return False


def save_reviews_content(driver):
    reviews_content = []
    full_element = driver.find_element(By.XPATH, pathes.scroll)

    review_blocks = driver.find_elements(By.XPATH, '//div[starts-with(@class, "_11gvyqv")]')
    total_count = len(review_blocks)

    last_count = 0
    last_review_block_index = 0
    while True:
        temp = 0
        review_blocks = driver.find_elements(By.XPATH, '//div[starts-with(@class, "_11gvyqv")]')

        for idx, review_block in enumerate(review_blocks[last_review_block_index:], start=1):
            temp += 1
            try:
                name_element = review_block.find_element(By.XPATH, './/span[starts-with(@class, "_16s5yj36")]')
                name = name_element.get_attribute('title')
            except NoSuchElementException:
                name = ''

            try:
                date_element = review_block.find_element(By.XPATH, './/div[@class="_4mwq3d"]')
                date = date_element.text.strip().split(',')[0]
            except NoSuchElementException:
                date = ''
            try:
                review_text_element = review_block.find_element(By.XPATH, './/div[@class="_49x36f"]/a')
                review_text = review_text_element.text.strip()
            except NoSuchElementException:
                review_text = ''

            ratings = {
                '50px': 5,
                '40px': 4,
                '30px': 3,
                '20px': 2,
                '10px': 1
            }

            rating_elements = review_block.find_elements(By.XPATH, './/div[@class="_1fkin5c"]')
            rating = 0
            for element in rating_elements:
                width = element.get_attribute('style').split('width: ')[1].split(';')[0]
                rating += ratings.get(width, 0)

            reviews_content.append({'Имя': name, 'Отзыв': review_text, 'Оценка': rating, 'Дата': date})

            actions = ActionChains(driver)
            actions.move_to_element(review_block)
            time.sleep(0.1)
            actions.perform()

            last_review_block_index += 1

        if temp == last_count:
            prev_height = driver.execute_script("return arguments[0].scrollHeight;", full_element)
            driver.execute_script("arguments[0].scrollTo(0, arguments[0].scrollHeight);", full_element)
            time.sleep(0.5)
            new_height = driver.execute_script("return arguments[0].scrollHeight;", full_element)
            if new_height == prev_height:
                break
            else:
                last_count = temp
        else:
            last_count = temp

    return reviews_content, total_count


def main():
    search_query = 'Вкусно и точка'
    url = f'https://2gis.ru/ufa/search/{search_query}'
    driver = webdriver.Edge()
    driver.maximize_window()
    driver.get(url)
    element_click(driver, pathes.main_banner)
    element_click(driver, pathes.cookie_banner)
    sleep(0.5)
    count_all_items = int(get_element_text(driver, pathes.items_count))
    pages = round(count_all_items / 12 + 0.5)
    for _ in range(pages):
        main_block = driver.find_element(By.XPATH, pathes.main_block)
        count_items = len(main_block.find_elements(By.XPATH, 'div'))
        for item in range(1, count_items + 1):
            if main_block.find_element(By.XPATH, f'div[{item}]').get_attribute('class'):
                continue
            element_click(driver,
                          f'/html/body/div[2]/div/div/div[1]/div[1]/div[2]/div[1]/div/div[2]/div/div/div/div[2]/div[2]/div[1]/div/div/div/div[2]/div/div[{item}]')
            sleep(0.5)
            try:
                address_element = driver.find_element(By.XPATH, './/span[@class="_er2xx9"]/a')
                address = address_element.text.strip()
            except NoSuchElementException:
                address = None
            if not element_click(driver, pathes.btnreviews1):
                element_click(driver, pathes.btnreviews2)
            sleep(2)
            place_rating = get_element_text(driver, pathes.place_rating)
            move_to_element(driver, main_block)
            link = unquote(driver.current_url)
            reviews_content, _ = save_reviews_content(driver)
            save_to_database(reviews_content, link, address, place_rating)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        element_click(driver, pathes.next_page_btn)
        sleep(0.5)
    driver.quit()


def save_to_database(data, link, address, place_rating):
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
                INSERT INTO places (link, address, rating, number)
                VALUES (%s, %s, %s, %s)
                """
                add_place_values = (link, address, place_rating, None)
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
    main()
