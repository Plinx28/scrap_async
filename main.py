from bs4 import BeautifulSoup
import csv, json
import datetime
import time
import asyncio 
import aiohttp

start_time = time.time()

books_data = []

async def get_page_data(session, page):
    url = f'https://www.labirint.ru/books/?order=date&way=back&display=table&available=1&price_min=&price_max=&page={page}'

    async with session.get(url=url) as response:
        response_text = await response.text()

        soup = BeautifulSoup(response_text, 'lxml')

        books_items = soup.find('tbody', class_="products-table__body").find_all('tr')

        for book in books_items:
            book_info = book.find_all('td')
            if book_info[0].text.strip() == '':
                continue

            try:
                book_title = book_info[0].find('a').text.strip()
            except:
                book_title = 'No title'

            try:
                book_author = book_info[1].find('a').text.strip()
            except:
                book_author = 'Author not specified'

            try:
                book_publishing = book_info[2].find_all('a')
                book_publishing = ":".join([bp.text for bp in book_publishing])
            except:
                book_publishing = 'No publishing'

            book_current_price = int(book_info[3].find('span', class_='price-val').find('span').text.replace(' ', ''))

            try:
                book_old_price = int(book_info[3].find('span', class_='price-old').text.replace(' ',''))
            except:
                book_old_price = 'No discount'

            try:
                book_discount = round(float((book_old_price - book_current_price) / book_old_price) * 100) 
            except: 
                book_discount = 0

            books_data.append({
                'title': book_title,
                'author': book_author,
                'publishing': book_publishing,
                'price': book_current_price,
                'old_price': book_old_price,
                'discount': book_discount
            })

        print(f'Page processed {page}')


async def gather_data():
    url = 'https://www.labirint.ru/books/?order=date&way=back&display=table&available=1&price_min=&price_max=&page=1'

    async with aiohttp.ClientSession() as session:
        response = await session.get(url=url)
        soup = BeautifulSoup(await response.text(), 'lxml')

        pagination_number = int(soup.find('div', class_='pagination-number').find_all('a')[-1].text)

        tasks = []
 
        for page in range(1, pagination_number):
            task = asyncio.create_task(get_page_data(session, page))
            tasks.append(task)

        await asyncio.gather(*tasks)


def main():
    asyncio.run(gather_data())

    cur_time = datetime.datetime.now().strftime('%d_%m_%Y_%H_%M')

    with open(f'data/labirint_{cur_time}_async.json', 'w', encoding='utf-8') as file:
        json.dump(books_data, file, indent=4, ensure_ascii=False)

    with open(f'data/labirint_{cur_time}_async.csv', 'w', encoding='utf-8') as file:
        writer = csv.writer(file, delimiter='\t')

        writer.writerow(('Title', 'Author', 'Publishig', 'Price', 'Old price', 'Discount'))    

    for book in books_data:
        with open(f'data/labirint_{cur_time}_async.csv', 'a', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter='\t')

            writer.writerow((
                book['title'],
                book['author'],
                book['publishing'],
                book['price'],
                book['old_price'],
                book['discount']
            ))

    finish_time = round(time.time() - start_time)
    print(f'Total time: {finish_time} second')


if __name__ == '__main__':
    main()