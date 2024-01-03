import csv
import time

import utils
import multiprocessing
manager = multiprocessing.Manager()

data = manager.list()

base_url = 'https://shopee.vn'


def crawl():
    url = f"{base_url}/api/v4/pages/get_category_tree"
    list_categories = utils.send_request(url)

    pool = multiprocessing.Pool()
    # for category in list_categories.json()['data']['category_list']:
    #     pool.apply_async(crawl_by_category, args=(category,))
    pool.starmap(crawl_by_category, [(category, data) for category in list_categories.json()['data']['category_list']])
    pool.close()
    pool.join()


def crawl_by_category(cat: dict, data):
    print(f"{'====================== ' if cat.get('level') == 1 else ''}crawl {cat.get('name')}")
    url = f"{base_url}/api/v4/recommend/recommend?bundle=category_landing_page&cat_level=1&catid={cat.get('catid')}&limit=60&offset=0"
    list_products = utils.send_request(url)
    for p in list_products.json()['data']['sections']:
        data += p['data']['item']
    if cat.get('children'):
        print(f"category {cat.get('name')} has {len(cat.get('children'))} children")
        for child in cat.get('children'):
            crawl_by_category(child, data)


if __name__ == '__main__':
    # test 1
    s = time.time()
    crawl()
    end = time.time()
    print(end - s)
    print("Số lượng sản phẩm lấy được", len(data))
    print("Số lượng sản phẩm lấy được trong một phút", len(data) / 60 / (end - s))
    with open('data.csv', 'w') as f:
        w = csv.DictWriter(f, data[0].keys())
        w.writeheader()
        for d in data:
            w.writerow(d)

    # test 2
    s = time.time()
    formatted_data = []
    for item in data:
        formatted_data.append(dict(
            product_name=item['name'],
            product_price=item['price'],
            product_rating=item['item_rating']['rating_star'],
            product_revenue=item['price'] * item['historical_sold'],
            product_url=base_url + '/' + item['name'].replace(' ', '-') + f'-i.{item["shopid"]}.{item["itemid"]}')
        )
    with open('formatted_data.csv', 'w') as f:
        w = csv.DictWriter(f, formatted_data[0].keys())
        w.writeheader()
        for d in formatted_data:
            w.writerow(d)
    end = time.time()
    print(end - s)
    print("Số lượng sản phẩm transform trong một phút", len(data) / 60 / (end - s))
