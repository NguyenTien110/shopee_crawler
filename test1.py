import csv
import time
import json

import utils
import multiprocessing

manager = multiprocessing.Manager()

list_data = manager.list()

base_url = 'https://shopee.vn'
size_crawl = 60


def crawl():
    url = f"{base_url}/api/v4/pages/get_category_tree"
    list_categories = utils.send_request(url)
    with open('data/category.json', 'w', encoding='utf-8') as fc:
        json.dump(list_categories.json()['data']['category_list'], fc, ensure_ascii=False, indent=4)

    pool = multiprocessing.Pool()
    print("Số processes:", pool._processes)
    pool.starmap(crawl_by_category, [(category, list_data) for category in list_categories.json()['data']['category_list']])
    pool.close()
    pool.join()


def crawl_by_category(cat: dict, data: list, offset=0):
    print(f"{'====================== ' if cat.get('level') == 1 else ''}crawl {cat.get('name')}-{cat.get('catid')} from offset {offset}")
    url = f"{base_url}/api/v4/recommend/recommend?bundle=category_landing_page&cat_level={cat.get('level')}&catid={cat.get('catid')}&limit={size_crawl}&offset={offset}"
    list_products = utils.send_request(url)
    for p in list_products.json()['data']['sections']:
        try:
            # item có thể bằng null nên phải kiểm tra trước khi thêm
            if p['data']['item']:
                data += [{**it, **{'crawl_catid': cat.get('catid')}} for it in p['data']['item']]
                # chỉ chạy crawl tiếp ở offset = 0 (trang đầu)
                if offset == 0:
                    for i in range(int(p['total'] / size_crawl)):
                        crawl_by_category(cat, data, offset + (i + 1) * size_crawl)

                    if cat.get('children'):
                        print(f"category {cat.get('name')} has {len(cat.get('children'))} children")
                        for child in cat.get('children'):
                            crawl_by_category(child, data)
        except Exception as error:
            raise error


if __name__ == '__main__':
    # test 1
    s = time.time()
    crawl()
    end = time.time()
    print(end - s)
    print("Số lượng sản phẩm lấy được", len(list_data))
    print("Số lượng sản phẩm lấy được trong một phút", len(list_data) / ((end - s) / 60))
    with open('data/data.csv', 'w') as f:
        w = csv.DictWriter(f, list_data[0].keys())
        w.writeheader()
        for d in list_data:
            w.writerow(d)

