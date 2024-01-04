import json
import time
import urllib

import pandas as pd

base_url = 'https://shopee.vn'


def transform(r):
    try:
        return (r['name'],
                r['price'],
                json.loads(r['item_rating'].replace("'", '"'))['rating_star'],
                r['price'] * r['historical_sold'],
                base_url + '/' + urllib.parse.quote_plus(r['name'].replace(' ', '-') + f'-i.{r["shopid"]}.{r["itemid"]}'),
                r['itemid'], r['catid'])
    except Exception as e:
        print(r.to_dict())
        raise e


if __name__ == '__main__':
    # test 2
    df_data = pd.read_csv('data/data.csv', engine='pyarrow', dtype_backend='pyarrow')
    list_data = []
    with open('data/category.json', 'r') as f:
        data = json.loads(f.read())

    for d in data:
        children = d.pop('children')
        list_data.append(d)
        list_data += children

    df_category = pd.DataFrame.from_records(list_data)
    df_category.drop(axis=1, columns=['children'])

    print("khối lượng dữ liệu raw:", df_data.shape)
    s = time.time()
    df_data[['product_name', 'product_url', 'product_rating', 'product_price', 'product_revenue', 'itemid', 'catid']] = df_data.apply(
        transform, axis=1, result_type='expand')
    df_formatted_data = df_data[['product_name', 'product_price', 'product_rating', 'product_url', 'product_revenue']]
    df_formatted_data.to_csv('data/formatted_data.csv', index=False)
    end = time.time()
    print(end - s)
    print("Số lượng sản phẩm transform trong một phút", df_formatted_data.shape[0] / ((end - s) / 60))

    df_full_data = df_data[
        ['product_name', 'product_price', 'product_rating', 'product_url', 'product_revenue', 'itemid', 'catid',
         'crawl_catid']].copy()
    df_full_data = df_full_data.merge(df_category, how='left', left_on='crawl_catid', right_on='catid',
                                      suffixes=('', '_category'))

    count = 0
    for i, row in df_category[df_category['level'] == 1].iterrows():
        x = df_full_data[
            (df_full_data['catid_category'] == row['catid']) | (df_full_data['parent_catid'] == row['catid'])]
        count += 1
        print(f"{count}. Số lượng mặt hàng {row['display_name']} là:", x.shape[0], ". Số lượng không trùng:",
              x['itemid'].drop_duplicates().shape[0])
        if x['itemid'].drop_duplicates().shape[0] < 3000:
            print("----------------- WARNING ------------: số lượng mặt hàng không thỏa mãn yêu cầu đề bài")
