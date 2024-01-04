import json
import time
import urllib

import pandas as pd

base_url = 'https://shopee.vn'


def transform(r):
    try:
        # trả về danh sách theo định dạng yêu cầu
        return (r['name'],
                r['price'],
                json.loads(r['item_rating'].replace("'", '"'))['rating_star'],
                r['price'] * r['historical_sold'],
                base_url + '/' + urllib.parse.quote_plus(
                    r['name'].replace(' ', '-') + f'-i.{r["shopid"]}.{r["itemid"]}'),
                r['itemid'], r['catid'])
    except Exception as e:
        print(r.to_dict())
        raise e


if __name__ == '__main__':
    # test 2
    list_data = []

    # chuyển file json các nhóm mặt hàng sang dạng dataframe
    with open('data/category.json', 'r') as f:
        data = json.loads(f.read())

    for d in data:
        children = d.pop('children')
        list_data.append(d)
        list_data += children

    df_category = pd.DataFrame.from_records(list_data)
    df_category.drop(axis=1, columns=['children'])

    # đọc dữ liệu raw
    df_data = pd.read_csv('data/data.csv', engine='pyarrow', dtype_backend='pyarrow')
    print("khối lượng dữ liệu raw:", df_data.shape)

    # thực hiện transform
    list_field = ['product_name', 'product_price', 'product_rating', 'product_revenue', 'product_url']
    s = time.time()
    # thực hiện lưu lại itemid và catid để phục vụ việc kiểm tra trùng lặp
    df_data[list_field + ['itemid', 'catid']] = df_data.apply(transform, axis=1, result_type='expand')
    df_transformed_data = df_data[list_field]

    # lưu lại vào file csv như yêu cầu
    df_transformed_data['product_name'] = df_transformed_data['product_name'].apply(lambda _x: repr(_x))
    df_transformed_data.to_csv('data/transformed_data.csv', index=False)
    df_transformed_data.to_excel('data/transformed_data_excel.xlsx', index=False)
    end = time.time()
    print(end - s)
    print("============> Số lượng sản phẩm transform trong một phút", df_transformed_data.shape[0] / ((end - s) / 60))

    # thực hiện một vài thống kê để kiểm tra dữ liệu
    df_data_with_cat = df_data[list_field + ['itemid', 'catid', 'crawl_catid', 'shopid']].copy()
    df_data_with_cat = df_data_with_cat.merge(df_category, how='left', left_on='crawl_catid', right_on='catid',
                                              suffixes=('', '_category'))

    # đếm số lượng sản phẩm trong mỗi nhóm mặt hàng lớn
    count = 0
    not_duplicate_data_count = 0
    for i, row in df_category[df_category['level'] == 1].iterrows():
        x = df_data_with_cat[
            (df_data_with_cat['catid_category'] == row['catid']) | (df_data_with_cat['parent_catid'] == row['catid'])]
        count += 1
        x_not_duplicate = x[['itemid', 'crawl_catid']].drop_duplicates()

        print(f"{count}. Số lượng mặt hàng {row['display_name']} là:", x.shape[0], ". Số lượng không trùng:",
              x_not_duplicate.shape[0])
        if x_not_duplicate.shape[0] < 3000:
            print("----------------- WARNING ------------: số lượng mặt hàng không thỏa mãn yêu cầu đề bài")
        not_duplicate_data_count += x_not_duplicate.shape[0]

    print("tổng số lượng dữ liệu bị trùng:", df_data.shape[0] - not_duplicate_data_count)
