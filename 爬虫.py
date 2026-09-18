import asyncio
import aiohttp
import json
import time
import math
import os

API_URL = 'https://console.saas.aiwa.top/customer_frontend/flows/v1/workflows/run'
API_KEY = os.environ.get('DIFY_API_KEY', 'app-zgFhukbKBxtamEdo6xMPvaIR')
USER_TAG = 'ydm-fast'

PAGE_SIZE = 10
MAX_RETRY = 3
CONN_LIMIT = 100
CONN_LIMIT_PER_HOST = 60

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer ' + API_KEY,
}


async def call_workflow(session, action, page=1, keyword='', brand='', category='', price_changed='false'):
    body = {
        'inputs': {
            'action': action,
            'keyword': keyword,
            'brand': brand,
            'category': category,
            'priceChanged': price_changed,
            'page': page,
        },
        'response_mode': 'blocking',
        'user': USER_TAG + '-' + str(int(time.time() * 1000000)) + str(page),
    }

    retry = 0
    while retry < MAX_RETRY:
        try:
            async with session.post(API_URL, json=body, headers=headers, timeout=20) as resp:
                resp.raise_for_status()
                res_json = await resp.json()
                if res_json.get('data', {}).get('status') != 'succeeded':
                    raise RuntimeError(res_json.get('data', {}).get('error', 'unknown error'))
                outcome = res_json.get('data', {}).get('outputs', {}).get('result')
                if not outcome or not isinstance(outcome, dict):
                    raise RuntimeError('result格式不对')
                return outcome
        except Exception as err:
            retry += 1
            if retry >= MAX_RETRY:
                print('category=%s page=%s 最终失败: %s' % (category, page, err))
                return None
            await asyncio.sleep(0.2 * retry)


async def fetch_one_page(session, category, page):
    res = await call_workflow(session, 'search', page=page, category=category)
    if res is None:
        return category, page, [], 0
    return category, page, res.get('products', []), res.get('total', 0)


async def pull_brand_list(session):
    res = await call_workflow(session, 'brands')
    return res.get('brands', []) if res else []


async def pull_category_list(session):
    res = await call_workflow(session, 'categories')
    return res.get('categories', []) if res else []


async def fetch_category_all(session, cat_label, cat_value):
    first_page, page1, batch1, total = await fetch_one_page(session, cat_value, 1)
    page_map = {1: batch1}

    total_pages = math.ceil(total / PAGE_SIZE) if total else 0
    if total_pages > 1:
        remain_tasks = [fetch_one_page(session, cat_value, p) for p in range(2, total_pages + 1)]
        remain_res = await asyncio.gather(*remain_tasks)
        for _, p, batch, _ in remain_res:
            page_map[p] = batch

    merged = []
    for p in sorted(page_map.keys()):
        merged.extend(page_map[p])
    return cat_label, merged


async def pull_everything_fast(session):
    cat_list = await pull_category_list(session)

    tasks = [fetch_category_all(session, c.get('label', c.get('value')), c.get('value', '')) for c in cat_list]
    cat_results = await asyncio.gather(*tasks)

    final_map = {}
    for cat_label, merged in cat_results:
        final_map[cat_label] = merged
        print('分类 %s 抓取完成，共 %d 条' % (cat_label, len(merged)))

    return final_map, cat_list


async def main():
    start_ts = time.time()

    connector = aiohttp.TCPConnector(limit=CONN_LIMIT, limit_per_host=CONN_LIMIT_PER_HOST)
    async with aiohttp.ClientSession(connector=connector) as session:
        brand_list = await pull_brand_list(session)
        products_by_cat, cat_list = await pull_everything_fast(session)

    all_products = []
    for cat_label, prods in products_by_cat.items():
        all_products.extend(prods)

    # TODO: 数据量涨到几十万条以后这里应该考虑分批写文件，不然内存和json.dump会变慢
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(all_products, f, ensure_ascii=False)

    cost = time.time() - start_ts
    print('导出完成，共 %d 条，耗时 %.1f 秒' % (len(all_products), cost))


if __name__ == '__main__':
    asyncio.run(main())
