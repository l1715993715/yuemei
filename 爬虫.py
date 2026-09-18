import asyncio
import aiohttp
import json
import time
import math
import os

DIFY_API_URL = 'https://console.saas.aiwa.top/customer_frontend/flows/v1/workflows/run'
DIFY_API_KEY = os.environ.get('DIFY_API_KEY', 'app-zgFhukbKBxtamEdo6xMPvaIR')
DIFY_USER_BASE = 'ydm-fast'

PAGE_SIZE = 10
MAX_RETRY = 3
CONN_LIMIT = 100
CONN_LIMIT_PER_HOST = 60

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer ' + DIFY_API_KEY,
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
        'user': DIFY_USER_BASE + '-' + str(int(time.time() * 1000000)) + str(page),
    }

    retry = 0
    while retry < MAX_RETRY:
        try:
            async with session.post(DIFY_API_URL, json=body, headers=headers, timeout=20) as resp:
                resp.raise_for_status()
                resp_json = await resp.json()
                if resp_json.get('data', {}).get('status') != 'succeeded':
                    raise RuntimeError(resp_json.get('data', {}).get('error', 'unknown error'))
                outcome = resp_json.get('data', {}).get('outputs', {}).get('result')
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


async def pull_everything_fast(session):
    cat_list = await pull_category_list(session)
    products_by_category = {c.get('label', c.get('value')): {} for c in cat_list}
    cat_total_map = {}

    print('先探测每个分类的总数...')
    probe_tasks = [fetch_one_page(session, c.get('value', ''), 1) for c in cat_list]
    probe_results = await asyncio.gather(*probe_tasks)

    for cat_info, (cat_value, page, batch, total) in zip(cat_list, probe_results):
        cat_label = cat_info.get('label', cat_info.get('value'))
        products_by_category[cat_label][1] = batch
        cat_total_map[cat_label] = (cat_value, total)

    all_tasks_meta = []
    for cat_label, (cat_value, total) in cat_total_map.items():
        total_pages = math.ceil(total / PAGE_SIZE) if total else 0
        for page in range(2, total_pages + 1):
            all_tasks_meta.append((cat_label, cat_value, page))

    print('剩余需要抓取的页数总计:', len(all_tasks_meta))

    # 之前用ThreadPoolExecutor起30个线程，现在直接一把all_tasks全丢进gather跑
    fetch_tasks = [fetch_one_page(session, cat_value, page) for cat_label, cat_value, page in all_tasks_meta]
    fetch_results = await asyncio.gather(*fetch_tasks)

    done_cnt = 0
    for (cat_label, cat_value, page), (_, _, batch, _) in zip(all_tasks_meta, fetch_results):
        products_by_category[cat_label][page] = batch
        done_cnt += 1
        if done_cnt % 50 == 0:
            print('已完成 %d / %d 页' % (done_cnt, len(all_tasks_meta)))

    final_result = {}
    for cat_label, page_map in products_by_category.items():
        merged = []
        for page in sorted(page_map.keys()):
            merged.extend(page_map[page])
        final_result[cat_label] = merged

    return final_result


async def main():
    start_ts = time.time()

    connector = aiohttp.TCPConnector(limit=CONN_LIMIT, limit_per_host=CONN_LIMIT_PER_HOST)
    async with aiohttp.ClientSession(connector=connector) as session:
        brand_list = await pull_brand_list(session)
        category_list = await pull_category_list(session)
        products_by_category = await pull_everything_fast(session)

    all_products = []
    for cat_label, products in products_by_category.items():
        all_products.extend(products)

    export_payload = {
        'brands': brand_list,
        'categories': category_list,
        'products_by_category': products_by_category,
        'all_products': all_products,
        'total_products': len(all_products),
    }

    with open('products_export_fast.json', 'w', encoding='utf-8') as f:
        json.dump(export_payload, f, ensure_ascii=False, indent=2)

    # TODO: 数据量涨到几十万条以后这里应该考虑分批写文件，不然内存和json.dump会变慢
    cost = time.time() - start_ts
    print('导出完成，共 %d 条，耗时 %.1f 秒' % (len(all_products), cost))


if __name__ == '__main__':
    await main()

