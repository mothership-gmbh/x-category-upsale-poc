from pymongo import MongoClient
import numpy as np
from tqdm import tqdm

mongo_local_conn = MongoClient('mongodb://127.0.0.1')
mongo_local_coll = mongo_local_conn['ms_shopping_cart']['fake_orders_weighted_100K_replace_false']


# Uniform distr. = .0625
CATEGORIES = [
    'Trousers',
    'T-Shirts',
    'Jackets',
    'Boots',
    'Shoes',
    'Skirts',
    'Dresses',
    'Accessories',
    'Sweatshirts',
    'Backpacks',
    'Bags',
    'Underwear',
    'Shirts',
    'Swimwear',
    'High Heels',
    'Sportwear'
]

CAT_WEIGHTS = [
    .1,
    .15,
    .1,
    .05,
    .1,
    .01,
    .01,
    .05,
    .1,
    .12,
    .06,
    .08,
    .02,
    .01,
    .02,
    .02
]

NUM_ITEM_WEIGHTS = [
    .15,
    .2,
    .26,
    .15,
    .1,
    .05,
    .05,
    .02,
    .01,
    .01
]

for i in tqdm(range(0, 100000)):
    order = {'items': [], 'categories': [], 'order_id': i}

    for j in range(1, np.random.choice(np.arange(1, 11), p=NUM_ITEM_WEIGHTS)):
        selected_category = np.random.choice(np.arange(0, len(CATEGORIES)), p=CAT_WEIGHTS, replace=False)

        order['items'].append(
            {'category_id': selected_category, 'category_label': CATEGORIES[selected_category]}
        )

        order['categories'].append(selected_category)

    mongo_local_coll.insert_one(order)
