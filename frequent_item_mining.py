from collections import OrderedDict

import itertools
from pprint import pprint

from pymongo import MongoClient
from tqdm import tqdm

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


def product(l, k=1):
    product_result = []

    for elem_1 in l:
        for elem_2 in l:
            if elem_1 == elem_2:
                continue

            if k == 1:
                can = tuple(sorted((elem_1, elem_2)))

                if can not in product_result:
                    product_result.append(can)

            else:
                if elem_1[:k-1] != elem_2[:k-1]:
                    continue

                can = tuple(sorted(set(elem_1 + elem_2)))

                if can not in product_result:
                    product_result.append(can)

    return product_result


MIN_SUP = .01  # min sup 1%

mongo_local_conn = MongoClient('mongodb://127.0.0.1')
mongo_local_coll = mongo_local_conn['ms_shopping_cart']['fake_orders_weighted_2']

total_orders = mongo_local_coll.find().count()

if __name__ == '__main__':
    initial_items = mongo_local_coll.aggregate([
        {'$unwind': '$categories'},
        {'$group': {'_id': '$categories', 'support': {'$sum': 1}}},
        {'$sort': {'support': -1}}
    ])

    frequent_items = OrderedDict()

    for result in initial_items:
        if float(result['support']) / float(total_orders) >= MIN_SUP:
            frequent_items[int(result['support'])] = result['_id']

    support_map = {}

    for support, item in frequent_items.iteritems():
        support_map[frozenset([item])] = int(support)

    L = {}
    L[1] = frequent_items.values()

    k = 1
    while len(L[k]) > 0:
        print "Running for K=%s" % k
        L[k+1] = []

        candidates = product(L[k], k=k)

        if k > 1:
            del_candidates = []

            for candidate in candidates:
                for subset in itertools.combinations(candidate, k):
                    if subset not in L[k]:
                        del_candidates.append(candidate)

            for del_candidate in del_candidates:
                try:
                    candidates.remove(del_candidate)
                except ValueError:  # not sure how this can happen
                    pass

        for candidate in tqdm(candidates):
            if (len(set(candidate))) <= 1:
                continue

            # Select only orders with at least k items???
            # support = mongo_local_coll.find({'categories': {'$all': list(candidate)}, 'items.%s' % str(k+1): {'$exists': True}}).count()
            support = mongo_local_coll.find({'categories': {'$all': list(candidate)}}).count()

            if float(support) / float(total_orders) >= MIN_SUP:
                L[k+1].append(candidate)
                support_map[frozenset(candidate)] = int(support)

        k += 1

    print "Done"

    pprint(L)

    rules = []

    for x in range(k-1, 1, -1):
        print "Running at level K=%s" % x

        # Xs
        local_fis = L[x]

        for item in tqdm(local_fis):
            item = set(item)

            i = 1

            while x-i in L.keys():
                # Ys
                subset_candidates = L[x-i]

                for subset_candidate in subset_candidates:
                    if type(subset_candidate) is int:
                        subset_candidate = {subset_candidate}
                    else:
                        subset_candidate = set(subset_candidate)

                    if subset_candidate.issubset(item):
                        # Build rules: Y => (X - Y)
                        # Local rules: subset_candidate => (item - subset_candidate)
                        # Confidence Y => X = P(X|Y) = sup(Y u X) / sup(Y)
                        # Confidence P(subset_candidate|item)
                        diff = item - subset_candidate

                        # make sure subset is not empty
                        if len(diff):
                            conf = float(support_map[frozenset(item)]) / float(support_map[frozenset(subset_candidate)])

                            rules.append({
                                'rule': '%s => %s' % (subset_candidate, diff),
                                'conf': conf,
                                'rh': diff,
                                'lh': subset_candidate
                            })

                            if conf >= .3:
                                print "Found rule (Y => (X - Y)): %s => (%s - %s) | Diff: %s" % (subset_candidate, item, subset_candidate, diff)
                                print "Confidence for above rule: %s" % conf

                                for cand in subset_candidate:
                                    print "Left side: %s" % CATEGORIES[cand]
                                for d in diff:
                                    print "Right side: %s" % CATEGORIES[d]

                i += 1

                print "Found rules so far: %s" % len(rules)

    pprint(rules)
