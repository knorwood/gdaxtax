#!env python
import collections
import datetime
import heapq
import pprint


class GdaxTransaction(object):

    def __init__(self, type, delta, balance, asset, id, dt):
        self.type = type
        self.delta = delta
        self.balance = balance
        self.asset = asset
        self.id = id
        self.datetime = dt

    @staticmethod
    def from_gdax_line(line):
        arr = line.split()
        return GdaxTransaction(
            arr[0], float(arr[4]), float(arr[6]), arr[7], arr[8],
            datetime.datetime.strptime('{} {}'.format(arr[1], arr[2]), '%Y-%m-%d %H:%M:%S')
        )

    def __str__(self):
        return '%s(type=%s, delta=%s, balance=%s, asset=%s, id=%s, dt=%s)' % (
            self.__class__.__name__, self.type, self.delta, self.balance, self.asset, self.id, self.datetime
        )

    def __repr__(self):
        return str(self)


def get_all_transactions_grouped(table_files):
    tables = {}
    for table_name in table_files:
        lines = open(table_name).readlines()
        tables[table_name] = {
            (t.type, t.id): t
            for t in [
            GdaxTransaction.from_gdax_line(l)
            for l in lines if l not in ['\n', '']
        ]
        }

    id_to_all = collections.defaultdict(list)
    for name, table in tables.iteritems():
        for k, txn in table.iteritems():
            id_to_all[txn.id].append(txn)

    dt_id_txns = []
    for t_id, txns in id_to_all.iteritems():
        first_dt = txns[0].datetime
        for txn in txns:
            # This is just some validation before grouping by (time, id)
            assert txn.datetime == first_dt
        dt_id_txns.append((first_dt, t_id, txns))

    dt_id_txns.sort()
    return dt_id_txns


class CostBasisRecord(object):

    def __init__(self, asset_type, rate, amount):
        self.asset_type = asset_type
        self.rate = rate
        self.amount = amount

    def combine(self, other_cbr):
        """
        Args:
            other_cbr (CostBasisRecord):
        Returns (CostBasisRecord):
        """
        assert self.asset_type == other_cbr.asset_type
        assert self.rate == other_cbr.rate
        return CostBasisRecord(self.asset_type, self.rate, self.amount + other_cbr.amount)


class Transaction(object):

    def __init__(self, source_asset, target_asset, source_amount, target_amount):
        self.source_asset = source_asset
        self.target_asset = target_asset
        self.source_amount = source_amount
        self.target_amount = target_amount


class Account(object):

    def __init__(self, asset_types):
        self.assets = {}
        self.cost_basis_heap = []
        for t in asset_types:
            self.assets[t] = {}

    def process_transaction(self, txn):
        """
        Args:
            txn (Transaction):
        """
        pass


def main():
    txn_groups = get_all_transactions_grouped([
        'table_{}'.format(asset)
        for asset in ['usd', 'eth', 'btc', 'bch', 'ltc']
    ])


    pp = pprint.PrettyPrinter()
    pp.pprint(txn_groups)

if __name__ == '__main__':
    main()
