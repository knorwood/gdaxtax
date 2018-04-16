#!env python
import collections
import datetime
import heapq
import pprint

PP = pprint.PrettyPrinter()

class TransactionTypes:
    DEPOSIT  = 'Deposit'
    MATCH = 'Match'
    FEE = 'Fee'

class AssetTypes:
    USD = 'USD'
    BTC = 'BTC'
    BCH = 'BCH'
    ETH = 'ETH'
    LTC = 'LTC'

class GdaxTransaction(object):

    def __init__(self, type, delta, balance, asset, id, dt):
        self.type = type
        self.delta = float(delta)
        self.balance = float(balance)
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


# class Transaction(object):
#
#     def __init__(self, source_asset, target_asset, source_amount, target_amount):
#         self.source_asset = source_asset
#         self.target_asset = target_asset
#         self.source_amount = source_amount
#         self.target_amount = target_amount


class Account(object):

    def __init__(self, asset_types):
        self.assets = collections.defaultdict(float)
        self.cost_basis_heaps = collections.defaultdict(list)
        self.tax_obligation = collections.defaultdict(float)

    def process_transaction(self, txn_data):
        """
        Args:
            txn_data ((datetime.datetime, str, list[GdaxTransaction])):
        """
        dt, t_id, txns = txn_data
        if len(txns) == 1:
            transaction = txns[0]
            assert transaction.type == TransactionTypes.DEPOSIT
            self.assets[transaction.asset] += transaction.delta
        elif self.is_usd_transaction(txns):
            usd_t, cry_t, fee_t = self.organize_usd_crytpo_fee(txns)
            curr_rate = abs(usd_t.delta / cry_t.delta)
            # print 'Kurt:', curr_rate, '{}/{}\n\t'.format(usd_t.asset, cry_t.asset), usd_t, '\n\t', cry_t, '\n\t', fee_t
            if cry_t.asset == 'LTC':
                pass
            if usd_t.delta < 0:
                # This was buying crypto
                self.assets[cry_t.asset] += cry_t.delta
                self.assets[usd_t.asset] += usd_t.delta
                heapq.heappush(self.cost_basis_heaps[cry_t.asset], (-1 * curr_rate, cry_t.delta))
            elif usd_t.delta > 0:
                # This was selling crypto
                self.assets[cry_t.asset] += cry_t.delta
                self.assets[usd_t.asset] += usd_t.delta
                remainder_old = -1 * cry_t.delta
                while remainder_old > 0:
                    if cry_t.asset == 'LTC':
                        pass
                    try:
                        neg_rate, amount = heapq.heappop(self.cost_basis_heaps[cry_t.asset])
                    except:
                        print "ERR:", cry_t.asset, remainder_old
                        break
                        # if cry_t.asset == AssetTypes.BCH:
                        #     break
                        # else:
                        #     raise
                    if remainder_old >= amount:
                        # This will exhaust this cost basis record
                        self.tax_obligation[cry_t.asset] += amount * (curr_rate - abs(neg_rate))
                        remainder_old -= amount
                    else:
                        # We'll need to put some cost basis back into heap
                        self.tax_obligation[cry_t.asset] += remainder_old * (curr_rate - abs(neg_rate))
                        amount -= remainder_old
                        heapq.heappush(self.cost_basis_heaps[cry_t.asset], (neg_rate, amount))
                        remainder_old = 0
            #print '\tASSETS:', self.assets
        else:
            from_txn, to_txn, fee = self.organize_from_to_fee(txns)
            print "Transfering {} {} -> {} {}".format(-1 * from_txn.delta, from_txn.asset, to_txn.delta, to_txn.asset)
            self.assets[from_txn.asset] += from_txn.delta
            self.assets[to_txn.asset] += to_txn.delta
            remainder_old = -1 * from_txn.delta
            remainder_new = to_txn.delta
            from_per_to_rate = abs(from_txn.delta / to_txn.delta)
            while remainder_old > 0:
                neg_rate, amount = heapq.heappop(self.cost_basis_heaps[from_txn.asset])
                if remainder_old >= amount:
                    transfer_to_new = amount / remainder_old * remainder_new
                    remainder_new -= transfer_to_new
                    heapq.heappush(
                        self.cost_basis_heaps[to_txn.asset],
                        (neg_rate * from_per_to_rate, transfer_to_new)
                    )
                    remainder_old -= amount
                else:
                    heapq.heappush(
                        self.cost_basis_heaps[to_txn.asset],
                        (neg_rate * from_per_to_rate, remainder_new)
                    )
                    amount -= remainder_old
                    heapq.heappush(self.cost_basis_heaps[from_txn.asset], (neg_rate, amount))
                    remainder_old = 0
        for asset, value in self.assets.iteritems():
            assert value >= 0, 'Asset %s must be non-negative' % asset

    @staticmethod
    def is_usd_transaction(txn_group):
        """
        Args:
            txn_group (list[GdaxTransaction]):
        Returns (bool):
        """
        for txn in txn_group:
            if txn.asset == 'USD':
                return True
        return False

    @staticmethod
    def organize_usd_crytpo_fee(txn_group):
        """
        Args:
            txn_group (list[GdaxTransaction]):
        Returns ((GdaxTransaction, GdaxTransaction, GdaxTransaction)):
        """
        usd_txn = None
        crypto_txn = None
        fee = None
        for txn in txn_group:
            if txn.type == TransactionTypes.MATCH:
                if txn.asset == AssetTypes.USD:
                    usd_txn = txn
                else:
                    crypto_txn = txn
            elif txn.type == TransactionTypes.FEE:
                fee = txn
            else:
                raise Exception('Cant identify transaction %s' % txn)
        assert usd_txn is not None
        assert crypto_txn is not None
        return usd_txn, crypto_txn, fee

    @staticmethod
    def organize_from_to_fee(txn_group):
        """
        Args:
            txn_group (list[GdaxTransaction]):
        Returns ((GdaxTransaction, GdaxTransaction, GdaxTransaction)):
        """
        from_txn = None
        to_txn = None
        fee = None
        for txn in txn_group:
            if txn.type == TransactionTypes.MATCH:
                if txn.delta < 0:
                    from_txn = txn
                else:
                    to_txn = txn
            elif txn.type == TransactionTypes.FEE:
                fee = txn
            else:
                raise Exception('Cant identify transaction %s' % txn)
        assert from_txn is not None
        assert to_txn is not None
        return from_txn, to_txn, fee


def main():
    txn_group_data = get_all_transactions_grouped([
        'table_{}'.format(asset)
        for asset in ['usd', 'eth', 'btc', 'bch', 'ltc']
    ])

    #PP.pprint(txn_groups)
    #PP.pprint([g for g in txn_groups if is_usd_transaction(g[2])])

    gdax_account = Account(['USD', 'BTC', 'ETH', 'BCH', 'LTC'])
    for txn_data in txn_group_data:
        gdax_account.process_transaction(txn_data)

    #print gdax_account.cost_basis_heaps['ETH']
    total_ob = 0
    for asset, oblig in gdax_account.tax_obligation.iteritems():
        print asset, oblig, .32 * oblig
        total_ob += oblig

    print "ALL", total_ob, .32 * total_ob
    print gdax_account.assets
    print gdax_account.cost_basis_heaps

if __name__ == '__main__':
    main()
