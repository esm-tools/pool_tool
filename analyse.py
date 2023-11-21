import os
import csv
import click
from collections import defaultdict, Counter


def read_csv(filename):
    result = []
    with open(filename) as fid:
        reader = csv.DictReader(fid)
        for d in reader: 
            mtime = d.get('mtime')
            if mtime:
                d['mtime'] = float(mtime)
            fsize = d.get('fsize')
            if fsize:
                d['fsize'] = int(fsize)
            result.append(d)
    prefix = os.path.commonprefix([d['fname'] for d in result])
    prefix_size = len(prefix)
    parent = os.path.dirname
    for item in result:
        item['rpath'] = item['fname'][prefix_size:]
        item['rparent'] = parent(item['rpath'])
    return result


def drop_errors(records):
    result = []
    for rec in records:
        if rec['checksum'] == '-':
            print(f"Error: droping... {rec['fname']}")
            continue
        result.append(rec)
    return result


def duplicates(records):
    import operator
    dups = Counter()
    tmp_records = defaultdict(list)
    for row in records:
        cs = row['checksum']
        tmp_records[cs].append(row)
        dups[cs] += 1
    dups = [key for key, value in dups.items() if value > 1]
    duplicate_records = []
    for dup in dups:
        # sort records w.r.t mtime in ascending order, the oldest record is treated as original
        sorted_records = sorted(tmp_records[dup], key=operator.itemgetter('mtime'))
        tmp_records[dup] = [sorted_records[0],]
        #duplicate_records.extend(sorted_records[1:])
        duplicate_records.append(sorted_records)
    result = []
    for items in tmp_records.values():
        result.extend(items)
    return Records(result), Duplicates(duplicate_records)


class Records:
    def __init__(self, data):
        self.data = data
        self.by_checksum = None
        self.by_dir = None
        self.update()

    def update(self):
        data = self.data
        self.by_checksum = {i['checksum']: i for i in data}
        rparent = defaultdict(list)
        for i in data:
            rparent[i['rparent']].append(i)
        self.by_dir = dict(rparent)

    def dir_counts(self):
        return {k: len(v) for k, v in self.by_dir.items()}

    def __iter__(self):
        for item in self.data:
            yield item


class Duplicates:
    def __init__(self, data):
        self.data = data
        self.by_dir = None
        self.by_checksum = None
        self.update()
        
    def update(self):
        rparent = defaultdict(list)
        checksum = defaultdict(list)
        for entry in self.data:
            rparent[entry[0]['rparent']].append(entry)
            checksum[entry[0]['checksum']].append(entry)
        self.by_dir = dict(rparent)
        self.by_checksum = dict(checksum)


class Trees:
    def __init__(self, left, right):
        self.left = left
        self.right = right
        self.cleanup()

    def cleanup(self):
        self.left = drop_errors(self.left)
        self.right = drop_errors(self.right)
        self.left, self.left_dups = duplicates(self.left)
        self.right, self.right_dups = duplicates(self.right)

    def compare(self):
        left = self.left
        right = self.right
        left_hashes = set(left.by_checksum)
        right_hashes = set(right.by_checksum)
        common = left_hashes.intersection(right_hashes)
        self.left_only_hashes = left_only_hashes = left_hashes - common
        self.right_only_hashes = right_only_hashes = right_hashes - common
        left_only = [left.by_checksum[h] for h in left_only_hashes]
        right_only = [right.by_checksum[h] for h in right_only_hashes]
        self.common_hashes = list(common)
        self.left_only = Records(left_only)
        self.right_only = Records(right_only)
        
    def common_groupby_dirs(self, left=True):
        bydir = defaultdict(list)
        if left:
            root = self.left
        else:
            root = self.right
        for c in self.common_hashes:
            r = root.by_checksum[c]
            by_dir[r['rparent']].append(r)
        return dict(bydir)
    



def compare_tree(left, right):
    assert isinstance(left, Records), f"Must be an instance of 'Records'"
    left_hashes = set(left.by_checksum)
    right_hashes = set(right.by_checksum)
    common = left_hashes.intersection(right_hashes)
    left_only_hashes = left_hashes - common
    right_only_hashes = right_hashes - common
    left_only = [left.by_checksum[h] for h in left_only_hashes]
    right_only = [right.by_checksum[h] for h in right_only_hashes]
    return list(common), Records(left_only), Records(right_only)


def main(left, right):
    data_l = read_csv(left)
    data_l = drop_errors(data_l)
    data_l, dups_l = duplicates(data_l)
    data_r = read_csv(right)
    data_r = drop_errors(data_r)
    data_r, dups_r = duplicates(data_r)
    common, left_only, right_only = compare_tree(data_l, data_r)

