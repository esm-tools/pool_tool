import os
import csv
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

    def size(self):
        d = self.data
        return sum(i['fsize'] for i in d)

    def __len__(self):
        return len(self.data)


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

    def size(self):
        d = self.data
        return sum(row['fsize'] for items in d for row in items[1:])

    def __len__(self):
        d = self.data
        return sum(len(items[1:]) for items in d)


class Trees:
    def __init__(self, left, right):
        self.left = left
        self.right = right
        self.left_prefix = os.path.commonprefix([i['fname'] for i in left])
        self.right_prefix = os.path.commonprefix([i['fname'] for i in right])
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
    
    def common_dirs(self, left=True):
        by_dir = defaultdict(list)
        branch = (self.right, self.left)[left]
        for c in self.common_hashes:
            r = branch.by_checksum[c]
            by_dir[r['rparent']].append(r)
        return dict(by_dir)

    def mismatch_hashes(self):
        result = []
        basename = os.path.basename
        for cs in self.common_hashes:
            if basename(self.left.by_checksum[cs]['fname']) != basename(self.right.by_checksum[cs]['fname']):
                result.append(cs)
        return result

    def mismatch_filenames(self):
        hashes = self.mismatch_hashes()
        result = []
        for h in hashes:
            result.append((self.left.by_checksum[h]['fname'], self.right.by_checksum[h]['fname']))
        return result

    def synced_dirs(self, left=True):
        partial_sync = self.partially_synced_dirs(left=left)
        common_dirs = self.common_dirs(left=left)
        return list(set(common_dirs) - set(partial_sync))

    def unsynced_dirs(self, left=True):
        branch = (self.right_only, self.left_only)[left]
        return list(set(branch.by_dir) - set(self.partially_synced_dirs(left=left))) 

    def partially_synced_dirs(self, left=True):
        common_dirs = self.common_dirs(left=left)
        branch = (self.right_only, self.left_only)[left]
        return list(set(branch.by_dir) & set(common_dirs))

    def partially_synced(self, return_checksum=False):
        partial_dirs = self.partially_synced_dirs()
        result = []
        if return_checksum:
            func = lambda x: x['checksum']
        else:
            func = lambda x: x['fname']
        for d in partial_dirs:
            left_cs = self.left.by_dir[d]
            for row in left_cs:
                result.append(func(row))
        return result

    def unsynced_to_file(self, left=True, files=False):
        pjoin = os.path.join
        sep = os.path.sep
        filename = (("Right_only", "Left_only")[left], ("dirs", "files")[files] )
        filename = "_".join(filename)
        unsynced_dirs = sorted(self.unsynced_dirs(left=left),
                               key=lambda x: (len(x.split(sep)), x))
        prefix = (self.right_prefix, self.left_prefix)[left]
        if files:
            branch = (self.right_only, self.left_only)[left]
            for index, dirname in enumerate(unsynced_dirs):
                files = sorted(i['fname'] for i in branch.by_dir[dirname])
                files_str = "\n".join(files)
                fname = f"{filename}_{index:02d}.txt"
                print(f"creating file-listing: {fname}")
                with open(fname, "w") as fid:
                    fid.writelines(files_str)
                    fid.write("\n")
        else:
            filename = filename + ".txt"
            unsynced_dirs = list(map(lambda x: pjoin(prefix, x), unsynced_dirs))
            unsynced_dirs_str = "\n".join(unsynced_dirs)
            print(f"creating dir-listing: {filename}")
            with open(filename, "w") as fid:
                fid.writelines(unsynced_dirs_str)
                fid.write("\n")
        
    def common_to_file(self):
        L = self.left
        R = self.right
        dirname = os.path.dirname
        dir_map = set()
        file_map = list()
        for h in self.common_hashes:
            lf = L.by_checksum[h]['fname']
            ld = dirname(lf)
            rf = R.by_checksum[h]['fname']
            rd = dirname(rf)
            dir_map.add((ld,rd))
            file_map.append((lf, rf))
        dir_mapping = "\n".join([",".join(pair) for pair in dir_map])
        filename = "common_directory_mapping.txt"
        print(f"creating: {filename}")
        with open(filename, "w") as fid:
            fid.writelines(dir_mapping)
            fid.write("\n")
        file_mapping = "\n".join([",".join(pair) for pair in file_map])
        filename = "common_file_mapping.txt"
        print(f"creating: {filename}")
        with open(filename, "w") as fid:
            fid.writelines(file_mapping)
            fid.write("\n")

    def mismatch_filenames_to_file(self):
        data = self.mismatch_filenames()
        data_str = "\n".join([",".join(row) for row in data])
        filename = "mismatch_filenames.txt"
        print(f"creating: {filename}")
        with open(filename, "w") as fid:
            fid.writelines(data_str)
            fid.write("\n")

    def report(self):
        self.unsynced_to_file(left=True, files=False)
        self.unsynced_to_file(left=True, files=True)
        self.unsynced_to_file(left=False, files=False)
        self.unsynced_to_file(left=False, files=True)
        self.common_to_file()
        self.mismatch_filenames_to_file()

    def summary(self):
        import humanize
        left_dups_count = len(self.left_dups)
        right_dups_count = len(self.right_dups)
        left_files_count = len(self.left) + left_dups_count
        right_files_count = len(self.right) + right_dups_count
        left_dups_size = self.left_dups.size()
        right_dups_size = self.right_dups.size()
        left_files_size = self.left.size() + left_dups_size
        right_files_size = self.right.size() + right_dups_size
        synced_files = len(self.common_hashes)
        by_checksum = self.left.by_checksum
        synced_files_size = sum(by_checksum[i]['fsize'] for i in self.common_hashes)
        s = f"""
Left:
      Total files: {left_files_count} ({humanize.naturalsize(left_files_size)})
  Duplicate files: {left_dups_count}  ({humanize.naturalsize(left_dups_size)})

Right:
      Total files: {right_files_count} ({humanize.naturalsize(right_files_size)})
  Duplicate files: {right_dups_count}  ({humanize.naturalsize(right_dups_size)})

Common:
    synced files: {synced_files} ({humanize.naturalsize(synced_files_size)} bytes)
"""
        print(s)