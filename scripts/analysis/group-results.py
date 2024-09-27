#!/usr/bin/env python3
import errno
import os
import re
import sys
from collections import OrderedDict


def printerr(*objects):
    print(*objects, file=sys.stderr)


def get_result_dirs(foldername):
    for root, dirs, files in os.walk(foldername):
        return dirs


def remove_empty_dirs(foldername):
    for dirname in get_result_dirs(foldername):
        try:
            os.rmdir(dirname)
        except OSError as e:
            if e.errno == errno.ENOTEMPTY:
                pass
            else:
                raise


# FIXME detailed warning
def is_colliding_groups(groups):
    for group in groups:
        for other_group in groups:
            if group == other_group:
                continue
            for part in group.keys():
                if part in other_group:
                    return True
    return False


def is_matching_part(part, folder):
    return not re.search("(^|-)" + part + "(-|$)", folder) is None


# FIXME detailed warning
def is_partitioning_group(group, result_dirs):
    for folder in result_dirs:
        matches = 0
        for part in group.keys():
            if is_matching_part(part, folder):
                matches += 1
        if matches == 0:
            print('Folder {}, group {}'.format(folder, group))
            return False
    return True


def move_folders(target_parts, groups, result_dirs, src_folder, target_folder):
    if len(groups) > 0:
        result_dirs = result_dirs.copy()
        for (part, dst_part) in groups[0].items():
            part_result_dirs = [d for d in result_dirs if is_matching_part(part, d)]
            for p in part_result_dirs:
                result_dirs.remove(p)
            move_folders(target_parts + [dst_part], groups[1:], part_result_dirs,
                         src_folder, target_folder)
        return

    if len(result_dirs) == 0:
        return

    target_folder = os.path.join(target_folder, "-".join(target_parts))
    os.makedirs(target_folder)
    for folder in result_dirs:
        src = os.path.join(src_folder, folder)
        dst = os.path.join(target_folder, folder)
        os.rename(src, dst)


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Group results')
    parser.add_argument('source', type=str, help='source folder')
    parser.add_argument('dest', type=str, help='dest folder')
    parser.add_argument('prefix', type=str, help='dest subfolder prefix')
    parser.add_argument('group', type=str, nargs='+', help='group: part1,part2 or part1,part2=dest1,dest2')

    args = parser.parse_args()

    groups = []
    for group in args.group:
        if "=" in group:
            parts = OrderedDict(zip(*[g.split(",") for g in group.split("=")]))
        else:
            parts = OrderedDict([(p, p) for p in group.split(",")])
        groups.append(parts)

    remove_empty_dirs(args.source)
    result_dirs = get_result_dirs(args.source)
    if is_colliding_groups(groups):
        printerr("Colliding groups")
        sys.exit(1)
    for group in groups:
        if not is_partitioning_group(group, result_dirs):
            printerr("Non partitioning group")
            sys.exit(1)
    move_folders([args.prefix], groups, result_dirs, args.source, args.dest)


if __name__ == '__main__':
    main()
