from collections import OrderedDict, defaultdict
from typing import List, Optional, Tuple


def regroup_and_print(groups: List[List[Optional[Tuple[str, str, str]]]], pattern: str, offset: int,
                      scaler: str = "/1000") -> None:
    """
    groups is a list of group elements
    a group element is a list of entries
    an entry can either be a tuple of (color, name, value) or None; the former yield a full bar,
      the latter a half bar width gap -> useful for subdivisions
    the output is grouped by color (each color can have a different amount of entries)
    each value in a color group is placed in a separate group of plot bars
    vary the offset to place independent groups
    """
    group_widths = []
    for group in groups:
        width = 0
        for val in group:
            if val is None:
                width += 0.5
            else:
                width += 1
        group_widths.append(width)
    if min(group_widths) != max(group_widths):
        raise AssertionError("All group must have the same size")
    groups_offset = -(group_widths[0] - 1) / 2
    color_offset = OrderedDict()
    color_group = defaultdict(lambda: [])
    for group in groups:
        col_count = defaultdict(lambda: 0)
        pos = groups_offset
        for val in group:
            if val is None:
                pos += 0.5
                continue

            color = val[0]
            col_count[color] += 1
            color += "_" + str(col_count[color])

            color_offset[color] = pos
            color_group[color].append(val)

            pos += 1

    for color, group_offset in color_offset.items():
        has_meta = bool(color_group[color][0][1])
        fields_suffix = ", meta index=2" if has_meta else ""

        print("\\data{{{}}}{{{}*\\mybarwidth}}{{{}}}"
              "{{x index=0, y expr=\\thisrowno{{1}}{}{}}}{{".format(color.rsplit("_", 1)[0], group_offset, pattern,
                                                                    scaler, fields_suffix))
        step = 1
        for (_, name, value) in color_group[color]:
            if name:
                name = " " + name
            print("    {} {}{}\\\\".format(step * 500 + offset, value, name))
            step += 1
        print("}")
    print("")
