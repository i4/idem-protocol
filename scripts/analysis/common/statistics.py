import math


def median(value_list):
    sorted_list = sorted(value_list)
    list_len = len(sorted_list)
    if list_len == 0:
        raise AttributeError()
    elif list_len % 2 == 0:
        return (sorted_list[list_len // 2 - 1] + sorted_list[list_len // 2]) * 0.5
    else:
        return sorted_list[list_len // 2]


def percentile(value_list, pct):
    # 90 percentile -> pct = 0.9
    sorted_list = sorted(value_list)
    list_len = len(sorted_list)
    if list_len == 0:
        return -1
    else:
        return sorted_list[math.ceil(list_len * pct) - 1]


def average(value_list):
    return sum(value_list) / len(value_list)


def stddev(value_list):
    avg = average(value_list)
    var = [(i - avg) * (i - avg) for i in value_list]
    return math.sqrt(sum(var) / (len(var) - 1))
