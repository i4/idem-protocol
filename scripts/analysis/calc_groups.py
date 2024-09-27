#!/usr/local/bin/python3


def get_permutations_helper(k, n, start, depth, parts, results):
    if depth >= k:
        results.append(parts)
        return results
    for i in range(start, n):
        get_permutations_helper(k, n, i + 1, depth + 1, parts + [i], results)
    return results


def gen_permutations(k, n):
    return get_permutations_helper(k, n, 0, 0, [], [])


def check_constellation(groups, weights, limits):
    used_limit = [0] * len(limits)
    for i in range(len(weights)):
        for p in groups[i]:
            used_limit[p] += weights[i]
            if used_limit[p] > limits[p]:
                return False
    return True


def group_value(group):
    # replicas with execution but with agreement group
    non_collocation_costs = 0.15
    non_collocation = 3
    for g in group:
        if g < 3:
            non_collocation -= 1
    return 1 / (1 + non_collocation_costs * non_collocation)
    # collocation = count of replicas with agreement group and execution
    # col_lookup = [1, 1.11, 1.27, 1.54]
    # return col_lookup[collocation]


def groups_value(groups, weights):
    s = 0
    for i in range(len(weights)):
        if weights[i] > 0:
            s += weights[i] * group_value(groups[i])
    return s


def solve_rec(groups, limits, remaining_group_count, part_weights, results):
    for i in range(len(part_weights), len(groups)):
        weight = 1
        while remaining_group_count > 0:
            next_part = part_weights + [weight]
            if not check_constellation(groups, next_part, limits):
                break
            if not solve_rec(groups, limits, remaining_group_count - 1,
                    next_part, results):
                break
            weight += 1
        part_weights.append(0)

    if check_constellation(groups, part_weights, limits):
        cur_value = groups_value(groups, results[0]) if len(results) else 0
        my_value = groups_value(groups, part_weights)
        if my_value > cur_value:
            while len(results):
                results.pop()
        if my_value >= cur_value:
            results.append(part_weights)
        return True
    return False


def solve(groups, limits, group_count):
    results = []
    solve_rec(groups, limits, group_count, [], results)
    return results


def main():
    # agreement-execution ration for readwrite-0: about 66%
    # performance gain of collocation
    # 44287/39925 = 1,10925485284909204759
    # 44287/34852 = 1,27071617123837943303
    # 44287/28676 = 1,54439252336448598131
    # [12] * 2 + [8] * 3 -> exec: 5, 2 groups
    # -> [(3, [0, 1, 2, 3]), (4, [0, 1, 3, 4])], groupweights: 12, 10
    # [12] * 2 + [4] * 4 -> exec: 4, 3 groups
    # -> [(2, [0, 1, 3, 4]), (2, [0, 1, 3, 5]), (2, [0, 1, 4, 5])], groupweights: 10, 10, 10
    executor_costs = 5
    limits = [15] * 2 + [10] * 3
    group_count = 2
    for i in range(0, 3):
        limits[i] -= executor_costs
        if limits[i] < 0:
            raise ValueError(str(limits))
    groups = gen_permutations(4, len(limits))
    solutions = solve(groups, limits, group_count)
    if len(solutions):
        print("Value: {}".format(groups_value(groups, solutions[0])))
    else:
        print("No solutions")

    for solution in solutions:
        print([(solution[i], groups[i]) for i in range(len(solution)) if solution[i] > 0])


if __name__ == '__main__':
    main()
