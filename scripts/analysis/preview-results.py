#!/usr/bin/env python3

import os
import re
import subprocess
import threading

SVG_MODELINE = "term svg enhanced dynamic mouse standalone"


# import matplotlib.pyplot as plt
#
# def main():
#     bundles = load_pickle_logs()
#     fig, ax = plt.subplots()
#
#     for bundle in bundles:
#         logs = select_logtype(bundle, "client-averages")
#         for log in logs:
#             tp, lat = log.extract(["throughput_average", "latency_average"])
#             ax.plot(tp, lat, marker='x')
# 
#     ax.set_xlim(xmin=0)
#     ax.set_ylim(ymin=0)
#     fig.savefig("avg.svg")


def main():
    averages = list_files(r"log-avgs-.+\.txt")
    throughput = list_files(r"log-throughput-.+\.txt")

    commands = [
        lambda: generate_xyplot("graphs-average.svg", averages, 2, 5, 4, 7),
        lambda: generate_xyplot("graphs-median.svg", averages, 3, 6, 4, 7)
    ]

    if throughput:
        commands += [
            lambda: generate_lineplot("graphs-throughput.svg", throughput, 1, 2),
            lambda: generate_lineplot("graphs-latency.svg", throughput, 1, 3)
        ]

    if any(["client-1" in fn for fn in averages]):
        for cl in ("client-0", "client-1", "client-2", "client-3"):
            commands += [
                lambda cl=cl: generate_lineplot("graphs-throughput-{}.svg".format(cl),
                                                [fn for fn in throughput if cl in fn], 1, 2),
                lambda cl=cl: generate_lineplot("graphs-latency-{}.svg".format(cl),
                                                [fn for fn in throughput if cl in fn], 1, 3)
            ]

    run_tasks(commands)


def list_files(fn_pattern):
    regexp = re.compile(fn_pattern)
    return [f for f in os.listdir(".") if os.path.isfile(f) and regexp.match(f)]


def run_tasks(commands) -> None:
    threads = [threading.Thread(target=cmd) for cmd in commands]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def generate_xyplot(outputfn, data_files, data_x, data_y, error_x, error_y, modeline=SVG_MODELINE):
    data_line = "\"{0}\" using {1}:{2}:{3}:{4} title '{5}' with xyerrorlines"
    data_lines = [data_line.format(fn, data_x, data_y, error_x, error_y, fn[7:-4]) for fn in sorted(data_files)]
    generate_plot(outputfn, data_lines, modeline)


def generate_lineplot(outputfn, data_files, data_x, data_y, modeline=SVG_MODELINE):
    data_line = "\"{0}\" using {1}:{2} title '{3}' with lines"
    data_lines = [data_line.format(fn, data_x, data_y, fn[4:-4]) for fn in sorted(data_files)]
    generate_plot(outputfn, data_lines, modeline)


def generate_plot(outputfn, data_lines, modeline=SVG_MODELINE):
    gnuplot_script = "set {2}\n" \
                     + "set key horizontal center bmargin\n" \
                     + "set key font \",7\"\n" \
                     + "set xrange [0:]\n" \
                     + "set yrange [-1:]\n" \
                     + "set output \"{0}\"\n" \
                     + "plot {1}\n"
    data_join = ", \\\n     "

    gnuplot_script = gnuplot_script.format(outputfn, data_join.join(data_lines), modeline).encode("utf-8")

    # stderr is not captured
    gp = subprocess.Popen(["gnuplot"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    gp.communicate(gnuplot_script)


if __name__ == '__main__':
    main()
