#!/usr/bin/env python3

import subprocess
import sys
from configparser import ConfigParser
from io import StringIO
from typing import Sequence, Optional


class Value:
    def __init__(self, value, scenario="", conflicts=(), requires=()):
        self.value = value
        self.scenario = scenario
        if isinstance(conflicts, str) or isinstance(requires, str):
            raise ValueError("Variant value '{}' '{}' contains a string '{}'/'{}' instead"
                             " of a list for conflicts/requires".format(value, scenario, conflicts, requires))
        self.conflicts = set(conflicts)  # names of conflicting scenarios
        self.requires = set(requires)  # names of required scenarios


class Option:
    def __init__(self, name, *values):
        self.name = name
        self.values = values


# gets loaded later
OPTIONS = None  # type: Optional[Sequence[Option]]
CONFIG_BASE_FILE = "config/refit-defaults"
CONFIG_FILE = "config/refit-overrides"
CONFIG_FILE_SOURCE = CONFIG_FILE
DRY_RUN = False
VERBOSE = False
SCENARIO = None
SCRIPT = []  # type: Sequence[str]
CWD = None
max_ret_code = 0


def printerr(*objects):
    print(*objects, file=sys.stderr)


def load_config_str(fn):
    with open(fn, "r") as f:
        return f.read()


def write_config_str(fn, data):
    with open(fn, "w") as f:
        f.write(data)


def parse_config(data):
    cp = ConfigParser()
    cp.read_string("[DEFAULT]\n" + data)
    return cp


def extract_options(config, base_config):
    variant_tag = ".variants"
    options_data = []
    for name, values in config["DEFAULT"].items():
        is_variant = name.endswith(variant_tag)
        if is_variant:
            name = name[:-len(variant_tag)]

        if name not in base_config["DEFAULT"]:
            raise ValueError("Overwriting option {} which does not exist in refit-defaults".format(name))

        if not is_variant:
            if str(values).find("Value(") >= 0:
                raise ValueError("Non-variant option {} contains 'Value('".format(name))
            continue

        lines = [p.strip() for p in values.strip().split("\\\n")]
        if lines[0]:
            raise ValueError("Variants lines must only contain an '\\' in the line with the key name")
        options_data.append("Option(\"{}\", {})".format(
            name, ", ".join([p for p in lines if p])
        ))

    return ",\n".join(options_data)


def parse_options(options_data):
    return eval("[" + options_data + "]", {"__builtins__": {}, "Option": Option, "Value": Value})


def write_config(fn, config: ConfigParser):
    if DRY_RUN:
        return
    # only write if file content differs
    out = StringIO()
    config.write(out)
    joined = out.getvalue()[10:]
    with open(fn, "r") as f:
        current_data = f.read()
    if joined == current_data:
        return
    with open(fn, "w") as f:
        f.write(joined)


def scenario_to_str(scenario):
    return "-".join([p for p in scenario if len(p) > 0])


def is_chosen_scenario(scenario):
    if SCENARIO is None:
        return True
    scn = scenario_to_str(scenario)
    return SCENARIO.startswith(scn)


def execute(scenario):
    global max_ret_code
    scn = scenario_to_str(scenario)
    if SCENARIO is not None and not SCENARIO == scn:
        return
    if DRY_RUN or VERBOSE:
        print(scn)
    if DRY_RUN:
        return

    cmd = []
    for part in SCRIPT:
        if part == "{}":
            part = scn
        cmd.append(part)

    ret = subprocess.call(cmd, cwd=CWD)
    max_ret_code = max(max_ret_code, ret)


def dot(fn, config, idx=0, scenario=None):
    if scenario is None:
        scenario = []
    if idx >= len(OPTIONS):
        write_config(fn, config)
        execute(scenario)
    else:
        opt = OPTIONS[idx]
        for value in opt.values:
            scenario.append(value.scenario)
            # skip conflicting scenarios
            if is_chosen_scenario(scenario) \
                    and not (set(scenario) & value.conflicts) \
                    and not value.requires - set(scenario):

                if opt.name == "client.count" and not value.scenario:
                    clients = sum([int(s) for s in value.value.split(",")])
                    scenario.pop()
                    scenario.append(str(clients))

                if VERBOSE:
                    print((" " * idx) + opt.name + " = " + value.value)

                # patched options will always be set no matter which scenario is configured
                config["DEFAULT"][opt.name] = value.value
                dot(fn, config, idx + 1, scenario)
            scenario.pop()


def run():
    global DRY_RUN, VERBOSE, SCENARIO, SCRIPT, OPTIONS, CONFIG_FILE_SOURCE, CWD
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--dry-run', action='store_true',
                        help='Don\'t modify the configuration file, don\'t run the script command, '
                             'just print the scenarios.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Verbose printing of config modification')
    parser.add_argument('-s', '--scenario', help='Only run the specified scenario. If multiple '
                                                 'configurations have the same name, all of them will be executed.')
    parser.add_argument('-c', '--config-file', help='Use the specified config file as source')
    parser.add_argument('-d', '--directory', help='Execute script in the given directory. Defaults '
                                                  'to the current directory.')
    parser.add_argument('scenario_prefix', help='Prefix for the assembled scenario name, can be '
                                                'empty (pass an empty string: "").')
    parser.add_argument('script', nargs='+', help='Command to run for every scenario. If a placeholder {} exists,'
                                                  ' then it will be replaced with the scenario name.')

    args = parser.parse_args()
    scenario_prefix = [args.scenario_prefix.strip("-")]
    SCRIPT = args.script
    DRY_RUN = args.dry_run
    VERBOSE = args.verbose
    SCENARIO = args.scenario
    CWD = args.directory
    if args.config_file is not None:
        CONFIG_FILE_SOURCE = args.config_file

    config_data = load_config_str(CONFIG_FILE_SOURCE)
    config = parse_config(config_data)
    base_config = parse_config(load_config_str(CONFIG_BASE_FILE))
    options_data = extract_options(config, base_config)
    # print(options_data)

    OPTIONS = parse_options(options_data)
    dot(CONFIG_FILE, config, 0, scenario_prefix)

    # restore original config
    write_config_str(CONFIG_FILE_SOURCE, config_data)


if __name__ == '__main__':
    run()
    sys.exit(max_ret_code)
