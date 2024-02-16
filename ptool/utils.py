import os
import re
import socket
import yaml

BASEPATH_SCRIPTS = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH_DEFAULT = f"{BASEPATH_SCRIPTS}/ptool_config.yaml"


def determine_computer_from_hostname(config_path=CONFIG_PATH_DEFAULT, verbose=True):
    """
    Determines which yaml config file is needed for this computer.
    Copied and edited from `ESM-Tools` (``esm_tools/src/esm_parser/esm_parser.py``)

    Notes
    -----
    The machine must be registered in the ``ptool_config.yaml`` file in
    order to be found.

    Input
    -----
    config_path : str
        Path to the ``ptool_config.yaml``

    Returns
    -------
    str
        A string with the name of the machine as described in ``ptool_config.yaml``. If
        pattern not matched it returns ``"local"``
    """
    with open(config_path, "r") as f:
        all_computers = yaml.load(f, Loader=yaml.SafeLoader)

    for computer_name, info in all_computers.items():
        nodes = info.get("node_names", [])
        for computer_pattern in nodes.values():
            if isinstance(computer_pattern, str):
                if re.match(computer_pattern, socket.gethostname()) or re.match(
                    computer_pattern, socket.getfqdn()
                ):
                    return computer_name

            elif isinstance(computer_pattern, (list, tuple)):
                computer_patterns = computer_pattern
                for pattern in computer_patterns:
                    if re.match(pattern, socket.gethostname()):
                        return computer_name

    if verbose:
        print(
            "Continues assuming that you are running under a ``local`` machine (no "
            f"matching pattern in ``{config_path}``)"
        )

    return "local"


if __name__ == "__main__":
    print(determine_computer_from_hostname())
