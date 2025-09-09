import numpy as np
import pandas as pd

################################################################################
#### MCNP output tally parsing  ################################################
################################################################################
def get_tallies(filepath: str) -> dict:
    """Gets master tally dictionary from MCNP output.

    Args:
        filepath (str): Path to MCNP output file.

    Returns:
        dict: Tallies dictionary.
    """
    tally_strings = parse_tallies(filepath)
    friendly_tallies = [make_friendly_tally(tally) for tally in tally_strings]
    return {tally[0]: tally[1] for tally in friendly_tallies}

def parse_tallies(filepath: str) -> list:
    """Collects tallies from MCNP simulation output.

    Args:
        filepath (str): Path to MCNP output file.

    Returns:
        list: Tally results from MCNP.
    """
    tallies = []
    with open(filepath, "r") as f:
        pre_output = True
        for line in f:
            if "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" in line:
                pre_output = False
                continue
            elif pre_output:
                continue
            elif "1status" in line:
                break
            try:
                first = line.split()[0]
                if first == "1tally":
                    tally = line
                    while True:
                        newline = f.readline()
                        tally += newline
                        if "fom = (histories/minute)" in newline:
                            tallies.append(tally)
                            break
                        elif " there are no nonzero tallies" in newline:
                            tallies.append(tally)
                            break
                    continue
            except IndexError:
                continue
    return tallies

def check_float(item) -> bool:
    """Checks if the item is float compatible.

    Args:
        item (_type_): Item to check if float compatible.

    Returns:
        bool: Compatibility of item.
    """
    try:
        float(item)
        return True
    except ValueError:
        return False

def make_friendly_tally(tally: str) -> tuple:
    """Generates serializably friendly tally data.

    Args:
        tally (str): Tally string from MCNP.

    Returns:
        tuple: (tally_name, [[energy], [flux], [std]])
    """
    lines = tally.split('\n')
    split_lines = [line.split() for line in lines]
    data_lines = []
    for split_line in split_lines:
        try:
            first_item = split_line[0]
            if first_item == "+":
                words = split_line[1:]
                tally_name = ""
                for i in words:
                    tally_name += i + " "
                tally_name = tally_name[:-1]
            if check_float(first_item):
                data_line = [float(item) for item in split_line]
                data_lines.append(data_line)
            if first_item == "total":
                total_line = split_line
        except IndexError:
            pass
    data_list = [data_line for data_line in data_lines[1:]]
    data_np = np.array(data_list)
    data_np = np.flip(data_np, axis=0).T
    data = [list(i) for i in data_np]
    data = [[float(j) for j in i] for i in data]
    return (tally_name, data)

################################################################################
#### Tally dictionary loading   ################################################
################################################################################
def conv_tallies(tallies_dict: dict) -> dict:
    """Converts all tallies in a dictionary to DataFrames.

    Args:
        tallies_dict (dict): All tallies.

    Returns:
        dict: Tallies converted to DataFrames.
    """
    tally_keys = tallies_dict.keys()
    return {key: conv_to_df(tallies_dict[key]) for key in tally_keys}

def conv_to_df(tally: list) -> pd.DataFrame:
    """Converts the data from a single tally into a DataFrame.

    Args:
        tally (list): Data from tally.

    Returns:
        pd.DataFrame: Processed tally data.
    """
    labels = ['energy', 'flux', 'std']
    d = {label: vals for label, vals in zip(labels, tally)}
    return pd.DataFrame(d)