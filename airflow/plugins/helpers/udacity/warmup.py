import pandas as pd


def read_aux_table(path: str, value: str) -> pd.DataFrame:
    """
    Function that servers has template for following read functions:
        - read_states
        - read_country
        - read_residence
        - read_mode
        - read_port
        - read_visa

    It reads a csv file from `path`, adds an `id` column, and changes the `code` column to `uid`, as well as
    `value` to the column in the parameter `value`

    Parameters
    ----------
    path:
        path of the dataframe to read
    value:
        column name for the `value` column

    Returns
    -------
    """
    df = pd.read_csv(
        path
    ).reset_index().rename(
        columns={
            'index': 'id',
            'code': 'uid',
            'value': value
        }
    )

    df['id'] += 1

    return df


def read_states(path: str) -> pd.DataFrame:
    """
    Reads the state dimension table in path
    """
    return read_aux_table(
        path,
        'state'
    )


def read_country(path: str) -> pd.DataFrame:
    """
    Reads the country dimension table in path
    """
    return read_aux_table(
        path,
        'city'
    )


def read_residence(path: str) -> pd.DataFrame:
    """
    Reads the residence dimension table in path
    """
    return read_aux_table(
        path,
        'residence'
    )


def read_mode(path: str) -> pd.DataFrame:
    """
    Reads the mode dimension table in path
    """
    return read_aux_table(
        path,
        'mode'
    )


def read_port(path: str) -> pd.DataFrame:
    """
    Reads the port dimension table in path
    """
    return read_aux_table(
        path,
        'port'
    )


def read_visa(path: str) -> pd.DataFrame:
    """
    Reads the visa type dimension table in path
    """
    return read_aux_table(
        path,
        'visa'
    )


def read_demographics(path: str) -> pd.DataFrame:
    """
    Reads the demographics dimension table in path. It cleans the column name so that all columns are software
    friendly names (no spaces, and no caps)
    """
    df = pd.read_csv(
        path,
        delimiter=';'
    )

    df.columns = [
        col.lstrip().rstrip().replace(' ', '_').replace('-', '_').lower()
        for col in df.columns
    ]

    return df
