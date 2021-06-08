import pandas as pd


def read_aux_table(path: str, value: str) -> pd.DataFrame:
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


def read_addr(path: str) -> pd.DataFrame:
    return read_aux_table(
        path,
        'address'
    )


def read_country(path: str) -> pd.DataFrame:
    return read_aux_table(
        path,
        'city'
    )


def read_residence(path: str) -> pd.DataFrame:
    return read_aux_table(
        path,
        'residence'
    )


def read_mode(path: str) -> pd.DataFrame:
    return read_aux_table(
        path,
        'mode'
    )


def read_port(path: str) -> pd.DataFrame:
    return read_aux_table(
        path,
        'port'
    )


def read_visa(path: str) -> pd.DataFrame:
    return read_aux_table(
        path,
        'visa'
    )


def read_demographics(path: str) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        delimiter=';'
    )

    df.columns = [
        col.lstrip().rstrip().replace(' ', '_').replace('-', '_').lower()
        for col in df.columns
    ]

    return df
