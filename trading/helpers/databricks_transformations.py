from pyspark.sql import DataFrame


def rename_columns(df: DataFrame, dict: dict) -> DataFrame:
    """
    renaming columns in dataframe according to definition in given dictionary
        Parameters:
             df(Dataframe): input dataframe to be transformed
             dict(dict): definition of replacement
        Returns:
             Dataframe: tranformed dataframe with new columns names in it
    """
    return df.withColumnsRenamed(dict)


def filter_data(df: DataFrame, cond: str):
    """
    filtering our records from dataframe
        Parameters:
             df(Dataframe): input dataframe to be transformed
             cond(str): condition on which filtering should be performed
        Returns:
             Dataframe - tranformed dataframe without filtered records
    """
    return df.filter(cond)
