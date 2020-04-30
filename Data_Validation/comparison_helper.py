def compare_dataframes(df1, df2):
    """
    Compare two dataframes by applying similar sorting and row indexes.
    :param df1: Source Data Frame.
    :param df2: Target Data Frame.
    """
    try:
        columns = df1.columns

        if len(columns) == 1:
            # Sorting of Data frames on common column
            df1 = df1.sort_values([columns[0]], ascending=[1])
            df2 = df2.sort_values([columns[0]], ascending=[1])
        else:
            # Sorting of Data frames on common column
            df1 = df1.sort_values([columns[0], columns[1]], ascending=[1, 1])
            df2 = df2.sort_values([columns[0], columns[1]], ascending=[1, 1])

        # Reindexing of Columns for Data frames
        #df1 = df1.reindex(sorted(df1.columns), axis=1)
        #df2 = df2.reindex(sorted(df2.columns), axis=1)

        # Resetting the Row Index for Data frames
        #df1 = df1.reset_index(drop=True)
        #df2 = df2.reset_index(drop=True)

        # Change Data types to Object
        df1 = df1.astype(str)
        df2 = df2.astype(str)

        # Comparing Data frames
        df1.to_csv (r'G:\postgres_data.csv', index = None, header=True)
        df2.to_csv (r'G:\api_data_data.csv', index = None, header=True)
        result = df1.equals(df2)

        return result
    except Exception as e:
        return e

