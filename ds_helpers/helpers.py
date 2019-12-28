import pandas as pd

def read_in_dataset(dataset_path, file_name, verbose=False):
    
    """Read in one of the Zillow datasets (train or properties)

    Keyword arguments:
    dataset_path -- A string containing the path to your dataset ('../datasets/folder/)
    file_name.csv -- a string containing your file name including extension. Only takes csv (data.csv)
    verbose -- whether or not to print info about the dataset (True, False)
    
    Returns:
    a pandas dataframe: df
    """
    
    df = pd.read_csv(f'{dataset_path}{file_name}')
    
    if verbose:
        print('------------------------------------------\n')
        print(f'Reading in the {file_name} dataset:')
        print('\n------------------------------------------\n')
        print("it has {0} rows and {1} columns".format(*df.shape))
        print('\n------------------------------------------\n')
        print('It has the following columns: \n')
        print(df.columns)
        print('\n------------------------------------------\n')
        print(' The first 5 rows look like this: \n')
        print(df.head())
        print('\n------------------------------------------\n')
        print(df.describe())
    return df


def merge_dataset(data1, data2, common_key ):
    
    """Merge two datasets. Both need to have a common key

    Keyword arguments:
    data1 -- first dataframe 
    data2 -- second dataframe
    common_key -- key to join dataframes on 

    Returns:
    a pandas dataframe: merged_df
    """

    merged_df = data1.merge(data2, how='left', on=common_key)
    
    return merged_df


def filter_duplicate_col(df,col,verbose=False):
    """Filter out duplicates in column

    Keyword arguments:
    df -- dataframe containing your data
    col -- column to remove duplicates in
    verbose -- whether or not to print info about the dataset (True, False)

    Returns:
    a pandas dataframe: df_reduced
    """

    # check for more than one record per parcel (lot)
    df[col].nunique() == len(df)

    # get counts per parcel ID
    counts_per_parcel = df.groupby(col).size()

    # Get parcel IDs that are recoded more than once
    multiple_transaction = df[df.parcelid.isin(counts_per_parcel[counts_per_parcel > 1].index)]

    # Get parcel IDs that are recorded only once
    one_transaction = df[df.parcelid.isin(counts_per_parcel[counts_per_parcel == 1].index)]

    # Print how many lots have multiple transactions
    if verbose:
        print('\n------------------------------------------\n')
        print(col + " duplicates")
        print(multiple_transaction.parcelid.nunique())
        print('\n------------------------------------------\n')

    # Verify seperation was correct
    assert len(df) == (len(multiple_transaction) + len(one_transaction))
    # select random transaction from with from multiple
    # Merge back into a single dataframe
    df_filtered = multiple_transaction.sample(frac=1, random_state=0).groupby('parcelid').head(1)
    df_filtered = pd.concat([one_transaction, df_filtered])

    # Verify we did not lose any parcel IDs from our train merged DF
    assert set(df_filtered[col]) == set(df[col])

    return df_filtered