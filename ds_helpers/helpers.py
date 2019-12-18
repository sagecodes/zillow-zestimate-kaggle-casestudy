import pandas as pd

def read_in_dataset(dataset_path, file_name, verbose=False):
    
    """Read in one of the Zillow datasets (train or properties)

    Keyword arguments:
    dataset_path -- A string containing the path to your dataset ('../datasets/folder/)
    file_name -- a string containing your file name including extension (data.csv)
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
        print('------------------------------------------')
        
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