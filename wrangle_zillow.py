import pandas as pd
import numpy as np

from env import get_db_url



def get_zillow_data():
    '''
    Function will try to return ad database from csv file if file is local and in same directory.
    IF file doesn't exist it will create and store in same directory
    Otherwise will pull from codeup database.
    Must have credentials for codeup database.
    '''
    query = '''
        SELECT * FROM properties_2017
        left JOIN propertylandusetype USING(propertylandusetypeid)
        left join airconditioningtype using(airconditioningtypeid)
        left join architecturalstyletype using(architecturalstyletypeid)
        left join buildingclasstype using (buildingclasstypeid)
        left join heatingorsystemtype using(heatingorsystemtypeid)
        left JOIN predictions_2017 using(parcelid)
        left join storytype using(storytypeid)
        left join typeconstructiontype using (typeconstructiontypeid)
        WHERE transactiondate like %s
        '''
    params = ('2017%',)
    try:
        csv_info = pd.read_csv('zillow.csv', index_col=0 )
        return csv_info
    except FileNotFoundError:
        url = get_db_url('zillow')
        info = pd.read_sql(query, url, params=params)
        info.to_csv("zillow.csv", index=True)
        return info
    
    
def nulls_by_col(df):
    '''
    This function takes in a dataframe 
    and finds the number of missing values
    it returns a new dataframe with quantity and percent of missing values
    '''
    num_missing = df.isnull().sum()
    rows = df.shape[0]
    percent_missing = num_missing / rows * 100
    cols_missing = pd.DataFrame({'num_rows_missing': num_missing, 'percent_rows_missing': percent_missing})
    return cols_missing.sort_values(by='num_rows_missing', ascending=False)


def nulls_by_row(df):
    '''
    This function takes in a dataframe 
    and finds the number of missing values in a row
    it returns a new dataframe with quantity and percent of missing values
    '''
    num_missing = df.isnull().sum(axis=1)
    percent_miss = num_missing / df.shape[1] * 100
    rows_missing = pd.DataFrame({'num_cols_missing': num_missing, 'percent_cols_missing': percent_miss})
    rows_missing = df.merge(rows_missing,
                        left_index=True,
                        right_index=True)[['num_cols_missing', 'percent_cols_missing']]
    return rows_missing.sort_values(by='num_cols_missing', ascending=False)



def summarize(df):
    '''
    summarize will take in a single argument (a pandas dataframe) 
    and output to console various statistics on said dataframe, including:
    # .head()
    # .info()
    # .describe()
    # .value_counts()
    # observation of nulls in the dataframe
    '''
    print('SUMMARY REPORT')
    print('=====================================================\n\n')
    print('Dataframe head: ')
    print(df.head(3))
    print('=====================================================\n\n')
    print('Dataframe info: ')
    print(df.info())
    print('=====================================================\n\n')
    print('Dataframe Description: ')
    print(df.describe())
    num_cols = [col for col in df.columns if df[col].dtype != 'O']
    cat_cols = [col for col in df.columns if col not in num_cols]
    print('=====================================================')
    print('DataFrame value counts: ')
    for col in df.columns:
        if col in cat_cols:
            print(df[col].value_counts(), '\n')
        else:
            print(df[col].value_counts(bins=10, sort=False), '\n')
    print('=====================================================')
    print('nulls in dataframe by column: ')
    print(nulls_by_col(df))
    print('=====================================================')
    print('nulls in dataframe by row: ')
    print(nulls_by_row(df))
    print('=====================================================')
    
    

def remove_outliers(df, col_list, k=1.5):
    '''
    remove outliers from a dataframe based on a list of columns
    using the tukey method.
    returns a single dataframe with outliers removed
    must drop columns that have dtypes of objects
    '''
    col_qs = {}
    for col in col_list:
        col_qs[col] = q1, q3 = df[col].quantile([0.25, 0.75])
    for col in col_list:
        iqr = col_qs[col][0.75] - col_qs[col][0.25]
        lower_fence = col_qs[col][0.25] - (k*iqr)
        upper_fence = col_qs[col][0.75] + (k*iqr)
        print(type(lower_fence))
        print(lower_fence)
        df = df[(df[col] > lower_fence) & (df[col] < upper_fence)]
    return df    
    

    
def drop_nulls(df, percent):
    '''
    Takes in a dataframe and a percent cutoff to return a dataframe with all the columns that are within the cutoff percentage.

    INPUT:
    df = pandas dataframe
    percent = Null percent cutoff. (0.00)

    OUTPUT:
    new_df = pandas dataframe with all columns that are within the cutoff percentage.
    '''
    original_cols = df.columns.to_list()
    drop_cols = []
    for col in original_cols:
        null_pct = df[col].isna().sum() / df.shape[0]
        if null_pct > percent:
            drop_cols.append(col)
    new_df = df.drop(columns=drop_cols)
    return new_df    
    
    
def prep_zillow(df):
    df.sort_values('transactiondate', ascending=False, inplace=True)
    df.drop_duplicates(subset=['parcelid'], keep='first', inplace=True)
    df = df[df.propertylandusedesc == 'Single Family Residential']
    df = drop_nulls(df, 0.25)
    mean_cols = ['calculatedfinishedsquarefeet', 'finishedsquarefeet12', 'lotsizesquarefeet', 
             'structuretaxvaluedollarcnt', 'taxvaluedollarcnt', 'landtaxvaluedollarcnt', 'taxamount']
    mode_cols = ['calculatedbathnbr', 'fullbathcnt', 'regionidcity', 'regionidzip', 'censustractandblock', 'yearbuilt']
    for col in mean_cols:
        df[col].fillna(df[col].mean(), inplace=True)
    for col in mode_cols:
        df[col].fillna(df[col].mode()[0], inplace=True)
    return df
