def drop_multiple_col(col_names_list, df):
    """
    AIM :  drop multiple columns based on their column names
    input : list of column names, df
    output : updated df with dropped columns
    """
    df.drop(col_names_list, axis=1, inplace=True)
    return df

# convert categorical to numerical:

def convert_cat2num(df):
    num_encode = {'col_1':{'YES':1, 'NO':0},
                'col_2':{'WON':1, 'LOSE':0, 'DRAW':0}}
    df.replace(num_encode, inplace=True)
    return df


# check the missing value
def check_missing_data(df):
    return df.isnull().sum().sort_values(ascending=False)


# remove the white space in cols
def remove_col_white_space(df):
    df[col] = df[col].str.lstrip()


#concat two string data
def concat_col_str_condition(df):
    mask = df['col_1'].str.endswith('pil',na=False)
    col_new = df[mask]['col_1'] + df[mask]['col_2']
    
