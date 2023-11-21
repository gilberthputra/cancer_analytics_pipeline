import pandas as pd
import re

def clean_column_name(column_name):
    column_name = re.sub(r'\s+|\W+', '_', column_name) # Replace spaces/special characters with underscores
    column_name = re.sub(r'__+', '_', column_name) # Remove double underscores
    column_name = re.sub(r'^_|_$', '', column_name) # Remove leading and ending underscore
    return column_name.upper()

if __name__ == "__main__":

    # Download manually from: https://www.aihw.gov.au/reports/cancer/cancer-data-in-australia/data
    # Download book1a, book2a, book3a and book7
    
    incidence_df = pd.read_excel('../data/raw/aihw-can-122-CDiA-2023-Book-1a-Cancer-incidence-age-standardised-rates-5-year-age-groups.xlsx', 
                                sheet_name='Table S1a.1',
                                skiprows=5,
                                skipfooter=20).iloc[:, :6].replace(". .", "").rename(columns=clean_column_name)
    incidence_df.to_csv('../data/transformed/incidence.csv', index=False, sep='|')

    mortality_df = pd.read_excel('../data/raw/aihw-can-122-CDiA-2023-Book-2a-Cancer-mortality-and-age-standardised-rates-by-age-5-year-groups.xlsx', 
                                sheet_name='Table S2a.1',
                                skiprows=5,
                                skipfooter=18).iloc[:, :6].replace(". .", "").rename(columns=clean_column_name)
    mortality_df.to_csv('../data/transformed/mortality.csv', index=False, sep='|')

    survival_df = pd.read_excel('../data/raw/aihw-can-122-CDiA-2023-Book-3a-Cancer-survival-summary-observed-relative-conditional-estimates.xlsx', 
                                sheet_name='Table S3a.2',
                                skiprows=5,
                                skipfooter=10).iloc[:, :-2].replace("n.p.", "").rename(columns=clean_column_name)
    survival_df.to_csv('../data/transformed/survival.csv', index=False, sep='|')

    territory_df = pd.read_excel('../data/raw/aihw-can-122-CDiA-2023-Book-7-Cancer-incidence-by-state-and-territory.xlsx', 
                                sheet_name='Table S7.1',
                                skiprows=5,
                                skipfooter=17).iloc[:, :6].replace(". .", "").rename(columns=clean_column_name)
    territory_df.to_csv('../data/transformed/incidence_territory.csv', index=False, sep='|')