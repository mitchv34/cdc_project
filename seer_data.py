# This script is used to convert the data from the Seer website into a radable CSV file.
# The script saves _ files:
#  - county_pop_data.csv - contains the population data for each county
#  - state_pop_data.csv - same but aggregated by state
#  - county_data_child.csv - instead of having all age groups, only divide the populaiton between child and adult
#  - state_data_child.csv - same but aggregated by state

import pandas as pd 


# Replace the path with the path of the data you to downloaded.
data = pd.read_csv("/mnt/c/Users/mitch/Downloads/us.1969_2019.19ages.adjusted.txt",  header=0, parse_dates=[0], index_col=0, squeeze=True)

#  Filter out unwanted years
rows = [r for r in  data.index.tolist() if int(r[:4]) >= 2000] 

# Create data dictionary
pop_data = {
    "year": [int(r[:4]) for r in rows],
    "state": [r[4:6] for r in rows],
    "FIPS_state": [r[6:8] for r in rows],
    "FIPS_county": [r[8:11] for r in rows],
    "registry" : [r[11:13] for r in rows],
    "race" : [r[13:14] for r in rows],
    "origin" : [r[14:15] for r in rows],
    "sex" : [r[15:16] for r in rows],
    "age" : [r[16:18] for r in rows],
    "population" : [int(r[18:]) for r in rows]
}

# Crteate dataframe 

data_df = pd.DataFrame(pop_data)

# Create a child column including oly the 0 to 14 years old
data.loc[:, "child"] = data.age.astype(int) <  5

# Write dataframe as csv file (replace path with the path of the file you want to save)

data_df.to_csv("/root/Work/cdc_project/FBI_UCR/county_pop_data.csv", index = False)

data.groupby(["state", "year", "age", "sex"]).agg({"population": "sum"}).reset_index().to_csv("/root/Work/cdc_project/FBI_UCR/state_pop_data.csv", index = False)

data.groupby(["state", "FIPS_county", "year", "child", "sex"]).agg({"population": "sum"}).reset_index().to_csv("/root/Work/cdc_project/FBI_UCR/county_data_child.csv", index = False)

data.groupby(["state", "year", "child", "sex"]).agg({"population": "sum"}).reset_index().to_csv("/root/Work/cdc_project/FBI_UCR/state_data_child.csv", index = False)