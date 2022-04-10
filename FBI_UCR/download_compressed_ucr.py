import os
from states import *

destination = "/root/Work/cdc_project/FBI_UCR/data/compressed"

years = [str(i) for i in range(1990, 2022)]

for year in years:
    for state in states_av:
        file = f"https://s3-us-gov-west-1.amazonaws.com/cg-d3f0433b-a53e-4934-8b94-c678aa2cbaf3/{year}/{state}-{year}.zip"
        response = os.system(f'wget -P {destination} {file}')

