import pandas as pd
import os
from  zipfile import ZipFile
from  states import *
import shutil


# This function assings a label given a relation_id and an offence_id
def get_crime_type(relationship_id,  offence_id):

    # Relathionship id of the victims for Child Maltreatment
    CM = [4, 5] # TODO: Ask why not include step child code 20
    # Relathionship id of the victims for Inter partnet violence
    IPV = [3, 6, 21, 26, 27]
    # Offence id Child Maltreatment
    CM_1 = [1, 27, 32, 38 ] # physical abuse
    CM_2 = [6] # neglect
    CM_3 = [36, 4, 43, 3, 55, 59, 60] # sexual abuse
    # Offence id Inter partnet violence
    IPV_1 = [1, 27, 32, 38 ] # physical abuse
    IPV_2 = [6] # neglect or emotional abuse
    IPV_3 = [36, 4, 43] # sexual abuse
    if relationship_id in CM:
        if offence_id in CM_1:
            return "CM_Physical_Abuse"
        elif offence_id in CM_2:
            return "CM_Neglect"
        elif offence_id in CM_3:
            return "CM_Sexual_Abuse"
        else:
            return "CM_Other"
    elif relationship_id in IPV:
        if offence_id in IPV_1:
            return "IPV_Physical_Abuse"
        elif offence_id in IPV_2:
            return "IPV_Neglect_or_Emotional_Abuse"
        elif offence_id in IPV_3:
            return "IPV_Sexual_Abuse"
        else:
            return "IPV_Other"

# This function extract files and homogenizes names
def extract_files(zip_file, temp_folder, state, keep_files = False):

    # Path for keeping files
    keep_folder = "/".join(temp_folder.split("/")[:-1] + [f"extracted_files/{state}/"])
    

    # Extract files
    ZipFile(zip_file).extractall(path=temp_folder)
    # Check if state folder created
    if len( os.listdir(temp_folder) ) == 1:
        os.rename(temp_folder + os.listdir(temp_folder)[0], temp_folder + state)
        temp_folder += f"{state}/"
    # Rename files
    [os.rename(temp_folder + file, temp_folder + file.upper()) for file in os.listdir(temp_folder) if file[-3:].lower() == "csv"]
    # Agencies
    for file in os.listdir(temp_folder):
        if "AGENCIES" in file:
            os.rename(temp_folder + file, temp_folder + "AGENCIES.csv")

    # Keep files
    if keep_files:
        os.mkdir(keep_folder)
        files_keep = ['NIBRS_OFFENSE_TYPE.CSV',  'NIBRS_OFFENSE.CSV',
                    'NIBRS_VICTIM.CSV', 'NIBRS_VICTIM_OFFENDER_REL.CSV',
                    'NIBRS_INCIDENT.CSV', 'NIBRS_INCIDENT.CSV']#, 'AGENCIES.CSV']
        for file in files_keep: 
            shutil.copy2(temp_folder + file, keep_folder+file)
    # Return (posibly modified path)
    return temp_folder

# This function clears temp file
def remove_files(temp_folder, state):
    [os.remove(temp_folder + file) for file in os.listdir(temp_folder)]

    

    if  f"{state}/" in temp_folder:
        os.rmdir(temp_folder)


    return temp_folder

# This function reads the data needed and filters the dataframe
def filter_data(temp_folder):
    ## Agency data to match agencies to their counties
    # agency_data = pd.read_csv(temp_folder + 'AGENCIES.CSV', low_memory=False)#[['AGENCY_ID', 'COUNTY_NAME']]

    ## Agency data to match agencies to their counties
    # agency_data = pd.read_csv(temp_folder + 'AGENCIES.CSV', low_memory=False).rename(columns = lambda x : x.upper())

    # Offence codes to match to the crime
    offence_type =   pd.read_csv(temp_folder + 'NIBRS_OFFENSE_TYPE.CSV', low_memory=False).rename(columns = lambda x : x.upper())
    # I will filter the data ro only include crimes againt persons
    offence_type = offence_type[offence_type['CRIME_AGAINST'] =='Person']

    offence = pd.read_csv(temp_folder + 'NIBRS_OFFENSE.CSV', low_memory=False).rename(columns = lambda x : x.upper())
    # Victim data
    victims = pd.read_csv(temp_folder + 'NIBRS_VICTIM.CSV', low_memory=False).rename(columns = lambda x : x.upper())

    controls = ["AGE_NUM", "SEX_CODE", "RACE_ID", "ETHNICITY_ID", "RESIDENT_STATUS_CODE"]
    victims = victims[["VICTIM_ID", "INCIDENT_ID"] + controls]

    victim_offender = pd.read_csv(temp_folder + 'NIBRS_VICTIM_OFFENDER_REL.CSV', low_memory=False).rename(columns = lambda x : x.upper())

    # Relationship between the offenders and the victm
    # relationship = pd.read_csv(temp_folder + 'NIBRS_RELATIONSHIP.CSV', low_memory=False).rename(columns = lambda x : x.upper())

    incidents =  pd.read_csv(temp_folder + 'NIBRS_INCIDENT.CSV', low_memory=False).rename(columns = lambda x : x.upper())

    # Relationship between the offenders and the victims 
    relationship_id = [4, 5, 3, 6, 21, 26, 27]
    # Get the victims with the relationship
    victims_with_relationship = victim_offender[victim_offender.RELATIONSHIP_ID.isin(relationship_id)][["VICTIM_ID", "RELATIONSHIP_ID"]]
    # Get the victims with the relationship and drop values where controls are missing
    victims_filtered = victims.merge(victims_with_relationship, on = "VICTIM_ID", how = "inner")#.dropna()

    # Codes of the offences we are interested in
    offence_id = [1, 3, 4, 6, 27, 32, 36, 38, 43, 55, 59, 60]

    incidents_interested_offence_type = offence[offence.OFFENSE_TYPE_ID.isin(offence_id)][["INCIDENT_ID", "OFFENSE_TYPE_ID"]]

    # incidents_interested_offence_type
    victims_filtered = victims_filtered.merge(incidents_interested_offence_type, on = "INCIDENT_ID", how = "inner")
    incidents_final = victims_filtered.merge(incidents[["AGENCY_ID","INCIDENT_ID"]], on = "INCIDENT_ID", how = "inner")

    return incidents_final


## Run the script
if __name__ == '__main__':
    
    # state = states_av[1]
    path = "/root/Work/cdc_project/FBI_UCR/data/compressed"
    files = os.listdir(path)
    files.sort()
    # years = ["2012"]#, "2016", "2017", "2018", "2019", "2020"]

    stats = {
        "year": [],
        "state": [],
        "incidents": [],
        "CM_Other": [],
        "CM_Physical_Abuse": [],
        "CM_Neglect": [],
        "CM_Sexual_Abuse": [],
        "IPV_Other": [],
        "IPV_Physical_Abuse": [],
        "IPV_Neglect_or_Emotional_Abuse": [],
        "IPV_Sexual_Abuse": [],
    }

    corruped_files = []
    outcomes = pd.DataFrame()

    for state in states_av:
        files_state = [path + '/' + file for file in files if state in file]
        print(f"\n {state}", end=" ")
        for zip_file in files_state:
            if zip_file[-4:] != ".zip":
                os.remove(zip_file)
            # if zip_file[-8:-4] not in years:
            #     continue
            print(f"{zip_file[-8:-4]}", end=" ")
            temp_folder = "/root/Work/cdc_project/FBI_UCR/data/temp/"
            temp_folder = extract_files(zip_file, temp_folder, state)
            try:
                df = filter_data(temp_folder)
                stats["year"].append(zip_file[-8:-4])
                stats["state"].append(state)
                stats["incidents"].append(len(df))
                if len(df) == 0:
                    stats["total_CM_Neglect"].append(0)
                    stats["total_CM_Sexual_Abuse"].append(0)
                    stats["total_IPV"].append(0)
                    stats["total_IPV_Physical_Abuse"].append(0)
                    stats["total_IPV_Neglect_or_Emotional_Abuse"].append(0)
                    stats["total_IPV_Sexual_Abuse"].append(0)
                else:   
                    # Tag the type of incident
                    df["OUTCOME"] = df.apply(lambda x : get_crime_type(x.RELATIONSHIP_ID, x.OFFENSE_TYPE_ID), axis=1)
                    outcomes = outcomes.append(df)
                    counts = df.OUTCOME.value_counts()
                    for key in stats.keys():
                        if key in ["year", "state", "incidents"]:
                            continue
                        if key in counts.index:
                            stats[key].append(counts[key])
                        else:
                            stats[key].append(0)
            except:
                temp_folder = remove_files(temp_folder, state)
                print(u'\u2717', end=" ")
                corruped_files.append(zip_file)
                continue
            temp_folder = remove_files(temp_folder, state)
            print(u'\u2713', end=" ")

    pd.DataFrame(stats).to_csv("/root/Work/cdc_project/FBI_UCR/data/stats.csv", index=False)
    outcomes.to_csv("/root/Work/cdc_project/FBI_UCR/data/outcomes.csv", index=False)
    pd.Series(corruped_files).to_csv("/root/Work/cdc_project/FBI_UCR/data/corruped_files.csv", index=False)
