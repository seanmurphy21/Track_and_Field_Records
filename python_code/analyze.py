# -*- coding: utf-8 -*-










'''
This script will analyze data collected from the Wikipedia page 
"List of world records in athletics" found here: 
https://en.wikipedia.org/wiki/List_of_world_records_in_athletics. 
It will produce three .csv files.  The first will contain the record counts 
broken down by event category and nation.  The second will contain the count 
of records broken down by decade and event category.  The third will contain 
the average and median number of years for which records have lasted broken 
down by event category.

Data accessed on 4/19/2024.
'''










### load necessary packages
import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta










### IMPORT THE DATA FILES THAT WERE CREATED BY GATHER.PY





# read in the data of all records as record_df
record_df = pd.read_csv('all_track_and_field_records.csv')

# read in the data of timed records as time_record_df
time_record_df = pd.read_csv('timed_track_and_field_records.csv')

# read in the data of distance records as distance_record_df
distance_record_df = pd.read_csv('distance_track_and_field_records.csv')

# read in the data of points records as pts_record_df
pts_record_df = pd.read_csv('points_track_and_field_records.csv')










'''
QUESTION 1) Which nations hold the most records overall? Which nations hold 
the most records when broken down by event category?  Include gender as a
grouping category.
'''










### GENERATE THE DF WITH RECORD COUNTS GROUPED BY CATEGORY, GENDER, and NATIONALITY





# subset record_df to include only rows that contain the nationality
non_na_nations_df = record_df[~pd.isna(record_df['Nationality'])]

# subset this df to include only rows with a single nation, the others will be incorporated later
one_nation_df = non_na_nations_df[~non_na_nations_df['Nationality'].str.contains(',')]



# group the record counts by nationality, gender, and event category, store as nat_cat_gen_groupby
nat_cat_gen_groupby = one_nation_df.groupby(by = ['Category', 'Gender', 'Nationality'])['Event'].count()

# grab a list of the multiindices from the above pd.Series, to be used below
multiindex_list = list(nat_cat_gen_groupby.index)

# grab the categories, genders, and nationalities from the multiindex of the above pd.Series

# initialize with an empty list
categories = []
genders = []
nationalities = []

# fill the lists with the appropriate values
for tup in multiindex_list:
    
    # append the first element to the categories list
    categories.append(tup[0])
    
    # append the second element to the genders list
    genders.append(tup[1])
    
    # append the third element to the nationalities list
    nationalities.append(tup[2])
    
    
    
# generate a pandas dataframe from the four above list/pd.Series objects
record_count_df = pd.DataFrame({'Category': categories,
                                'Gender': genders,
                                'Nationality': nationalities,
                                'Record_Count': list(nat_cat_gen_groupby)})



### now, to incorporate the records held by multiple nations

# first, one can uncomment and run the below code to see that all such records are relays
#non_na_nations_df[non_na_nations_df['Nationality'].str.contains(',')]

# grab all the rows with records held by multiple nations
multi_nation_df = non_na_nations_df[non_na_nations_df['Nationality'].str.contains(',')]

# split the multi_nation_df into male and female for counting
multi_nation_male_df = multi_nation_df[multi_nation_df['Gender'] == 'Male']
multi_nation_female_df = multi_nation_df[multi_nation_df['Gender'] == 'Female']



# write a function that generates a dict of nations and the number of lists they appear in
# input should be a list containing lists of nation abbreviations as strings
def nation_counter(lists_of_nations):
    
    # initialize the nation_counter_dict
    nation_counter_dict = {}
    
    # initialize the empty list of nations
    nations = []
    
    # for every string list, add the new nations to the above list
    for string in lists_of_nations:
        
        # split the list
        split_string = string.split(', ')
        
        # iterate through the nations
        for nation in split_string:
            
            # check if the nation is in the list of nations
            if nation not in nations:
                
                # if not, append it to the list
                nations.append(nation)
    
    # now for each nation in the list, check how many of the string lists it appears in
    for nation in nations:
        
        # initialize the count at 0
        appearance_count = 0
        
        # check whether the nation is in each string and, if so, add 1 to the count
        for string in lists_of_nations:
            
            if nation in string:
                
                appearance_count += 1
                
        # add the count for the nation to the dictionary
        nation_counter_dict[nation] = appearance_count
        
    # when the dict is full, return it
    return nation_counter_dict



# call the above function on the 'Nationality' column in the male and female
# multi_nation_df's and store the resulting dicts
multi_nation_male_dict = nation_counter(list(multi_nation_male_df['Nationality']))
multi_nation_female_dict = nation_counter(list(multi_nation_female_df['Nationality']))


# grab the dataframe subsetted for rows that are counts of relay records for both males and females
relay_df = record_count_df[record_count_df['Category'] == 'Relay']
relay_male_df = relay_df[relay_df['Gender'] == 'Male']
relay_female_df = relay_df[relay_df['Gender'] == 'Female']

# grab the list of nations who already have records in relay events in 
# record_count_df for males and females
relay_nation_male_list = list(relay_male_df['Nationality'])
relay_nation_female_list = list(relay_female_df['Nationality'])



# given a gender, a multi_nation_dict, a relay_nation_list, a relay_df, and a record_count_df,
# define a function to add record counts to the record_count_df
def record_adder(gender_str, multi_nation_dict, relay_nation_list, relay_df, record_count_df):
    
    # grab a copy of the record_count_df
    record_count_df = record_count_df.copy()
    
    # grab a list of the multi_nation_dict keys
    list_keys = list(multi_nation_dict.keys())
    
    for nation in list_keys:
        
        # first, check if already in relay_nation_list
        if nation in relay_nation_list:
            
            # if so, grab the index location of the row where it is
            ind = relay_df[(relay_df['Nationality'] == nation) & (relay_df['Gender'] == gender_str)].index[0]
            
            # grab its current value
            current_val = record_count_df['Record_Count'].iloc[ind]
            
            # update with the new value
            new_val = current_val + multi_nation_dict[nation]
            
            # assign the new value to the appropriate row location
            record_count_df.at[ind, 'Record_Count'] = new_val
            
    return record_count_df



# call the above function for males and females
record_count_df = record_adder('Male', multi_nation_male_dict, relay_nation_male_list, relay_male_df, record_count_df)
record_count_df = record_adder('Female', multi_nation_female_dict, relay_nation_female_list, relay_female_df, record_count_df)
        
        
        
# grab a dict of all the nations in multi_nation_dict not already in the df for males
new_nations_male_dict = {}

for nation in list(multi_nation_male_dict.keys()):
    
    if not nation in relay_nation_male_list:
        
        new_nations_male_dict[nation] = multi_nation_male_dict[nation]

# grab a dict of all the nations in multi_nation_dict not already in the df for females
new_nations_female_dict = {}

for nation in list(multi_nation_female_dict.keys()):
    
    if not nation in relay_nation_female_list:
        
        new_nations_female_dict[nation] = multi_nation_female_dict[nation]
        
        
        
# for the nations not already in the df, we will just make new df's and concatenate

# df for males
new_nation_male_df = pd.DataFrame({'Category': list(np.repeat('Relay', 5)),
                                   'Gender': list(np.repeat('Male', 5)),
                                   'Nationality': list(new_nations_male_dict.keys()),
                                   'Record_Count': list(new_nations_male_dict.values())})

# df for females
new_nation_female_df = pd.DataFrame({'Category': list(np.repeat('Relay', 3)),
                                   'Gender': list(np.repeat('Female', 3)),
                                   'Nationality': list(new_nations_female_dict.keys()),
                                   'Record_Count': list(new_nations_female_dict.values())})



# now add the rows to record_count_df
record_count_df = pd.concat([record_count_df, new_nation_male_df, new_nation_female_df])

# reset the index
record_count_df.reset_index(inplace = True, drop = True)










### EXPORT THE DF TO A .CSV FILE





# export the new df to a .csv file
record_count_df.to_csv('record_counts.csv', index = False)










'''
QUESTION 2) What proportion of records have remained unbroken from each 
decade? Which records have lasted the longest? What are their event 
categories? What is the average and median amount of time records have 
remained unbroken when grouped by event categories?  Include gender as 
a grouping category
'''










### ADD THE DECADE AND YEARS_LASTED COLUMNS TO THE RECORD_DF





# define a function to return the decade for a date string
def decade_finder(date_str):
    
    # if the value is np.nan, return np.nan
    if pd.isna(date_str):
        
        return np.nan
    
    # return the decade as a string
    return f'{date_str[:3]}0s'



# generate the 'Decade' column
record_df = record_df.assign(Decade = record_df['Date'].apply(decade_finder))



# define a function that takes a date string and returns the number of years since the date
def years_since(date_str):
    
    # if the value is np.nan, return np.nan
    if pd.isna(date_str):
        
        return np.nan
    
    # otherwise, convert the date to a datetime object
    datetime_obj = datetime.strptime(date_str, '%Y-%m-%d')
    
    # get a timedelta of the time between the datetime and now
    td = datetime.now() - datetime_obj
    
    # estimate the number of years since the date
    years_since = round(td.days / 365.25)
    
    # return the estimated number of years since the date
    return years_since



# generate the 'Years_Lasted' column
record_df = record_df.assign(Years_Lasted = record_df['Date'].apply(years_since))










### SAVE ALL RECORDS TO A .CSV FILE





# export the record_df to a .csv file
record_df.to_csv('all_records.csv', index = False)











### GENERATE THE DF WITH RECORD COUNTS GROUPED BY CATEGORY, DECADE, AND GENDER





# group the record counts by event category, gender, and decade, store as decade_groupby
decade_groupby = record_df.groupby(by = ['Category', 'Gender', 'Decade'])['Event'].count()

# grab a list of the multiindices from the above pd.Series, to be used below
multiindex_list = list(decade_groupby.index)

# grab the categories, genders, and decades from the multiindex of the above pd.Series

# initialize with an empty list
categories = []
genders = []
decades = []

# fill the lists with the appropriate values
for tup in multiindex_list:
    
    # append the first element to the categories list
    categories.append(tup[0])
    
    # append the second element to the genders list
    genders.append(tup[1])
    
    # append the third element to the decades list
    decades.append(tup[2])
    
    
    
# generate a pandas dataframe from the four above list/pd.Series objects
decade_count_df = pd.DataFrame({'Category': categories,
                                'Gender': genders,
                                'Decade': decades,
                                'Record_Count': list(decade_groupby)})










### EXPORT THE DF TO A .CSV FILE





# export the data to a .csv file
decade_count_df.to_csv('decade_record_counts.csv', index = False)










### GENERATE THE DF WITH AVG YEARS LASTED GROUPED BY CATEGORY AND GENDER





# group the average years lasted by event category and gender, store as avg_years_lasted_groupby
avg_years_lasted_groupby = record_df.groupby(by = ['Category', 'Gender'])['Years_Lasted'].mean()

# grab a list of the multiindices from the above pd.Series, to be used below
multiindex_list = list(avg_years_lasted_groupby.index)

# grab the categories and genders from the multiindex of the above pd.Series

# initialize with an empty list
categories = []
genders = []

# fill the lists with the appropriate values
for tup in multiindex_list:
    
    # append the first element to the categories list
    categories.append(tup[0])
    
    # append the second element to the genders list
    genders.append(tup[1])
    
    
    
# generate a pandas dataframe from the three above list/pd.Series objects
avg_years_record_df = pd.DataFrame({'Category': categories,
                                    'Gender': genders,
                                    'Average_Years_Lasted': list(avg_years_lasted_groupby)})










### EXPORT THE DF TO A .CSV FILE





# export the df to a .csv file
avg_years_record_df.to_csv('avg_years_record.csv', index = False)










### GENERATE THE DF WITH MED YEARS LASTED GROUPED BY CATEGORY





# group the median years lasted by event category, store as med_years_lasted_groupby
med_years_lasted_groupby = record_df.groupby(by = 'Category')['Years_Lasted'].median()

# grab the index
categories_list = list(med_years_lasted_groupby.index)



# generate a pandas dataframe from the three above list/pd.Series objects
med_years_record_df = pd.DataFrame({'Category': categories_list,
                                    'Median_Years_Lasted': list(med_years_lasted_groupby)})










### EXPORT THE DF TO A .CSV FILE





# export the df to a .csv file
med_years_record_df.to_csv('med_years_record.csv', index = False)


