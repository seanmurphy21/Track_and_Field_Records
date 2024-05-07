# -*- coding: utf-8 -*-











'''
This script will crawl and scrape data from the Wikipedia page 
"List of world records in athletics" found here: 
https://en.wikipedia.org/wiki/List_of_world_records_in_athletics. 
The data in this set consists of 8 tables containing world records for track 
and field events. The tables are broken down according to male versus female 
records, indoor versus outdoor records, and official versus unofficial records.

Data accessed on 4/19/2024.
'''










### import the necessary packages
from bs4 import BeautifulSoup as bs
import requests
import numpy as np
import pandas as pd
from datetime import datetime, time, timedelta










### GRAB ALL THE TABLE CONTENT FROM THE WIKIPEDIA PAGE





# get the website url
url = 'https://en.wikipedia.org/wiki/List_of_world_records_in_athletics'

# make the request
response = requests.get(url)

# get the request content
soup = bs(response.text, features = 'lxml')

# retrieve the data from the table
table_data = soup.find_all('table', {'class': 'wikitable'})










### GET ALL THE TABLE CONTENT INTO A PANDAS DATAFRAME





# grab a list of the tables as dataframes
dataframes_list = pd.read_html(str(table_data), encoding = 'utf-8')

# get rid of the tables we don't want
dataframes_list.pop(2)
dataframes_list.pop(-1)
dataframes_list.pop(-1)

# put the desired column names in a list
desired_columns = ['Event', 'Perf.', 'Athlete(s)', 'Nat.', 'Date', 'Meeting', 'Location', 'Ctry.']

# initialize the new list of dataframes before subsetting them
new_dataframes = []

# subset each dataframe in the dataframes list
for dataframe in dataframes_list:
    
    # to prevent warning messages
    dataframe = dataframe.copy()
    
    # subset the dataframe
    new_dataframe = dataframe[desired_columns]
    
    # add the subsetted dataframe to the list of new dataframes
    new_dataframes.append(new_dataframe)
    
    
    
# define a function to add three new columns to a data frame with 0 values
# new columns will be 'Gender', 'Setting', and 'Official'
def column_adder(df):
    
    # to prevent warning messages
    df = df.copy()
    
    # grab a count of the df rows
    df_row_count = df.shape[0]
    
    # add the 'Gender' column
    df.loc[:, 'Gender'] = np.zeros(df_row_count)
    
    # add the 'Setting' column
    df.loc[:, 'Setting'] = np.zeros(df_row_count)
    
    # add the 'Official' column
    df.loc[:, 'Official'] = np.zeros(df_row_count)
    
    # return the df with the columns added
    return df



# map the function defined above to each df in the new_dataframes list
new_dataframes = list(map(column_adder, new_dataframes))



# define a function to add 'Male' to each value of a column
def male_adder(df):
    
    # to prevent warnings
    df = df.copy()
    
    # grab the number of rows
    df_num_rows = df.shape[0]
    
    # set the values of the 'Gender' column to 'Male'
    df = df.assign(Gender = np.repeat('Male', df_num_rows))
    
    # return the modified dataframe
    return df



# define a function to add 'Female' to each value of a column
def female_adder(df):
    
    # to prevent warnings
    df = df.copy()
    
    # grab the number of rows
    df_num_rows = df.shape[0]
    
    # set the values of the 'Gender' column to 'Female'
    df = df.assign(Gender = np.repeat('Female', df_num_rows))
    
    # return the modified dataframe
    return df



# define a function to add 'Indoor' to each value of a column
def indoor_adder(df):
    
    # to prevent warnings
    df = df.copy()
    
    # grab the number of rows
    df_num_rows = df.shape[0]
    
    # set the values of the 'Setting' column to 'Indoor'
    df = df.assign(Setting = np.repeat('Indoor', df_num_rows))
    
    # return the modified dataframe
    return df



# define a function to add 'Outdoor' to each value of a column
def outdoor_adder(df):
    
    # to prevent warnings
    df = df.copy()
    
    # grab the number of rows
    df_num_rows = df.shape[0]
    
    # set the values of the 'Setting' column to 'Outdoor'
    df = df.assign(Setting = np.repeat('Outdoor', df_num_rows))
    
    # return the modified dataframe
    return df



# define a function to add a boolean True value to the 'Official' column of a df
def official_true_adder(df):
    
    # to prevent warnings
    df = df.copy()
    
    # grab the number of rows
    df_num_rows = df.shape[0]
    
    # set the values of the 'Official' column to True
    df = df.assign(Official = np.repeat(True, df_num_rows))
    
    # return the modified dataframe
    return df



# define a function to add a boolean False value to the 'Official' column of a df
def official_false_adder(df):
    
    # to prevent warnings
    df = df.copy()
    
    # grab the number of rows
    df_num_rows = df.shape[0]
    
    # set the values of the 'Official' column to False
    df = df.assign(Official = np.repeat(False, df_num_rows))
    
    # return the modified dataframe
    return df



# map the male_adder() function to the men's dfs
new_dataframes[::2] = list(map(male_adder, new_dataframes[::2]))

# map the female_adder() function to the women's dfs
new_dataframes[1::2] = list(map(female_adder, new_dataframes[1::2]))

# map the indoor_adder() function to the indoor dfs
new_dataframes[2:4] = list(map(indoor_adder, new_dataframes[2:4]))
new_dataframes[6:] = list(map(indoor_adder, new_dataframes[6:]))

# map the outdoor_adder() function to the outdoor dfs
new_dataframes[0:2] = list(map(outdoor_adder, new_dataframes[0:2]))
new_dataframes[4:6] = list(map(outdoor_adder, new_dataframes[4:6]))

# map the official_true_adder() function to the official WR dfs
new_dataframes[0:4] = list(map(official_true_adder, new_dataframes[0:4]))

# map the official_false_adder() function to the unofficial WR dfs
new_dataframes[4:] = list(map(official_false_adder, new_dataframes[4:]))



# clean up the column names and make them uniform before concatenating
for df in new_dataframes:
    
    # reset the column names to the specified list
    df.columns = ['Event',
                  'Performance',
                  'Athlete',
                  'Nationality',
                  'Date',
                  'Meeting',
                  'Location City',
                  'Location Country',
                  'Gender',
                  'Setting',
                  'Official']
    
    
    
# finally, concatenate all the dataframes into one
record_df = pd.concat(new_dataframes)

# reset the index
record_df.reset_index(drop = True, inplace = True)










### CLEAN THE DATAFRAME










### CLEAN THE ATHLETE COLUMN





# define a function to clean up a messy string of names and return a comma-separated string
def name_cleaner(name_str):
    
    # if the value is a np.nan, just return a np.nan
    if pd.isna(name_str):
        
        return np.nan
    
    # initialize the empty list of names
    names = []
    
    # iterate through the names in a split list of names
    for name in name_str.split():
        
        # initialize the index of the splitting point
        split_index = None
        
        # iterate through the letters after the first letter in the name
        for ii in range(1,len(name)):
            
            # if there is an upper case letter in the middle of the name, check if it is a new name
            if name[ii].isupper():
                
                # makes sure the name is not a short name, like 'CJ'
                if len(name) == 2:
                    
                    pass
                
                # Ways of having uppercase letters in the middle of a name without it being a new name
                elif name[ii + 1] == 'c' or name[ii - 2:ii] == 'La' or name[ii - 2:ii] == 'Mc' or name[ii - 1] == "'" or name[ii - 1] == '-' or name[ii - 1] == '.':
                    
                    pass
                
                # if the uppercase letter is the start of a new name
                else:
                    
                    # retrieve the index position of the start of the new name
                    split_index = ii
                    
                    pass
                
        # if there was only one name, just add the name to the list        
        if split_index is None:
            
            names.append(name)
        
        # if there were two names, separate them with a comma and space
        else:
        
            # use split_index to grab the two separate names
            name_1 = name[:split_index]
        
            name_2 = name[split_index:]
        
            name = name_1 + ', ' + name_2
            
            # append the joined names to the list
            names.append(name)
    
    # return the list of names joined with a space as a single string
    return ' '.join(names)



# apply the function to the 'Athlete' column in record_df
record_df = record_df.assign(Athlete = record_df['Athlete'].apply(name_cleaner))



# manually fix a couple rows with unusual 'Athlete' columns
record_df.at[140, 'Athlete'] = 'Zach Shinnick, Rai Benjamin, Ricky Morgan Jr., Michael Norman'
record_df.at[240, 'Athlete'] = 'Earl McCullouch, Fred Kuller, O. J. Simpson, Lennox Miller (JAM)'









### CLEAN THE DATE COLUMN





# define a function to return a datetime object of the first day of a string that has a range of dates
def date_range_string_parser(date_range_str):
    
    # if the range character is a long hyphen, do the following
    if date_range_str.count('–') > 0:
        
        # grab the first day
        first_day = date_range_str.split('–')[0]
        
        # grab the month and year
        month_and_year = ' '.join(date_range_str.split()[-2:])
        
        # join them togther
        first_day_date = first_day + ' ' + month_and_year
        
        # convert to a datetime object
        dttime_obj = datetime.strptime(first_day_date, '%d %b %Y')
        
        # return the datetime object
        return dttime_obj
    
    # if the range character is a short hyphen, do the following
    else:
        
        # grab the first day
        first_day = date_range_str.split('-')[0]
        
        # grab the month and year
        month_and_year = ' '.join(date_range_str.split()[-2:])
        
        # join them togther
        first_day_date = first_day + ' ' + month_and_year
        
        # convert to a datetime object
        dttime_obj = datetime.strptime(first_day_date, '%d %b %Y')
        
        # return the datetime object
        return dttime_obj
    
    
    
# define a function to convert strings of dates into datetime objects
def datetime_maker(date_str):
    
    # if the value is a np.nan, return np.nan
    if pd.isna(date_str):
        
        return np.nan
    
    # if there is a series of dates, use the function defined above to return the date of first day
    if date_str.count('-') > 0 or date_str.count('–') > 0:
        
        # call the above function
        return date_range_string_parser(date_str)
    
    # if the date is simply a year, treat it separately
    if len(date_str) == 4:
        
        return datetime.strptime(date_str, '%Y')
    
    # make a list of the full month names
    months = ['January',
              'February',
              'March',
              'April',
              'May',
              'June',
              'July',
              'August',
              'September',
              'October',
              'November',
              'December']
    
    # if the date has a full month name, return the datetime following the proper format
    if date_str.split()[1] in months:
        
        return datetime.strptime(date_str, '%d %B %Y')
    
    # otherwise, simply return the datetime following the proper string format
    else:
        
        return datetime.strptime(date_str, '%d %b %Y')
    
    
    
# apply the function to the 'Date' column in record_df
record_df = record_df.assign(Date = record_df['Date'].apply(datetime_maker))










### CLEAN UP THE NATIONALITY COLUMN





# define a function to clean up the 'Nationality' column
def nationality_string_cleaner(nat_string):
    
    # if the value is np.nan, return np.nan
    if pd.isna(nat_string):
        
        return np.nan
    
    # otherwise, return the cleaned string
    return nat_string.replace('\xa0', ', ')



# clean up the multi-nation rows in the 'Nationality' column
record_df = record_df.assign(Nationality = record_df['Nationality'].apply(nationality_string_cleaner))










### ADD THE EVENT CATEGORY COLUMN





# grab a list of the unique events
unique_events = list(set(record_df['Event']))

# define a function to grab all events with a key word and put them into a list
def event_grabber(keyword, event_list):
    
    # initialize empty list
    events = []
    
    # iterate through the events
    for event in event_list:
        
        # if the keyword is in the event description, add the event to the list
        if keyword in event:
            
            events.append(event)
     
    # return the list of events at the end
    return events



# grab the list of jumping events
jumping_events = event_grabber('jump', unique_events) + event_grabber('vault', unique_events)

# grab the list of walking events
walking_events = event_grabber('walk', unique_events)

# grab the list of relay events
relay_events = event_grabber('relay', unique_events)

# grab the list of multisport events
multisport_events = event_grabber('athlon', unique_events)

# grab the list of throwing events
throwing_events = event_grabber('throw', unique_events) + event_grabber('put', unique_events)

# grab the list of hurdle events
hurdle_events = event_grabber('hurdle', unique_events)



# grab all the events that remain
remaining_events = []

# for each event, check if it is in any of the above lists
for event in unique_events:
    
    if event in jumping_events or event in walking_events or event in relay_events or event in multisport_events or event in throwing_events or event in hurdle_events:
        
        pass
    
    # otherwise, append it to the list
    else:
        
        remaining_events.append(event)
        
        
        
# define a function to grab the first num in a string along with the units it is measured in
def first_num_grabber(event_string):
    
    # split the string
    split_elements = event_string.split()
    
    # grab the numeric elements
    
    # initialize with an empty list
    numbers = []
   
    # iterate through the first element
    for char in split_elements[0]:
        
        # check if the chartacter is numeric:
        if char.isnumeric():
            
            # if so, append it to the list of numbers
            numbers.append(char)

    # combine the resulting list of numbers into a single integer
    first_num = int(''.join(numbers))
    
    # define a dict to map keywords to their corresponding units
    unit_map = {'hour': 'hours',
                'm': 'meters',
                'miles': 'miles',
                'y': 'yards',
                'km': 'kilometers'}
    
    # grab the list of unit_map keys to be used below
    unit_map_keys = list(unit_map.keys())
    
    # check for the unit in the first two elements of the event description
    for element in split_elements[0:2]:
        
        # unit keywords are the keys to the dict
        for key in unit_map_keys:
            
            # if the element contains the key
            if key in element:
                
                # grab the units corresponding to the key
                unit = unit_map[key]
        
    # return a tuple of the number and its corresponding units
    return (first_num, unit)



# define a function to classify events into groups based on their distance category
def run_classifier(event_list):
    
    # define a dictionary with empty lists as values for the keys 'sprint', 'middle', and 'long'
    run_class_dict = {'sprint': [],
                      'middle': [],
                      'long': []}
    
    # make a dict containing short, middle, and long distances for each unit
    distance_category_dict = {'yards': {'sprint': range(1, 501),
                                        'middle': range(501, 2000),
                                        'long': range(2001, 50000)},
                              'kilometers': {'sprint': range(0, 1),
                                             'middle': range(1, 3),
                                             'long': range(3, 101)},
                              'meters': {'sprint': range(1, 800),
                                         'middle': range(800, 3000),
                                         'long': range(3000, 100001)},
                              'miles': {'sprint': range(0, 1),
                                        'middle': range(1, 2),
                                        'long': range(2, 101)},
                              'hours': {'sprint': range(0, 1),
                                        'middle': range(0, 1),
                                        'long': range(1, 100)},
                             }
    
    # iterate through the events
    for event in event_list:
        
        # check if the event has a non-numeric first element in its description
        if not event[0].isnumeric():
            
            # if it is the 2 mile run, 1 hour run, or any kind of marathon, it's long distance
            if 'hour' in event or 'miles' in event or 'arathon' in event:
                
                # append the event to the long distance event list
                run_class_dict['long'].append(event)
            
            # if there is a singular 'mile' in the event description
            if ('Mile' in event and not 'Miles' in event) or ('mile' in event and not 'miles' in event):
                
                # the event is middle distance
                run_class_dict['middle'].append(event)
        
        # if the first element in the event description is numeric, grab the number and units
        else:
            
            # call the first_num_grabber function to get the distance in number and units
            distance = first_num_grabber(event)
            
            # if the distance is a sprint, append the event to the sprint list
            if distance[0] in distance_category_dict[distance[1]]['sprint']:
                
                run_class_dict['sprint'].append(event)
                
            # if the distance is middle, append the event to the middle list
            if distance[0] in distance_category_dict[distance[1]]['middle']:
                
                run_class_dict['middle'].append(event)
                
            # if the distance is long, append the event to the long list
            if distance[0] in distance_category_dict[distance[1]]['long']:
                
                run_class_dict['long'].append(event)
                
    # at the end, return the run_class_dict
    return run_class_dict



# call the above classifier function on the list of unique remaining events, save to dict
sprint_mid_long_dict = run_classifier(list(set(remaining_events)))



# make a dictionary of event types in each event category
event_category_dict = {'Sprint': sprint_mid_long_dict['sprint'],
                       'Middle Distance': sprint_mid_long_dict['middle'],
                       'Long Distance': sprint_mid_long_dict['long'],
                       'Relay': relay_events,
                       'Hurdles': hurdle_events,
                       'Walking': walking_events,
                       'Jumping': jumping_events,
                       'Throwing': throwing_events,
                       'Multisport Events': multisport_events
                      }



# define a function that returns the event category as a string based on the event
def event_categorizer(event_str, category_dict = event_category_dict):
    
    # initialize with event_category as None
    event_category = None
    
    # iterate through the keys to grab the corresponding lists
    for category in list(category_dict.keys()):
        
        # if the event_str is in the list of events for that category
        if event_str in category_dict[category]:
            
            # assign category to event_category
            event_category = category
    
    # return the event_category
    return event_category



# finally, create a new column in record_df with the event categories
record_df = record_df.assign(Category = record_df['Event'].apply(event_categorizer))










### CLEAN THE MEETING COLUMN





# define a function to clean meeting column entries
def meeting_cleaner(meeting_str):
    
    # if the value is np.nan, return np.nan
    if pd.isna(meeting_str):
        
        return np.nan
    
    # otherwise, remove the ''\xa0' string from the entries that have it
    else:
        
        return meeting_str.replace('\xa0', ' ')
    
    
    
# apply the above function to record_df to clean the 'Meeting' column
record_df = record_df.assign(Meeting = record_df['Meeting'].apply(meeting_cleaner))



# manually fix a couple rows with unusual 'Meeting' columns
record_df.at[192, 'Meeting'] = 'Great North City Games'
record_df.at[109, 'Meeting'] = 'A Night at the TRACK Presented by New Balance Running'










### GENERATE THREE NEW DATAFRAMES SUBSETTING RECORD_DF ON PERFORMANCE METRIC





# grab all the distance records (measured in meters)
distance_record_df = record_df[~record_df['Performance'].isna() & (record_df['Performance'].str.contains('m') | record_df['Performance'].str.contains('ft'))]

# grab all the pts records
pts_record_df = record_df[~record_df['Performance'].isna() & record_df['Performance'].str.contains('pts')]

# make a list of all the indices of rows not in either of the above subsetted dfs
index_list = list(distance_record_df.index) + list(pts_record_df.index)

# use index_list to generate a boolean array of rows that do not match the specified indices
time_indexer = ~record_df.index.isin(index_list)

# subset record_df to include only timed events
time_record_df = record_df[time_indexer]

# reset the index of the time_record_df
time_record_df.reset_index(drop = True, inplace = True)

# reset the index of the distance_record_df
distance_record_df.reset_index(drop = True, inplace = True)

# reset the index of the pts_record_df
pts_record_df.reset_index(drop = True, inplace = True)










### CLEAN THE DISTANCE_RECORD_DF





# there is one row in the distance_record_df whose performance is measured in ft and in
# let's convert that value to meters
# the value is 12 ft 3 in, which in inches alone is:
row_inches = 12 * 12 + 3

# 1 inch is 0.0254 meters
conversion = 0.0254

# convert to meters
row_meters = round((row_inches * conversion), 2)

# set the value in the df with consistent formatting
distance_record_df.at[35, 'Performance'] = f'{row_meters} m'

# strip the 'm' from each of the performances and convert the column to numeric
distance_record_df = distance_record_df.assign(Performance = distance_record_df['Performance'].apply(lambda x: pd.to_numeric(x[:-2].replace(',', ''))))

# change the 'Performance' column to include the units
distance_record_df.columns = ['Event',
                              'Performance (meters)',
                              'Athlete',
                              'Nationality',
                              'Date',
                              'Meeting',
                              'Location City',
                              'Location Country',
                              'Gender',
                              'Setting',
                              'Official',
                              'Category']










### CLEAN THE PTS_RECORD_DF





# get rid of 'pts' from the 'Performance column' and convert to numeric
pts_record_df = pts_record_df.assign(Performance = pts_record_df['Performance'].apply(lambda x: pd.to_numeric(x[:-4])))

# rename the 'Performance' column to include the units
pts_record_df.columns = ['Event',
                         'Performance (points)',
                         'Athlete',
                         'Nationality',
                         'Date',
                         'Meeting',
                         'Location City',
                         'Location Country',
                         'Gender',
                         'Setting',
                         'Official',
                         'Category']










### CLEAN THE TIME_RECORD_DF





# define a function to clean time strings to proper formats
def time_cleaner(time_string):
    
    # iterate through the string
    for char in time_string:
        
        # if the character is a digit, leave it
        if char.isdigit():
            
            pass
        
        # if the character is either a colon or a period, leave it
        elif char == ':' or char == '.':
            
            pass
        
        # if the character is none of the above, remove it from the string
        else:
            
            time_string = time_string.replace(char, '')
            
    # return time_string at the end
    return time_string



# define a function to take in a time string and return it as a datetime.time object
def str_to_time(time_string):
    
    # if value is not np.nan, use the above function to ensure time_string is in proper format
    if not pd.isna(time_string):
        
        # apply the above function
        time_string = time_cleaner(time_string)
        
    #if the value is a np.nan, just return np.nan
    else:
        
        return np.nan
    
    # if the string has hours, minutes, seconds, and microseconds, this code runs
    if time_string.count(':') == 2 and time_string.count('.') == 1:
        
        # grab the time object in from the proper string format
        time_obj = datetime.strptime(time_string, '%H:%M:%S.%f').time()
    
    # if the string has hours, minutes, and seconds, this code runs
    elif time_string.count(':') == 2 and time_string.count('.') == 0:
        
        # grab the time object in from the proper string format
        time_obj = datetime.strptime(time_string, '%H:%M:%S').time()
        
    # if the string has minutes and seconds, this code runs
    elif time_string.count(':') == 1 and time_string.count('.') == 0:
        
        # grab the time object in from the proper string format
        time_obj = datetime.strptime(time_string, '%M:%S').time()
        
    # if the string has minutes, seconds, and microseconds, this code runs
    elif time_string.count(':') == 1 and time_string.count('.') == 1:
        
        # grab the time object in from the proper string format
        time_obj = datetime.strptime(time_string, '%M:%S.%f').time()
        
    # if the string only has seconds and microseconds, this code runs    
    elif time_string.count(':') == 0 and time_string.count('.') == 1:
        
        # check if the second count is above 59
        # grab the seconds
        sec = int(time_string.split('.')[0])
        
        # check if above 59
        if sec > 59:
            
            # if so, grab the minutes
            mins = int(sec / 60 - (sec % 60))
            
            # grab the remaining seconds
            sec = int(sec % 60)
            
            # grab the microseconds
            microsec = int(time_string.split('.')[1])
            
            # set time_obj to the appropriate value
            time_obj = datetime(1, 1, 1, 0, mins, sec, microsec).time()
        
        # if the seconds are in an acceptable range, then proceed as normal
        else:
            
            # grab the time object in from the proper string format
            time_obj = datetime.strptime(time_string, '%S.%f').time()
        
    # otherwise, you have a nan
    else:
        
        # set time_obj to np.nan
        time_obj = np.nan
    
    # return the time object
    return time_obj



# change the 'Performance' column to datetime.time objects
time_record_df = time_record_df.assign(Performance = time_record_df['Performance'].apply(str_to_time))



# define a function to take in a datetime.time object and return an equivalent timedelta object
def to_timedelta(time_obj):
    
    # if the value is np.nan, return np.nan
    if pd.isna(time_obj):
        
        return np.nan
    
    # convert the datetime.time obj to a datetime.datetime obj
    datetime_obj = datetime.combine(datetime.min, time_obj)
    
    # return the timedelta object
    return datetime_obj - datetime.min



# change the 'Performance' column to timedelta objects
time_record_df = time_record_df.assign(Performance = time_record_df['Performance'].apply(to_timedelta))










### EXPORT THE DATA TO .CSV FILES





# export the record_df to a .csv file
record_df.to_csv('all_track_and_field_records.csv', index = False)



# export the time_record_df to a .csv file
time_record_df.to_csv('timed_track_and_field_records.csv', index = False)



# export the pts_record_df to a .csv file
pts_record_df.to_csv('points_track_and_field_records.csv', index = False)



# export the distance_record_df to a .csv file
distance_record_df.to_csv('distance_track_and_field_records.csv', index = False)