

import sys

import pandas as pd
from matching.games import HospitalResident

#################### Functions #################################################


def concat_columns(df, category, num_cols):
    '''Smaller data cleaning preparation function
       to concat multiple columns accross categories.
       Assumes naming conventions accross categories of
       variable_1, variable_2 etc. '''

    df[category] = ''
    counter = 1
    while counter < num_cols + 1:
        df[category] += df[category + '_' + str(counter)] + ', '
        counter += 1

    return df[category]

################################################################################


def clean_data(file_path,
               col_names,
               cols_use,
               sort_on,
               concat_col,
               num_cols_concat,
               drop_na_subset):
    '''This was an attempt to generalize file cleaning prior to matching,
       but it was not worth the effort that would be necessary
       to generalize broadly enough for multiple applications'''

    file = file_path

    df = pd.read_excel(file,
                       usecols=cols_use,
                       names=col_names)\
        .dropna(subset=drop_na_subset)\
        .sort_values(sort_on)

    for category in concat_col:
        concat_columns(df, category, num_cols_concat)

    df_as_dict = df.fillna('')\
                   .reset_index(drop=True)\
                   .to_dict(orient='index')

    return df_as_dict

################################################################################


def score(group_one, group_two, i, row):
    ''' This function scores pairs on how high they match based
        on weighted categories. Unfortunately, as currently
        constructed this will not extrapolate outside of this context.'''

    categories = ['lived_experience',
                  'professional_skills',
                  'leadership_skills']

    acceptable_regions = {'NAM': ['NAM', 'LAC', 'EUR'],
                          'LAC': ['LAC', 'NAM', 'EUR'],
                          'AP': ['MEA', 'AP', 'EUR'],
                          'MEA': ['AP', 'EUR', 'MEA'],
                          'EUR': ['NAM', 'LAC', 'EUR']}
    score = 0

    if group_one[i].get('time_zone') == group_two[row].get('time_zone'):
        score += 100

    elif group_one[i].get('region') \
            in acceptable_regions.get(group_two[row].get('region')):
        score += 75

    if group_one[i].get('gender_pref') == group_two[row].get('gender'):
        score += 15

    if str(group_one[i].get('bus_unit')) == str(group_two[row].get('bus_unit')):
        score += 5

    for category in categories:
        counter = 1
        while counter < 4:

            if group_one[i].get(category + '_' + str(counter)) \
               in str(group_two[row].get(category)):
                score += 3

            counter += 1
        counter = 1

    return score

################################################################################


def rank_matches(group_one, group_two):
    '''This takes each person in the first group and runs the scoring
       function for every possible person in group 2. It then sorts
       possibile pairings from highest to lowest. The final output is
       a dictionary where for k, v key is every member of group one and
       value is a sorted descending list of every possible match in group 2. '''

    group_one_pref = {}

    for i in group_one:
        name = group_one[i].get('name')
        pref = []

        for row in group_two:
            x = group_two[row].get('name')
            scoring = score(group_one, group_two, i, row)
            pref.append((x, scoring))

        pref.sort(key=lambda y: y[1])
        pref = [i[0] for i in pref]

        group_one_pref[name] = pref

    return group_one_pref

################################################################################


def capacity(hosting_group):
    '''This function creates a dictionary of the hosting group where for
       every k, v pair key = name of the group member and value = their hosting
       capacity.'''

    capacity = {}

    for i in hosting_group:
        key = hosting_group[i].get('name')
        capacity[key] = hosting_group[i].get('number_to_mentor')

    return capacity


#################### Mentee File ##############################################
mentees_file = "mentees.xlsx"

column_names = ['id',
                'name',
                'email',
                'previous_participant',
                'aq_y_n',
                'aq_c',
                'bus_unit',
                'region',
                'time_zone',
                'bus_unit_pref',
                'gender_pref',
                'lived_experience_1',
                'lived_experience_2',
                'lived_experience_3',
                'professional_skills_1',
                'professional_skills_2',
                'professional_skills_3',
                'leadership_skills_1',
                'leadership_skills_2',
                'leadership_skills_3']


concat_col = ['lived_experience',
              'professional_skills',
              'leadership_skills']

mentees = clean_data(mentees_file,
                     column_names,
                     list(range(17, 37)),
                     'region',
                     concat_col,
                     3,
                     ['bus_unit', 'region'])

#################### Mentor File ###############################################

mentors_file = "mentors.xlsx"

column_names_1 = ['id',
                  'name',
                  'email',
                  'gender',
                  'bus_unit',
                  'region',
                  'time_zone',
                  'bus_unit_pref',
                  'capacity',
                  'lived_experience_1',
                  'lived_experience_2',
                  'lived_experience_3',
                  'professional_skills_1',
                  'professional_skills_2',
                  'professional_skills_3',
                  'leadership_skills_1',
                  'leadership_skills_2',
                  'leadership_skills_3']

mentors = pd.read_excel(mentors_file,
                        usecols=list(range(17, 35)),
                        names=column_names_1)\
    .sort_values('region')

mentors.bus_unit = mentors.bus_unit.str.replace('Product and Engineering',
                                                'Product & Engineering')

categories = ['lived_experience',
              'professional_skills',
              'leadership_skills']

for category in categories:
    concat_columns(mentors, category, 3)

mentors = mentors.fillna('')\
                 .to_dict(orient='index')

################################################################################
#################### Solve the Game ############################################

sys.setrecursionlimit(10**6)

all_mentees_pref = rank_matches(mentees, mentors)
all_mentors_pref = rank_matches(mentors, mentees)
mentor_capacity = capacity(mentors)


game = HospitalResident.create_from_dictionaries(
    all_mentees_pref, all_mentors_pref, mentor_capacity)

matching = game.solve(optimal="resident")

matches = pd.DataFrame.from_dict(matching, orient='index').reset_index()
matches.to_csv('matches.csv')
