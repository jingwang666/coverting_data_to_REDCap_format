
# -*- coding: utf-8 -*-
"""
Created on Mon May 10 15:03:51 2021

@author: jingwang666@GitHub

"""

# =============================================================================
# abbr.:
#     var -- variable
#     vars -- variables
# =============================================================================


import pandas as pd
import numpy as np
import datetime
import string
import re

folder9_states = r'\\cdc.gov\project\NCEZID_DHCPP_IDPB_Operations\Staff_Folders\Wang\1_Projects\9_CSTE_project_01112021\Submitted by States'
folder9_states_backup = r'\\cdc.gov\project\NCEZID_DHCPP_IDPB_Operations\Staff_Folders\Wang\1_Projects\9_CSTE_project_01112021\Submitted by States\backup_files'
folder9_states_datafiles = r'\\cdc.gov\project\NCEZID_DHCPP_IDPB_Operations\Staff_Folders\Wang\1_Projects\9_CSTE_project_01112021\Submitted by States\original_data'
# folder_onedrive_crosswalk = r'C:\Users\ogf7\CDC\NCEZID-DHCPP_IDPB_Epi_Ops - CSTE Unexplained Deaths CoAg\RedCap'

date_today = datetime.datetime.now().strftime('%m%d%y')
# import the data file and crosswalk file
filename_nyc = r'\Possible Viral Resp Deaths through 4-25-21 CDC upload 4-28-21.xlsx'
filename_nyc_crosswalk = r'\NYC_OCME_CSTE_crosswalk.xlsx'
zdf_template = pd.read_csv(folder9_states + r'\SurveillanceForUnexplainedResp_ImportTemplate_2021-06-11.csv'  )
zdf_nyc = pd.read_excel(folder9_states_datafiles + filename_nyc  )
zdf_nyc_crosswalk = pd.read_excel(folder9_states + filename_nyc_crosswalk)
zdf_nyc_crosswalk.to_excel(folder9_states_backup + r'\NYC_OCME_CSTE_crosswalk_' + date_today +  '.xlsx', index=False)


# initiation - reset the headers of the data file  
list_col_names = zdf_nyc.iloc[0,:]
zdf_nyc.set_axis(list_col_names, axis='columns', inplace = True)     
zdf_nyc = zdf_nyc.iloc[1:,:]


def format_date(zdf, list_vars_date):
    list_cols = list(zdf.columns)
    for x in list_vars_date:
        if x in list_cols:
            if zdf[x].dtypes != object:
                zdf[x] = zdf[x].dt.strftime('%m/%d/%Y')
            else:
                continue
        else:
            continue
    return zdf



def nyc():
    
    #%%
    
# =============================================================================
#     vars to directly rename
# =============================================================================
    # select the vars in crosswalk file that can be renamed directly
    # zdf_nyc_crosswalk_var_to_rename_directly = zdf_nyc_crosswalk[ zdf_nyc_crosswalk['coding_note'] == 'ok'] [['Variable','redcap_variable_name']]
    zdf_nyc_crosswalk_var_to_rename_directly = zdf_nyc_crosswalk[ zdf_nyc_crosswalk['coding_type'] == 'ok'] [['Variable','redcap_variable_name']]
    # get the list of the vars to be renamed directly and the list of the new names
    list_keys_var_names_old = zdf_nyc_crosswalk_var_to_rename_directly['Variable'].to_list()
    list_values_var_names_new = zdf_nyc_crosswalk_var_to_rename_directly['redcap_variable_name'].to_list()
    dic1 = dict(zip(list_keys_var_names_old, list_values_var_names_new))



# process numeric variables
    def process_numeric_var(var):
        # var = 'Age'
        var_original = var + '0'
        # var_original
        # zdf_nyc2['Age']
        zdf_nyc2[var_original] = zdf_nyc2[var]
        zdf_nyc2[var] = zdf_nyc2[var].astype('str')
        zdf_nyc2.loc[ zdf_nyc2[var].str[0].str.upper().isin(list(string.ascii_uppercase)) , var ] = '999'
        # zdf_nyc2.loc[ zdf_nyc2[var].str[0:3].str.upper().isin(['UNK', 'N/A']) , var ] = '999'
        zdf_nyc2[var].replace(['nan', ''], '999',inplace=True)
        zdf_nyc2.loc[ ( zdf_nyc2[var].isna() == True ), var ] = '999' 
        zdf_nyc2[var] = zdf_nyc2[var].astype('float')
        zdf_nyc2[var].replace([999], np.nan, inplace=True)
        print("'{}', '{}',".format(var_original, var))
     
 # process categorical variables
    def process_categoric_var(var):
        # var = 'Race'
        var_original = var + '0'
        zdf_nyc2[var_original] = zdf_nyc2[var]
        zdf_nyc2[var] = zdf_nyc2[var].astype('str')
        zdf_nyc2[var] = zdf_nyc2[var].str.upper()
        zdf_nyc2.loc[ ( zdf_nyc2[var].isna() == True ), var ] = 'N/A'
        zdf_nyc2.loc[ ( zdf_nyc2[var] == 'NAN' ), var ] = 'N/A'
        print("'{}', '{}',".format(var_original, var))
 

 # process date 
    def get_date_pattern(x, pattern):
        return re.search(pattern, x ).group(0) if re.search(pattern, x ) else 'N/A'
        # x = '~1/1/2021'
        # pattern = r'\d{1,2}/\d{1,2}/\d{2,4}'
        # re.search(pattern, x ).group(0)
    
    def process_date(var):
        var_original = var + '00'
        zdf_nyc2[var_original] = zdf_nyc2[var]
        list_dates = zdf_nyc2[var].to_list()
        list_dates2 = [ x.strftime('%m/%d/%Y') if type(x) == datetime.datetime else x for x in list_dates]
        zdf_nyc2[var] = list_dates2
        process_categoric_var(var) 
        zdf_nyc2[var] = zdf_nyc2[var].apply(lambda x: get_date_pattern(x, r'\d{1,2}/\d{1,2}/\d{2,4}'))
        zdf_nyc2.loc[ zdf_nyc2[var] == 'N/A', var ] = np.nan 
      
    def convert_value_int_to_str(var):
        zdf_nyc2[ var ].replace( [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, ], [ '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10' ],inplace=True )

# =============================================================================
#     process the variables
# =============================================================================

# rename the vars in data file
    zdf_nyc2 = zdf_nyc.rename( columns= dic1)  
    zdf_nyc2.rename( columns= {'Fever: Subjective or Measured >100.4':'fever_type'}, inplace = True) 
    
   
# add variables   
    zdf_nyc2['res_juris'] = '3' # 3 = NYC
    zdf_nyc2['resp_death_definition'] = 1 # 1 means Yes
    num_records = zdf_nyc2['res_juris'].count()
    list_record_numbers = [ x+1 for x in range(num_records)]
    zdf_nyc2['record_id'] = list_record_numbers
  
# info_source

    process_categoric_var('info_source___1')
    zdf_nyc2.loc[ zdf_nyc2['info_source___1'].str[0] == 'Y', 'info_source___1' ] = 1
    zdf_nyc2.loc[ zdf_nyc2['info_source___1'] != 1 , 'info_source___1' ] = 0
    
    process_categoric_var('info_source___2')
    zdf_nyc2.loc[ zdf_nyc2['info_source___2'].str[0] == 'Y', 'info_source___2' ] = 1
    zdf_nyc2.loc[ zdf_nyc2['info_source___2'] != 1 , 'info_source___2' ] = 0
        
 # age
    process_numeric_var('Age')
    process_categoric_var('age_units')
    # zdf_nyc2['Age'] = zdf_nyc2['Age'].astype('str')
    # zdf_nyc2.loc[ zdf_nyc2['Age'].str[0:3].str.upper() == 'UNK', 'Age' ] = '999'
    # zdf_nyc2['Age'].replace(['nan', ''], '999',inplace=True)
    # zdf_nyc2['Age'] = zdf_nyc2['Age'].astype('int')
    
    # zdf_nyc2.columns
    zdf_nyc2['age_years'] = 999
    zdf_nyc2['age_months'] = 999
    zdf_nyc2.loc[ zdf_nyc2['age_units'].str[0:3] == 'YEA', 'age_years' ] = zdf_nyc2['Age']
    zdf_nyc2.loc[ zdf_nyc2['age_units'].str[0:3] == 'MON', 'age_months' ] = zdf_nyc2['Age']
    # 0, Months | 1, Years
    zdf_nyc2.loc[ zdf_nyc2['age_units'].str[0:3] == 'YEA', 'age_units' ] = 1
    zdf_nyc2.loc[ zdf_nyc2['age_units'].str[0:3] == 'MON', 'age_units' ] = 0
    zdf_nyc2['age_years'].replace([999], np.nan, inplace=True)
    zdf_nyc2['age_months'].replace([999], np.nan, inplace=True)
    zdf_nyc2['age_units'].replace( 'N/A', np.nan, inplace=True)

# Obesity
    process_numeric_var('BMI')
    
    # if BMI is unknown and Obesity is Yes, then obese_bmi_unk = 1    
    zdf_nyc2.loc[ ( zdf_nyc2['BMI'].isna() == True ) & (zdf_nyc2['Obesity'].str.upper() == 'YES' ), 'obese_bmi_unk' ] = 1    
    zdf_nyc2.loc[ zdf_nyc2['BMI'] >= 40 , 'severe_obese' ] = '1'
    zdf_nyc2.loc[ (zdf_nyc2['BMI'] >= 30) & (zdf_nyc2['BMI'] < 40 ) , 'obese_30_39' ] = 1
 


# process patterns
    def get_pattern(x, pattern):
        return 1 if re.search(pattern, x ) else 0

# Sex & Race typos!!!
    zdf_nyc2['typo_sex'] = zdf_nyc2['sex']
    zdf_nyc2['typo_race'] = zdf_nyc2['Race']
    
    zdf_nyc2.loc[ zdf_nyc2['typo_sex'] == 'H', 'Race' ] = 'H'
    zdf_nyc2.loc[ zdf_nyc2['typo_race'].isin(['M','F']), 'sex' ] = zdf_nyc2['typo_race']
    
    process_categoric_var('sex')
    
    # 0, Male | 1, Female | 2, Other | 3, Unknown
    zdf_nyc2.loc[ zdf_nyc2['sex'] == 'M', 'sex' ] = 0
    zdf_nyc2.loc[ zdf_nyc2['sex'] == 'F', 'sex' ] = 1
    zdf_nyc2.loc[ zdf_nyc2['sex'] == 'N/A', 'sex' ] = 3
    zdf_nyc2.loc[ ~ zdf_nyc2['sex'].isin( [ 0, 1, 3 ] ), 'sex' ] = 2
    
    # zdf_nyc2.sex.value_counts()
    
    

# Race and ethnicity
    process_categoric_var('Race')

    def get_pattern_new_var(var, pattern):
        zdf_nyc2[var]= zdf_nyc2["Race"].apply(lambda x: get_pattern(x, pattern)) 
   
    # get race
    get_pattern_new_var("race___asian", r"^A.*/?P?.*")
    get_pattern_new_var("race___black", r"^B.*")
    get_pattern_new_var("race___white", r"^W.*")
    get_pattern_new_var("race___unk", r"^UNK.*")

    zdf_nyc2.loc[ zdf_nyc2['Race'].str.count('EAST INDIAN') > 0, 'race___asian'] = 1
    zdf_nyc2.loc[ zdf_nyc2['Race'] == 'BW', 'race___white' ] = 1 # race =  White as well besides black
    zdf_nyc2.loc[ zdf_nyc2['Race'].str.count('UNK') > 0, 'race___unk'] = 1
    zdf_nyc2.loc[ zdf_nyc2['Race'] == 'N/A', 'race___unk' ] = 1
    zdf_nyc2.loc[ zdf_nyc2['Race'] == 'M', 'race___unk' ] = 1

    # get ethnicity
    get_pattern_new_var("ethnicity", r"^H.*") # this will overwrite the origianl ethnicity var after renaming process

    # special cases of race and ethnicity manually handled 
    list_races_to_sum = [ 'race___asian','race___black', 'race___white', 'race___unk',  ]
    zdf_nyc2['race_sum'] =  zdf_nyc2[list_races_to_sum].sum(axis=1)
    zdf_nyc2.loc[ (zdf_nyc2['ethnicity'] == 1  ) & ( zdf_nyc2['race_sum'] == 0 ), 'race___unk' ] = 1
    

# Location of death
    process_categoric_var('loc_death') 
    zdf_nyc2["loc_death1"] = zdf_nyc2["loc_death"]
    
    def get_pattern_new_value_LocDeath(pattern, value):
        zdf_nyc2.loc[ zdf_nyc2["loc_death1"].apply(lambda x: get_pattern(x, pattern)) == 1, 'loc_death'] = value
      
    # 1, Home  2, ED  3, DOA in ED  4, Hospital  5, LTCF/Nursing home  
    # 6, Hospice facility  7, Unknown  8, Other
    
    # by default, loc_death = 8, other
    zdf_nyc2['loc_death'] = 8 
    
    get_pattern_new_value_LocDeath(r'^RESIDENCE.*', 1)
    get_pattern_new_value_LocDeath(r'^EMERGENCY.*', 2)
    get_pattern_new_value_LocDeath(r'^HOSPITAL.*', 4)
    get_pattern_new_value_LocDeath(r'^MEDICAL FACILITY.*', 4)
    get_pattern_new_value_LocDeath(r'N/A', 7)
    get_pattern_new_value_LocDeath(r'.+RESIDENCE.*', 8) # e.g. other's residence = other
    
    zdf_nyc2.loc[ zdf_nyc2['loc_death'] == 8, 'death_loc_other' ] = zdf_nyc2['loc_death0']
   
# death_date_m_d_y
    process_date('death_date_m_d_y')
    
# date_recent_med_visit

    # recent_med_visit
    zdf_nyc2["date_recent_med_visit00"] = zdf_nyc2["date_recent_med_visit"]

    list_date_recent_med_visit = zdf_nyc2['date_recent_med_visit'].to_list()
    list_date_recent_med_visit2 = [ x.strftime('%m/%d/%Y') if type(x) == datetime.datetime else x for x in list_date_recent_med_visit]
    zdf_nyc2['date_recent_med_visit'] = list_date_recent_med_visit2
    
    process_categoric_var('date_recent_med_visit') 

    
    zdf_nyc2['date_recent_med_visit'] = zdf_nyc2['date_recent_med_visit'].apply(lambda x: get_date_pattern(x, r'\d{1,2}/\d{1,2}/\d{2,4}'))
    zdf_nyc2.loc[ zdf_nyc2['date_recent_med_visit'] == 'N/A', 'date_recent_med_visit' ] = np.nan                           

    # 1, Yes, date known | 2, Yes, date unknown | 0, No | 4, Unknown
    zdf_nyc2.loc[ zdf_nyc2['date_recent_med_visit'].isna(), 'recent_med_visit' ] = '4'  
    zdf_nyc2.loc[ ~ zdf_nyc2['date_recent_med_visit'].isna(), 'recent_med_visit' ] = '1'                           


# symptoms      
      
    def get_pattern_new_value_sx(var, var1, pattern, value):
        zdf_nyc2.loc[ zdf_nyc2[var1].apply(lambda x: get_pattern(x, pattern)) == 1, var] = value

    def get_yes1_no0_unk2(var):
        process_categoric_var(var)
        var1 = var + '1'
        zdf_nyc2[var1] = zdf_nyc2[var]
        get_pattern_new_value_sx(var, var1, r'^Y.*', '1')
        get_pattern_new_value_sx(var, var1, r'^NO.*', '0') #be careful: "N" will retrieve both "NO" and "N/A"
        get_pattern_new_value_sx(var, var1, r'^UN.*', '2')
        get_pattern_new_value_sx(var, var1, r'N/A', np.nan)
        
    list_vars_symptom = [ 'cough', 'rhinorrhea', 'sob', 'sorethroat_pharyngitis', 'chills', 
                     'loss_taste_smell', 'chestpain', 'fatigue', 'myalgia', 'headache', 
                     'nausea_vomit', 'abdominal_pain', 'diarrhea', 
                     ]  
    for var in list_vars_symptom: 
        get_yes1_no0_unk2(var)
        
    list_vars_symptom0 = [ x +'0' for x in list_vars_symptom] 
    list_vars_symptom1 = [ x +'1' for x in list_vars_symptom] 
    list_vars_symptom_012 = []
    for i in range(len(list_vars_symptom)):
        list_vars_symptom_012 = list_vars_symptom_012 + [list_vars_symptom0[i], list_vars_symptom1[i], list_vars_symptom[i]]

# fever
    process_categoric_var('fever')
    process_categoric_var('fever_type')
    zdf_nyc2.loc[ ( zdf_nyc2['fever'] == 'YES' ) & (zdf_nyc2['fever_type'].str[0:3] == 'SUB' ), 'subjective_fever' ] = '1' 
    zdf_nyc2.loc[ ( zdf_nyc2['fever'] == 'YES' ) & (zdf_nyc2['fever_type'].str[0:3] == 'THE' ), 'documented_fever' ] = 1 
    # zdf_nyc2.loc[ ( zdf_nyc2['fever'] == 'YES' ) & (zdf_nyc2['fever_type'] == 'N/A' ), 'unspecified_fever' ] = 1 

    list_vars_symptom_all = [ 'subjective_fever', 'documented_fever'] + list_vars_symptom

# date of onset
    
    process_date('symptom_onset_date_m_d_y')
    
    # symptom_date: Is symptom onset date known?
    # 1, Yes | 0, No | 2, Asymptomatic
    zdf_nyc2.loc[ zdf_nyc2['symptom_onset_date_m_d_y'].isna(), 'symptom_date' ] = 0                            

# vaccine
        
    # covid_vaccine = 1, Yes | 0, No | 2, Unknown
    get_yes1_no0_unk2('covid_vaccine')
    
    
    # vaccine_manufact = 1, Pfizer-BioNTech | 2, Moderna | 3, Johnson & Johnson | 4, Other | 5, Unknown
    process_categoric_var('vaccine_manufact')
    zdf_nyc2.loc[  (zdf_nyc2['vaccine_manufact'].str[0:3] == 'PFI' ), 'vaccine_manufact' ] = 1 
    zdf_nyc2.loc[  (zdf_nyc2['vaccine_manufact'].str[0:3] == 'MOD' ), 'vaccine_manufact' ] = 2 
    zdf_nyc2.loc[  (zdf_nyc2['vaccine_manufact'].str[0:3] == 'JOH' ), 'vaccine_manufact' ] = 3  
    zdf_nyc2.loc[  (zdf_nyc2['vaccine_manufact'].str[0:3] == 'UNK' ), 'vaccine_manufact' ] = 5 
    zdf_nyc2.loc[  (zdf_nyc2['covid_vaccine'] == 1 ) &  (zdf_nyc2['vaccine_manufact'] == 'N/A' ) , 'vaccine_manufact' ] = 5 
    zdf_nyc2.loc[ ~ zdf_nyc2['vaccine_manufact'].isin([1,2,3,5, 'N/A' ]), 'vaccine_manufact' ] = 4 
    zdf_nyc2.loc[ zdf_nyc2['vaccine_manufact'] == 'N/A', 'vaccine_manufact' ] = np.nan
    
     # vaccine_doses =  1, 1 | 0, 2 | 2, Unknown | 3, Other
    process_numeric_var('vaccine_doses')
    zdf_nyc2['vaccine_doses'].replace(2,0, inplace= True)
    zdf_nyc2.loc[  ( zdf_nyc2['covid_vaccine'] == 1 ) & ( zdf_nyc2['vaccine_doses'].isna() )  , 'vaccine_doses' ] = 2 # unknown
    zdf_nyc2.loc[ ( ~ zdf_nyc2['vaccine_doses'].isna() ) & (~ zdf_nyc2['vaccine_doses'].isin([1,0, 2 ]) ), 'vaccine_doses' ] = 3 # other 
    
    
    process_date('dose_1_date')
    process_date('dose_2_date')
    # zdf_nyc2.loc[ ( zdf_nyc2['covid_vaccine'] == 1 )  & ( zdf_nyc2['vaccine_doses'].isna() ) & ( zdf_nyc2['dose_2_date'].isna() ) & ( ~ zdf_nyc2['dose_1_date'].isna() )  , 'vaccine_doses' ] = 1 # dose 1
    # zdf_nyc2.loc[ ( zdf_nyc2['covid_vaccine'] == 1 )  & ( zdf_nyc2['vaccine_doses'].isna() ) & ( ~ zdf_nyc2['dose_2_date'].isna() )   , 'vaccine_doses' ] = 2 # dose 1
    
    
# underlying conditions

    def get_list_vars_0_1_original(list_vars):
        list_vars0 = [ x +'0' for x in list_vars] 
        list_vars1 = [ x +'1' for x in list_vars] 
        list_vars_0_1_2 = []
        for i in range(len(list_vars)):
            list_vars_0_1_2 = list_vars_0_1_2 + [list_vars0[i], list_vars1[i], list_vars[i]]
        return list_vars_0_1_2
    

    def underlying_conditions( ):
        list_vars = [ 'sickle_cell', 'Tobacco use', 
                     'diabetes', 'hypertension', 'cardiovascular_disease', 'chronic_lung_dis',
                            'chronic_kidney_dis', 'chronic_liver_dis', 'cancer', 'immune_supp',
                            'vaping', 'subs_misuse', 'disability', 
                            'psychiatric_diagnosis', 'pregn', 'other_med', 
                            
                     ]  
        # var = 'diabetes'
        for var in list_vars: 
            get_yes1_no0_unk2(var)
           
        # if any other value, then = 1, Yes
        for var in list_vars: 
            zdf_nyc2.loc[ ( ~ zdf_nyc2[var].isna() ) & (~ zdf_nyc2[var].isin([1,2,0 ]) ), var ] = 1 # Yes

        global list_vars_conditions, list_vars_conditions_0_1_2
        list_vars_conditions =  list_vars
        list_vars_conditions_0_1_2 = get_list_vars_0_1_original(list_vars)
        
    underlying_conditions()
    
    
    zdf_nyc2.loc[  (zdf_nyc2['Tobacco use'] == 1 ) &  (zdf_nyc2['Tobacco:  Current or Former'].str.upper().str[0:3] == 'CUR' ) , 'cig_smoke' ] = 1
    zdf_nyc2.loc[  (zdf_nyc2['Tobacco use'] == 1 ) &  (zdf_nyc2['Tobacco:  Current or Former'].str.upper().str[0:3] == 'FOR' ) , 'cig_smoke_former' ] = 1



    zdf_nyc2.loc[  (zdf_nyc2['specify_e_cig'] == 'yes' ), 'specify_e_cig' ] = np.nan 
    zdf_nyc2.loc[  (zdf_nyc2['specify_disability'] == 'yes' ), 'specify_disability' ] = zdf_nyc2['disability0'] 
    zdf_nyc2.loc[ ~(zdf_nyc2['specify_neuro_condition'].isna() ), 'neuro_condition' ] = 1 # Yes
    zdf_nyc2.loc[ ~(zdf_nyc2['specify_organ_transplant'].isna() ), 'organ_stem_transplant' ] = 1 # Yes
    zdf_nyc2.loc[ ~(zdf_nyc2['specify_autoimmune'].isna() ), 'autoimmune' ] = 1 # Yes
    zdf_nyc2.loc[ ~(zdf_nyc2['specify_autoimmune'].isna() ) & (zdf_nyc2['specify_immunosup'].isna() ), 'specify_immunosup' ] = zdf_nyc2['specify_autoimmune'] 
    zdf_nyc2.loc[ ~(zdf_nyc2['Hematologic'].isna() ) & (zdf_nyc2['specify_immunosup'].isna() ), 'specify_immunosup' ] = zdf_nyc2['Hematologic'] 
    
    
    
    list_vars_conditions_all = [
        'diabetes', 'hypertension', 'cardiovascular_disease', 'chronic_lung_dis', 'specify_chronic_lung_dis',
        'obese_30_39', 'severe_obese', 'obese_bmi_unk', 'chronic_kidney_dis', 'chronic_liver_dis', 
        'specify_chronic_liver_dis', 'cancer', 'specify_cancer', 'autoimmune', 'immune_supp', 'specify_immunosup','organ_stem_transplant',
        'cig_smoke', 'cig_smoke_former', 'vaping', 'specify_e_cig', 'subs_misuse', 'disability', 
        'specify_disability', 'neuro_condition', 'specify_neuro_condition', 'psychiatric_diagnosis', 
        'specify_psych_condition', 'pregn', 'other_med', 'specify_other_condition', 
        ]
    
    
# autopsy 
    
    process_date('autopsy_date')
    get_yes1_no0_unk2('autopsy_performed')
    zdf_nyc2.loc[  ( ~ zdf_nyc2['autopsy_date'].isna() )  , 'autopsy_performed' ] = 1 # yes 


# np_swab testing

    # np_swab_yn = yesno
    get_yes1_no0_unk2('np_swab_yn')
    process_date('np_swab_date')

    
    # if testing has been performed
    process_categoric_var('nyc_type_viral_test')

    list_vars_if_PCR_done = ['if_covid_npswab_pcr' , 'if_influenza_a_npswab_pcr', 'if_influenza_b_npswab_pcr', 'if_rpp_npswab_pcr']
    for var in list_vars_if_PCR_done:
        zdf_nyc2[var] = 0
    zdf_nyc2.loc[ zdf_nyc2[ 'nyc_type_viral_test' ].str.find('COVID') >= 0  , 'if_covid_npswab_pcr' ] = 1 
    zdf_nyc2.loc[ zdf_nyc2[ 'nyc_type_viral_test' ].str.find('INFLUENZA A') >= 0  , 'if_influenza_a_npswab_pcr' ] = 1
    zdf_nyc2.loc[ zdf_nyc2[ 'nyc_type_viral_test' ].str.find('INFLUENZA B') >= 0  , 'if_influenza_b_npswab_pcr' ] = 1
    zdf_nyc2.loc[ zdf_nyc2[ 'nyc_type_viral_test' ].str.find('BIOFIRE') >= 0  , 'if_rpp_npswab_pcr' ] = 1 
    zdf_nyc2.loc[  
        ( zdf_nyc2[ 'nyc_type_viral_test' ].str.find('INFLUENZA') >= 0 ) 
        & ( zdf_nyc2[ 'nyc_type_viral_test' ].str.find('INFLUENZA A') == -1 ) 
        & ( zdf_nyc2[ 'nyc_type_viral_test' ].str.find('INFLUENZA B') == -1 )  , 'if_influenza_a_npswab_pcr' ] = 1
    zdf_nyc2.loc[  
        ( zdf_nyc2[ 'nyc_type_viral_test' ].str.find('INFLUENZA') >= 0 ) 
        & ( zdf_nyc2[ 'nyc_type_viral_test' ].str.find('INFLUENZA A') == -1 ) 
        & ( zdf_nyc2[ 'nyc_type_viral_test' ].str.find('INFLUENZA B') == -1 )  , 'if_influenza_b_npswab_pcr' ] = 1
    
    # results of the testing
    # covid_npswab_pcr = 1, Positive | 2, Negative | 3, Not performed
    process_categoric_var('nyc_result_viral_test')
    
    list_vars_results_PCR = ['result_covid_npswab_pcr' , 'result_influenza_a_npswab_pcr', 'result_influenza_b_npswab_pcr', 'result_rpp_npswab_pcr']
    for var in list_vars_results_PCR:
        zdf_nyc2[var] = 2
        
    for var in list_vars_results_PCR:        
        zdf_nyc2.loc[ zdf_nyc2[ 'nyc_result_viral_test' ] == 'POSITIVE'  , var ] = 1
    for var in list_vars_results_PCR:        
        zdf_nyc2.loc[ zdf_nyc2[ 'nyc_result_viral_test' ] == 'NEGATIVE'  , var ] = 2
        zdf_nyc2.loc[ zdf_nyc2[ 'nyc_result_viral_test' ] == 'ALL NEGATIVE'  , var ] = 2 
         
    zdf_nyc2.loc[ zdf_nyc2[ 'nyc_result_viral_test' ].str.find('POSITIVE COVID') >= 0 , 'result_covid_npswab_pcr' ] = 1 
    zdf_nyc2.loc[ zdf_nyc2[ 'nyc_result_viral_test' ].str.find('POSITIVE FOR COVID') >= 0 , 'result_covid_npswab_pcr' ] = 1 
    
    special_value_result_pcr = 'NEGATIVE FOR COVID AND INFLUENZA; ADENOVIRUS AND HUMAN RHINOVIRUS/ENTEROVIRUS DETECTED' 
    zdf_nyc2.loc[ zdf_nyc2[ 'nyc_result_viral_test' ] == special_value_result_pcr , 'result_covid_npswab_pcr' ] = 2 
    zdf_nyc2.loc[ zdf_nyc2[ 'nyc_result_viral_test' ] == special_value_result_pcr , 'result_influenza_a_npswab_pcr' ] = 2 
    zdf_nyc2.loc[ zdf_nyc2[ 'nyc_result_viral_test' ] == special_value_result_pcr , 'result_influenza_b_npswab_pcr' ] = 2
    zdf_nyc2.loc[ zdf_nyc2[ 'nyc_result_viral_test' ] == special_value_result_pcr , 'result_rpp_npswab_pcr' ] = 1
    zdf_nyc2.loc[ zdf_nyc2[ 'nyc_result_viral_test' ] == special_value_result_pcr , 'rpp_npswab_pos_agents' ] = 'ADENOVIRUS; RHINOVIRUS/ENTEROVIRUS '
        
    list_vars_final_PCR = ['covid_npswab_pcr' , 'influenza_a_npswab_pcr', 'influenza_b_npswab_pcr', 'rpp_npswab_pcr']
    for var in list_vars_final_PCR:
        zdf_nyc2[var] = 3
    
    # list_vars_if_PCR_done = ['if_covid_npswab_pcr' , 'if_influenza_a_npswab_pcr', 'if_influenza_b_npswab_pcr', 'if_rpp_npswab_pcr']
    zdf_nyc2.loc[ zdf_nyc2[ 'if_covid_npswab_pcr' ] == 1  , 'covid_npswab_pcr' ] = zdf_nyc2[ 'result_covid_npswab_pcr' ] 
    zdf_nyc2.loc[ zdf_nyc2[ 'if_influenza_a_npswab_pcr' ] == 1  , 'influenza_a_npswab_pcr' ] = zdf_nyc2[ 'result_influenza_a_npswab_pcr' ] 
    zdf_nyc2.loc[ zdf_nyc2[ 'if_influenza_b_npswab_pcr' ] == 1  , 'influenza_b_npswab_pcr' ] = zdf_nyc2[ 'result_influenza_b_npswab_pcr' ] 
    zdf_nyc2.loc[ zdf_nyc2[ 'if_rpp_npswab_pcr' ] == 1  , 'rpp_npswab_pcr' ] = zdf_nyc2[ 'result_rpp_npswab_pcr' ] 
  
    # zdftest = zdf_nyc2[[ 'nyc_type_viral_test', 'nyc_result_viral_test' , 'rpp_npswab_pos_agents'] + list_vars_if_PCR_done + list_vars_results_PCR + list_vars_final_PCR ]
    zdftest = zdf_nyc2[[ 'nyc_type_viral_test', 'nyc_result_viral_test' , 'rpp_npswab_pos_agents'] +  list_vars_final_PCR ]
      
    # zdf_nyc2.influenza_b_npswab_pcr.value_counts()
    # mark1
    # convert_value_int_to_str( 'cig_smoke')    


    # zzzz = zdf_nyc2[ ['covid_npswab_pcr' , 'influenza_a_npswab_pcr', 'influenza_b_npswab_pcr', 'rpp_npswab_pcr', 
    #                   'if_covid_npswab_pcr' , 'if_influenza_a_npswab_pcr', 'if_influenza_b_npswab_pcr', 'if_rpp_npswab_pcr' ,
    #                   'result_covid_npswab_pcr' , 'result_influenza_a_npswab_pcr', 'result_influenza_b_npswab_pcr', 'result_rpp_npswab_pcr',
    #                   ]]
   


# other blood, lung swab, lung culture testing
    # culture_lung_yn = yesno
    
    list_vars_if_other_testing = [ 'culture_lung_yn', 'culture_blood_yn', 'lung_swab_yn' ]
    for var in list_vars_if_other_testing:
        process_categoric_var( var )
        zdf_nyc2.loc[ zdf_nyc2[ var ].str.find('YES') >= 0  , var ] = 1 
        zdf_nyc2.loc[ zdf_nyc2[ var ].str.find('NO') >= 0  , var ] = 0 
        zdf_nyc2.loc[ ( ~ zdf_nyc2[ var ].isin( [ 1 ]) )  , var ] = 0 # coded to be "no, not performed" unless = "yes"
    
    # 'lung_swab_yn'
    # 1, Yes | 2, No
    zdf_nyc2[ 'lung_swab_yn' ].replace( 0, 2, inplace=True )
    # zdf_nyc2[ 'lung_swab_yn' ].value_counts()

# autopsy findings

    # gross finding
    list_vars_finding_gross = [ 'finding_gross_respir', 'finding_gross_cardiovac', 'finding_gross_kidney',
                               'finding_gross_liver', 'finding_gross_brain', 'finding_gross_other']
    
    for var in list_vars_finding_gross:    
        zdf_nyc2.loc[ zdf_nyc2[ var ].str.upper().str.find('NO GROSS') >= 0 , var ] = np.nan
    
    zdf_nyc2[ 'autop_other_findings_desc' ] = zdf_nyc2[ list_vars_finding_gross ].apply(lambda x: "; ".join( x.dropna().astype(str) ),axis=1 )
    zdf_nyc2[ 'autop_other_findings' ] = 0
    zdf_nyc2.loc[ zdf_nyc2[ 'autop_other_findings_desc' ] != '' , 'autop_other_findings' ] = 1
   
    
   
    # microscopic finding 
    list_vars_finding_micro_original = [ 'finding_micro_respir', 'finding_micro_cardiovac', 'finding_micro_kidney',
                               'finding_micro_liver', 'finding_micro_brain', ]
    
    list_vars_finding_micro_yesno = [ 'autop_signif_resp_findings', 'autop_signif_card_findings', 'autop_signif_renal_find',
                               'autop_signif_liver_find', 'autop_signif_brain_find', ]
      
    
    # !!! need to remove this code in the future!!!
    # zdf_nyc2[ 'finding_micro_respir' ] = '1, Tracheitis | 2, Bronchitis | 3, Bronchiolitis | 4, Diffuse alveolar damage | 5, Capillaritis | 6, Vasculitis | 7, Interstitial pneumonitis | 8, Fibrin thrombi | 9, Pulmonary emboli | 10, Bronchopneumonia | 11, Intravascular leukocytosis | 12, Pulmonary hemorrhage | 13, Other' 
    # zdf_nyc2[ 'finding_micro_cardiovac' ] = '1, Myocarditis | 2, Other'
    # zdf_nyc2[ 'finding_micro_kidney' ] = '1, Fibrin thrombi | 2, Interstitial nephritis | 3, Other'
    # zdf_nyc2[ 'finding_micro_liver' ] = '1, Intravascular leukocytosis | 2, Other'
    # zdf_nyc2[ 'finding_micro_brain' ] = '1, Meningitis | 2, Encephalitis | 3, Thrombi | 4, Other'
    
    # get findings_yesno
    for i in range( len( list_vars_finding_micro_original ) ):

        var_ori = list_vars_finding_micro_original[ i ]
        process_categoric_var( var_ori )
        zdf_nyc2[ var_ori ] = zdf_nyc2[ var_ori ].str.upper()
        
        var = list_vars_finding_micro_yesno[ i ] 
        zdf_nyc2[ var ] = np.nan       # these variables do not exist in NYC's datafiles, so we need to assign values np.nan to them
        process_categoric_var( var ) 
        
        # 1, Significant findings | 2, No significant findings | 3, Tissues not collected
        # zdf_nyc2[ var ] = 2  # Tissues not collected  # may need to change the default value in the future ??? or may just leave var as np.nan
        zdf_nyc2.loc[ zdf_nyc2[ var_ori ].str.find('NOT DONE') >= 0 , var ] = 3
        zdf_nyc2.loc[ zdf_nyc2[ var_ori ].str.find('NOT DONE') >= 0 , var_ori ] = np.nan
        zdf_nyc2.loc[ zdf_nyc2[ var_ori ] == 'N/A' , var_ori ] = np.nan
        
        
        zdf_nyc2.loc[ ~ zdf_nyc2[ var_ori ].isna() , var ] = 1
        zdf_nyc2.loc[ zdf_nyc2[ var ] == 'N/A' , var ] = np.nan
        
        # 0 respir
        # 1 cardiovac
        # 2 kidney
        # 3 liver
        # 4 brain
                    
    zdf_nyc2.autop_other_findings_desc.value_counts()
        
           
  
# test
             
        
    def get_autopsy_findings_each_choice(string_choices, var_original, prefix_var_choice, list_pair_keywords ):
        finding_choices_respir0 = string_choices
        finding_choices_respir = finding_choices_respir0.upper()
        list_finding_choices_respir = finding_choices_respir.split(' | ')        
        # list_finding_choices_respir
    
        list_finding_choices_respir_number_from1 = [ x.split(', ')[0] for x in list_finding_choices_respir]
        list_finding_choices_respir_index_from0 = [ int(x) -1  for x in list_finding_choices_respir_number_from1]
        list_finding_choices_respir_words = [ x.split(', ')[1] for x in list_finding_choices_respir]
        list_finding_choices_respir_vars = [ prefix_var_choice + x for x in list_finding_choices_respir_number_from1]
        for i in range( len( list_finding_choices_respir_number_from1 ) ):
            print( "{}: '{}'__{}".format(list_finding_choices_respir_index_from0[i], list_finding_choices_respir_number_from1[i], list_finding_choices_respir_words[i] ) )   
         

        
        for index in range( len( list_finding_choices_respir_vars ) ):
            var = list_finding_choices_respir_vars[ index ]
            word = list_finding_choices_respir_words[ index ]
            zdf_nyc2.loc[ zdf_nyc2[ var_original ].str.find( word ) >= 0 , var ] = '1'
      
            for x in list_pair_keywords:
                # print( x[0], x[1])
                if  x[0] == index : 
                    keyword = x[1]
                    zdf_nyc2.loc[ zdf_nyc2[ var_original ].str.find( keyword ) >= 0 , var ] = '1'
                
   
    def get_autopsy_findings_respir():
        string_choices = '1, Tracheitis | 2, Bronchitis | 3, Bronchiolitis | 4, Diffuse alveolar damage | 5, Capillaritis | 6, Vasculitis | 7, Interstitial pneumonitis | 8, Fibrin thrombi | 9, Pulmonary emboli | 10, Bronchopneumonia | 11, Intravascular leukocytosis | 12, Pulmonary hemorrhage | 13, Other' 
        var_original = 'finding_micro_respir'
        prefix_var_choice = 'autop_resp_findings___'
        list_pair_keywords = [
                            [8, 'EMBOLI'],
                            [11, 'HEMORRHAGE'],
                            ]
        get_autopsy_findings_each_choice(string_choices, var_original, prefix_var_choice, list_pair_keywords )
        # 0: '1'__TRACHEITIS
        # 1: '2'__BRONCHITIS
        # 2: '3'__BRONCHIOLITIS
        # 3: '4'__DIFFUSE ALVEOLAR DAMAGE
        # 4: '5'__CAPILLARITIS
        # 5: '6'__VASCULITIS
        # 6: '7'__INTERSTITIAL PNEUMONITIS
        # 7: '8'__FIBRIN THROMBI
        # 8: '9'__PULMONARY EMBOLI  or EMBOLI ?
        # 9: '10'__BRONCHOPNEUMONIA
        # 10: '11'__INTRAVASCULAR LEUKOCYTOSIS
        # 11: '12'__PULMONARY HEMORRHAGE or HEMORRHAGE ?
        # 12: '13'__OTHER
        
    get_autopsy_findings_respir()       
         
    def get_autopsy_findings_cardiovac():
        string_choices = '1, Myocarditis | 2, Other'
        var_original = 'finding_micro_cardiovac'
        prefix_var_choice = 'autop_card_findings___'
        list_pair_keywords = [
                            # [999, 'XXXX'],
                            ]
        get_autopsy_findings_each_choice(string_choices, var_original, prefix_var_choice, list_pair_keywords )
        # 0: '1'__MYOCARDITIS
        # 1: '2'__OTHER
    
    get_autopsy_findings_cardiovac()  



    def get_autopsy_findings_kidney():
        string_choices = '1, Fibrin thrombi | 2, Interstitial nephritis | 3, Other'
        var_original = 'finding_micro_kidney'
        prefix_var_choice = 'autop_renal_findings___'
        list_pair_keywords = [
                            # [999, 'XXXX'],
                            ]
        get_autopsy_findings_each_choice(string_choices, var_original, prefix_var_choice, list_pair_keywords )
        # 0: '1'__FIBRIN THROMBI
        # 1: '2'__INTERSTITIAL NEPHRITIS
        # 2: '3'__OTHER
        
    get_autopsy_findings_kidney()
    
    
    def get_autopsy_findings_liver():
        string_choices = '1, Intravascular leukocytosis | 2, Other'
        var_original = 'finding_micro_liver'
        prefix_var_choice = 'autop_liver_findings___'
        list_pair_keywords = [
                            # [999, 'XXXX'],
                            ]
        get_autopsy_findings_each_choice(string_choices, var_original, prefix_var_choice, list_pair_keywords )
        # 0: '1'__INTRAVASCULAR LEUKOCYTOSIS
        # 1: '2'__OTHER
        
    get_autopsy_findings_liver()
    
    
    def get_autopsy_findings_brain():
        string_choices = '1, Meningitis | 2, Encephalitis | 3, Thrombi | 4, Other'
        var_original = 'finding_micro_brain'
        prefix_var_choice = 'autop_brain_findings___'
        list_pair_keywords = [
                            # [999, 'XXXX'],
                            ]
        get_autopsy_findings_each_choice(string_choices, var_original, prefix_var_choice, list_pair_keywords )
        # 0: '1'__MENINGITIS
        # 1: '2'__ENCEPHALITIS
        # 2: '3'__THROMBI
        # 3: '4'__OTHER
        
    get_autopsy_findings_brain()
    

    list_vars_finding_micro_other = [ 'resp_findings_other', 'card_findings_other', 'renal_findings_other',
                                     'liver_findings_other', 'brain_findings_other', ]  
    def to_check_in_the_future():
        pass
        # check the findings to see if any additional keywords can be used to capture the choices of the finding
        # check the other findings to see how to capture the other finding

    # convert variables from int to str        
    
    list_vars_value_to_convert_to_str =  ['vaccine_doses', 'age_units' ] + list_vars_conditions_all + list_vars_symptom_all
    for var in list_vars_value_to_convert_to_str:
        convert_value_int_to_str( var ) 
    
    
# end test

    zdftest = zdf_nyc2[ zdf_nyc2.filter(regex='^autop_',axis=1).columns ]

    # zdf_nyc2[ 'all_finding_micro_original' ] = zdf_nyc2[ list_vars_finding_micro_original ].apply(lambda x: "; ".join( x.dropna().astype(str) ),axis=1 )
    
    
    # zdftest = zdf_nyc2[ list_finding_choices_respir_vars + list_vars_finding_micro_original + list_vars_finding_micro_yesno +  ['finding_micro_respir0', 'all_finding_micro_original', 'autop_other_findings_desc', 'autop_other_findings', 'autopsy_performed0', 'autopsy_performed', 'autopsy_date','cod_line_a', 'cod_line_b', 'cod_line_c',  'cod_part_2', 'cod_manner', ]]
 

#%%

# =============================================================================
#     vars to output
# =============================================================================

    list_vars_zdf_nyc2 = zdf_nyc2.columns.to_list()    
    # list_vars_zdf_nyc2 = zdf_nyc_crosswalk[ 
    #     (zdf_nyc_crosswalk[ 'coding_status' ].str[0:4] == 'Done' ) & 
    #     ( ~ zdf_nyc_crosswalk[ 'redcap_variable_name' ].isna() ) & 
    #     ( zdf_nyc_crosswalk[ 'coding_type' ] != 'no need to add var' ) 
    #     ] [ 'redcap_variable_name' ].to_list()
      
    list_vars_template = zdf_template.columns.to_list()
    
    set_list_vars_template = set(list_vars_template)
    set_common_vars = set_list_vars_template.intersection(list_vars_zdf_nyc2)
    list_common_vars = list(set_common_vars)
    list_common_vars.remove('record_id')

    # check the data 
    zdftest_output = zdf_nyc2[ ['record_id'] + list_common_vars ] 
    
    # zdftest_output.iloc[:20,:].to_csv( folder9_states + r'\test.csv' , index=False) 
    zdftest_output.to_csv( folder9_states + r'\test.csv' , index=False) 
    zdftest_output.iloc[:100,:].to_csv( folder9_states + r'\test1.csv' , index=False) 
    zdftest_output.iloc[100:200,:].to_csv( folder9_states + r'\test2.csv' , index=False) 
    zdftest_output.iloc[200:300,:].to_csv( folder9_states + r'\test3.csv' , index=False)
    zdftest_output.iloc[300:,:].to_csv( folder9_states + r'\test4.csv' , index=False)
    
    # zdftest_output.info()
#%%    
    

    zdftest = zdf_nyc2[[ 'autop_other_findings_desc', 'autopsy_performed0', 'autopsy_performed', 'autopsy_date','cod_line_a', 'cod_line_b', 'cod_line_c',  'cod_part_2', 'cod_manner', ]]
    

    zdftest = zdf_nyc2[[ 'specify_organ_transplant','organ_stem_transplant', 'sickle_cell', 'specify_autoimmune','autoimmune', 'immune_supp','specify_immunosup', ] ]

    zdftest = zdf_nyc2[ [
'dose_1_date0', 'dose_1_date',
'dose_2_date0', 'dose_2_date',
'vaccine_doses0', 'vaccine_doses',   
'vaccine_manufact0', 'vaccine_manufact', 'covid_vaccine0', 'covid_vaccine1', 'covid_vaccine', ] ]
    # zdftest = zdf_nyc2[list_vars_conditions_all ] 
    # zdftest = zdf_nyc2[list_vars_conditions_0_1_2 ]  
    zdftest = zdf_nyc2[ 
        [
            # 'Tobacco use0', 'Tobacco use1', 'Tobacco use', 
         #  'Tobacco:  Current or Former',
         # 'cig_smoke', 'cig_smoke_former',
         #    'specify_chronic_lung_dis', 
            ]  
        + 
        list_vars_conditions_all +[
'dose_1_date0', 'dose_1_date',
'dose_2_date0', 'dose_2_date',
'vaccine_doses0', 'vaccine_doses',   
'vaccine_manufact0', 'vaccine_manufact', 'covid_vaccine0', 'covid_vaccine1', 'covid_vaccine', 
'symptom_onset_date_m_d_y00','symptom_onset_date_m_d_y0', 'symptom_onset_date_m_d_y', 'fever0', 'fever',
'fever_type0', 'fever_type', 'subjective_fever', 'documented_fever', 
'date_recent_med_visit00','date_recent_med_visit0', 'date_recent_med_visit' , 
'loc_death0', 'loc_death1', 'loc_death', 'death_loc_other', 'Race0', 'Race', 
 'ethnicity','race_sum','race___asian','race___black', 'race___white', 'race___unk',  
 'BMI0', 'BMI', 'Obesity', 'obese_bmi_unk', 'severe_obese', 'obese_30_39', 
 'Age0', 'Age', 'age_units', 'age_years', 'age_months' ]
                       + list_vars_symptom_012 ]
    # zdftest.to_csv( folder9_states + r'\test.csv' , index=False)    
    zdf_nyc2['loc_death'].value_counts()
    # zdf_nyc2['death_date_m_d_y'][:5]

    
#%%

# nyc()
    
    
def check_the_data(): 
    
    
    
    # Important notes when updating!!!
    # Race 
        # - currently not including race___nhpi, race___aian, race___other
        # - need to check the pivot table of race in the future, and see if need to add other race vars.
    
    # useful Python tricks/tips
    zdf_nyc2.loc[  zdf_nyc2['BMI'].isna() == True , 'BMI_is_null_nan' ] = 1 
  
    # output the df ztest with 2 rows only to test the upload in RedCap
    ztest = zdf_nyc2[ list_values_var_names_new].iloc[:2, :]   

    # end - check the data
    ztest.columns 
    zdf_nyc2.info()
    zdf_nyc2.describe()
    zdf_nyc2['Race'].value_counts()
    zdf_nyc2['age_units'].value_counts()
    zdf_nyc2['age_years'].value_counts()
    zdf_nyc2['age_months'].value_counts()
    zdf_nyc2['site_id'].dtypes
    
    zdftest = zdf_nyc2[ ['BMI0', 'BMI', 'Age0', 'Age', 'age_units', 'age_years', 'age_months' ] ]
    z = ztest[list_vars_date]
    zz = ztest.dtypes
    zdf_nyc2['death_date_m_d_y'].astype()
    set(zz)
    
    # end - test or draft
    '''
    
    # get each finding of respir
    finding_choices_respir0 = '1, Tracheitis | 2, Bronchitis | 3, Bronchiolitis | 4, Diffuse alveolar damage | 5, Capillaritis | 6, Vasculitis | 7, Interstitial pneumonitis | 8, Fibrin thrombi | 9, Pulmonary emboli | 10, Bronchopneumonia | 11, Intravascular leukocytosis | 12, Pulmonary hemorrhage | 13, Other' 
    finding_choices_respir = finding_choices_respir0.upper()
    list_finding_choices_respir = finding_choices_respir.split(' | ')        
    list_finding_choices_respir

    list_finding_choices_respir_number_from1 = [ x.split(', ')[0] for x in list_finding_choices_respir]
    list_finding_choices_respir_index_from0 = [ int(x) -1  for x in list_finding_choices_respir_number_from1]
    list_finding_choices_respir_words = [ x.split(', ')[1] for x in list_finding_choices_respir]
    list_finding_choices_respir_vars = [ 'autop_resp_findings___' + x for x in list_finding_choices_respir_number_from1]
    for i in range( len( list_finding_choices_respir_number_from1 ) ):
        print( "{}: '{}'__{}".format(list_finding_choices_respir_index_from0[i], list_finding_choices_respir_number_from1[i], list_finding_choices_respir_words[i] ) )   
    
    for index in range( len( list_finding_choices_respir_vars ) ):
        var = list_finding_choices_respir_vars[ index ]
        word = list_finding_choices_respir_words[ index ]
        zdf_nyc2.loc[ zdf_nyc2[ 'finding_micro_respir' ].str.find( word ) >= 0 , var ] = 1
        # num = index + 1
        if index == 8: 
            zdf_nyc2.loc[ zdf_nyc2[ 'finding_micro_respir' ].str.find( 'EMBOLI' ) >= 0 , var ] = 1
        if index == 11: 
            zdf_nyc2.loc[ zdf_nyc2[ 'finding_micro_respir' ].str.find( 'HEMORRHAGE' ) >= 0 , var ] = 1
  
    
  
    # list_vars_finding_micro = [ 'autop_resp_findings', 'autop_card_findings', 'autop_renal_findings',
    #                            'autop_liver_findings', 'autop_brain_findings', ]
    
    # for i in range( len( list_vars_finding_micro_original ) ):
    #     print ( i, list_vars_finding_micro_original[ i ][ 14: ] )
    
    
    # for var in list_vars_symptom:    
    #     process_categoric_var(var)
    #     var1 = var + '1'
    #     zdf_nyc2[var1] = zdf_nyc2[var]
           
    #     get_pattern_new_value_sx(var, r'^Y.*', 1)
    #     get_pattern_new_value_sx(var, r'^NO.*', 0) #be careful: "N" will retrieve both "NO" and "N/A"
    #     get_pattern_new_value_sx(var, r'^U.*', 2)
    #     get_pattern_new_value_sx(var, r'N/A', np.nan)
    
    
    value = None
    a_string = "abc" if value is None else value
    print(a_string)
    # format the date
    # list_vars_type_datetime64 = [ 'DateReceived', 'month_received',  'DateOfDeath',  'SignOutDate',  ]
    list_vars_type_datetime64 = [ x for x in ztest.columns if x.find('date') > 0 ]
    zz = format_date(ztest, list_vars_type_datetime64)
    zlist = [ ztest[x].dtypes for x in list_vars_type_datetime64 ]
    
        
    # def get_var_sx(var):
    #     var = 'cough'
    #     process_categoric_var(var)
    #     var1 = var + '1'
    #     zdf_nyc2[var1] = zdf_nyc2[var]
        
    #     def get_pattern_new_value_sx(pattern, value):
    #         zdf_nyc2.loc[ zdf_nyc2[var1].apply(lambda x: get_pattern(x, pattern)) == 1, var] = value
            
    #     get_pattern_new_value_sx(r'^Y.*', 1)
    #     get_pattern_new_value_sx(r'^N.*', 0)
    #     get_pattern_new_value_sx(r'^U.*', 2)

    # for x in list_vars_symptom:
    #     get_var_sx(x)  
    
    zdf_nyc2.loc[ zdf_nyc2["loc_death"].apply(lambda x: get_pattern(x, r'^RESIDENCE.*')) == 1, 'loc_death'] = 1
    zdf_nyc2.loc[ zdf_nyc2["loc_death"].apply(lambda x: get_pattern(x, r'.+RESIDENCE.*')) == 1, 'loc_death'] = 8 
    zdf_nyc2.loc[ zdf_nyc2["loc_death"].apply(lambda x: get_pattern(x, r'^HOSPITAL.*')) == 1, 'loc_death'] = 4 
    zdf_nyc2.loc[ zdf_nyc2["loc_death"].apply(lambda x: get_pattern(x, r'^MEDICAL FACILITY.*')) == 1, 'loc_death'] = 4 
    
    zdf_nyc2['Race0'] = zdf_nyc2['Race']
    zdf_nyc2['Race'] = zdf_nyc2['Race'].str.upper()
    zdf_nyc2.loc[ ( zdf_nyc2['Race'].isna() == True ), 'Race' ] = 'N/A'
    
    def race___asian(x):     
        return 1 if re.search(r"^A.*/?P?.*", x ) else 0
        
    zdf_nyc2["race___asian"]= zdf_nyc2["Race"].apply(lambda x: race_pattern(x, r"^A.*/?P?.*")) 
    zdf_nyc2["race___black"]= zdf_nyc2["Race"].apply(lambda x: race_pattern(x, r"^B*")) 

    def race___asian(x): 
        pattern = r"^A.*/?P?.*"
        if re.search(pattern, x ):  return 1  
        else: return 0
     
        
     
    '''
  
    