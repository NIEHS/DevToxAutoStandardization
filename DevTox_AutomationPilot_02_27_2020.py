import os
import pandas as pd
import numpy as np
import re
import timeit
import csv
from collections import OrderedDict
from datetime import date

start = timeit.default_timer()


abspath = os.path.abspath(__file__)
path = os.path.dirname(abspath)
os.chdir(path)


#####_______________INPUTS_____________________###

# extractionPath = r"C:\Users\51243\ICF\National Institute of Environmental Health Sciences - NTP-4-06 Dev Tox Vocab\02_Mapping vocabularies\Automation Code\Python files\DevTox\VERSION_2\NTP Reports DevTox Data Extraction_17August2017_SMALL.xlsx"
# extractionSheet = "devtox03August2017"
# xlsxFilename = "TEST_Extraction_" + str(date.today())
# extractionColumns = None

# extractionPath = r"C:\Users\51243\ICF\National Institute of Environmental Health Sciences - NTP-4-06 Dev Tox Vocab\01_Original Materials\02_Extracted Data\NTP Reports DevTox Data Extraction_17August2017.xlsx"
# extractionSheet = "devtox03August2017"
# xlsxFilename = "NTP_Extraction_" + str(date.today())
# extractionColumns = None

extractionPath = r"C:\Users\51243\ICF\National Institute of Environmental Health Sciences - NTP-4-06 Dev Tox Vocab\01_Original Materials\02_Extracted Data\ECHA Extractions-24July2017.xlsx"
extractionSheet = "Extractions"
xlsxFilename = "ECHA_Extraction_" + str(date.today())
extractionColumns = None

crosswalksPath = r"C:\Users\51243\ICF\National Institute of Environmental Health Sciences - NTP-4-06 Dev Tox Vocab\02_Mapping vocabularies\Vocab crosswalks_Master.xlsx"
crosswalkSheet = "UMLS x DevTox x OECD_CodeReady"
crosswalkColumns = "M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z,AA,AB"

synonymListsPath = r"C:\Users\51243\ICF\National Institute of Environmental Health Sciences - NTP-4-06 Dev Tox Vocab\02_Mapping vocabularies\Automation_combined terms and synonyms lists.xlsx"
synonymListsSheet= ['Localizations (A)','Observations (B)','Combo Words (C)','Unique Words (D)', 'Hard Codes (E)']
synonymListsLocalization = "A,B"
synonymListsObservation = "A,B"
synonymListsCombo = "A,B,C"
synonymListsUnique = "A,B"
synonymListsHardCodes = "A,B,C,D,E,F"


####_______________READING INPUTS________________________####
print("Reading XLSX Files:")
df_Extraction = pd.read_excel(extractionPath , sheet_name = extractionSheet, usecols = extractionColumns).astype(str).apply(lambda x: x.str.lower())

df_Crosswalk = pd.read_excel(crosswalksPath , sheet_name = crosswalkSheet, skiprows=1, usecols = crosswalkColumns).apply(lambda col: col.str.lower())

df_Crosswalk_Synonyms = df_Crosswalk

df_LocalizationSyns_A1_A2 = pd.read_excel(synonymListsPath, sheet_name=synonymListsSheet[0], usecols=synonymListsLocalization).astype(str).apply(lambda x: x.str.lower())
#df_LocalizationSyns_A1_A2.replace('nan', '', inplace=True)

df_ObservationsSyns_B1_B2 = pd.read_excel(synonymListsPath, sheet_name=synonymListsSheet[1], usecols=synonymListsObservation).astype(str).apply(lambda x: x.str.lower())
#df_ObservationsSyns_B1_B2.replace('nan', "", inplace=True)

df_ComboWordsSyns_C1_C2_C3 = pd.read_excel(synonymListsPath, sheet_name=synonymListsSheet[2], usecols=synonymListsCombo).astype(str).apply(lambda x: x.str.lower())
#df_ComboWordsSyns_C1_C2_C3.replace('nan', "", inplace=True)

df_UniqueWordsSyns_D1_D2 = pd.read_excel(synonymListsPath,  sheet_name=synonymListsSheet[3], usecols=synonymListsUnique).astype(str).apply(lambda x: x.str.lower())
#df_UniqueWordsSyns_D1_D2.replace('nan', "", inplace=True)

df_HardCodes = pd.read_excel(synonymListsPath,  sheet_name=synonymListsSheet[4], usecols=synonymListsHardCodes).astype(str).apply(lambda x: x.str.lower())


print("#__________________________FILES READ___________________________#")

LocalizationLookUpList = df_LocalizationSyns_A1_A2["A1"].values.tolist() + df_LocalizationSyns_A1_A2["A2"].values.tolist()
LocalizationLookUpList = list(set(LocalizationLookUpList))
LocalizationLookUpList_Whole = [x for x in LocalizationLookUpList if "*" not in x]
LocalizationLookUpList_Roots = [x for x in LocalizationLookUpList if "*" in x]   ###THIS IS WHERE ROOTS WORDS ARE PULLED INTO A LIST###
LocalizationLookUpList_Roots = [s.strip('*') for s in LocalizationLookUpList_Roots]

ObservationLookUpList  = df_ObservationsSyns_B1_B2["B1"].values.tolist() + df_ObservationsSyns_B1_B2["B2"].values.tolist()
ObservationLookUpList = list(dict.fromkeys(ObservationLookUpList))
ObservationLookUpList_Whole = [x for x in ObservationLookUpList if "*" not in x]
ObservationLookUpList_Roots = [x for x in ObservationLookUpList if "*" in x]
ObservationLookUpList_Roots = [s.strip('*') for s in ObservationLookUpList_Roots]

ComboWordsLookUpList =  df_ComboWordsSyns_C1_C2_C3["C1"].values.tolist()
ComboWordsLookUpList = list(set(ComboWordsLookUpList))
ComboWordsLookUpList_Whole = [x for x in ComboWordsLookUpList if "*" not in x]
ComboWordsLookUpList_Roots = [x for x in ComboWordsLookUpList if "*" in x]
ComboWordsLookUpList_Roots = [s.strip('*') for s in ComboWordsLookUpList_Roots]

UniqueWordsLookUpList =  df_UniqueWordsSyns_D1_D2["D1"].values.tolist()
UniqueWordsLookUpList = list(set(UniqueWordsLookUpList))
UniqueWordsLookUpList_Whole = [x for x in UniqueWordsLookUpList if "*" not in x]
UniqueWordsLookUpList_Roots = [x for x in UniqueWordsLookUpList if "*" in x]
UniqueWordsLookUpList_Roots = [s.strip('*') for s in UniqueWordsLookUpList_Roots]

print("#__________LIST CREATED LOWERED AND DUPLICATES REMOVED___________#")

def findRootWords(haystack,needles):
	ret_val = '' 
	for needle in needles:
		if haystack.find(needle) >= 0:
			ret_val += needle + '*, '
	return ret_val[:-2] + '' if len(ret_val) > 1 else ret_val + ''

def findWholeWords(haystack, needles):
	ret_val = ''
	for needle in needles:
		if re.search(r'\b' + needle + r'\b', haystack) != None:
			ret_val += needle + ', '
	return ret_val[:-2] + '' if len(ret_val) > 1 else ret_val + ''



print("#__FINDING LOCALIZATIONS/OBSERVATIONS/COMBO WORDS/UNIQUE WORDS IN EXTRACTION__#")


df_Extraction["Localizations_Whole_Found"] = df_Extraction['Effects'].apply(findWholeWords, needles=LocalizationLookUpList_Whole)
df_Extraction["Localizations_Roots_Found"] = df_Extraction['Effects'].apply(findRootWords, needles=LocalizationLookUpList_Roots)   ### POTENTIAL BUG BEING APPLIED HERE ####
print("Localizations Found")

df_Extraction["Observations_Whole_Found"]  = df_Extraction['Effects'].apply(findWholeWords, needles=ObservationLookUpList_Whole)
df_Extraction["Observations_Roots_Found"]  = df_Extraction['Effects'].apply(findRootWords, needles=ObservationLookUpList_Roots)
print("Observations Found")

df_Extraction["ComboWords_Whole_Found"]	= df_Extraction['Effects'].apply(findWholeWords, needles=ComboWordsLookUpList_Whole)
df_Extraction["ComboWords_Roots_Found"]	= df_Extraction['Effects'].apply(findRootWords, needles=ComboWordsLookUpList_Roots)
print("Combo Words Found")

df_Extraction["UniqueWords_Whole_Found"]   = df_Extraction['Effects'].apply(findWholeWords, needles=UniqueWordsLookUpList_Whole)
df_Extraction["UniqueWords_Roots_Found"]   = df_Extraction['Effects'].apply(findRootWords, needles=UniqueWordsLookUpList_Roots)
print("Unique Words Found")
#df_Extraction.to_csv("test.csv")


##Combining WHOLE and ROOT words that were found in the effects column##
df_Extraction.replace('', np.nan, inplace=True)
df_Extraction["Localizations_Found"] = df_Extraction[["Localizations_Whole_Found" , "Localizations_Roots_Found"]].apply(lambda x:', '.join(x[x.notnull()]), axis = 1).str.lower()
df_Extraction = df_Extraction.drop(["Localizations_Whole_Found" , "Localizations_Roots_Found"],axis=1)

df_Extraction["Observations_Found"] = df_Extraction[["Observations_Whole_Found" , "Observations_Roots_Found"]].apply(lambda x:', '.join(x[x.notnull()]), axis = 1).str.lower()
df_Extraction = df_Extraction.drop(["Observations_Whole_Found" , "Observations_Roots_Found"],axis=1)

df_Extraction["ComboWords_Found"] = df_Extraction[["ComboWords_Whole_Found" , "ComboWords_Roots_Found"]].apply(lambda x:', '.join(x[x.notnull()]), axis = 1).str.lower()
df_Extraction = df_Extraction.drop(["ComboWords_Whole_Found" , "ComboWords_Roots_Found"],axis=1)

df_Extraction["UniqueWords_Found"] = df_Extraction[["UniqueWords_Whole_Found" , "UniqueWords_Roots_Found"]].apply(lambda x:', '.join(x[x.notnull()]), axis = 1).str.lower()
df_Extraction = df_Extraction.drop(["UniqueWords_Whole_Found" , "UniqueWords_Roots_Found"],axis=1)


print("#____________FINDING SYNONYMS FOR ALL SEARCH COLUMNS_____________#")
def findSynonyms(haystack, needles_dataframe):
	ret_val = ''
	for i in range(len(needles_dataframe.index)):
		if needles_dataframe.iloc[i,1] != "nan":
			needle= needles_dataframe.iloc[i,0]
			needleSyn= needles_dataframe.iloc[i,1]
			if re.search( r'\b' + needle + r'\b' , haystack) != None:  ###SEEMS TO BE SLOW, .find() potentially quicker?
				ret_val += needleSyn + ', '
	return ret_val[:-2] + '' if len(ret_val) > 1 else ret_val + ''

def addComboWordSynonyms(haystack, needles_dataframe):
	ret_val_loc = ''
	ret_val_obs = ''
	for i in range(len(needles_dataframe.index)):
		needle= needles_dataframe.iloc[i,0]
		needleLocalization = needles_dataframe.iloc[i,1]
		needleObservation =  needles_dataframe.iloc[i,2]
		if re.search( r'\b' + needle + r'\b' , haystack) != None:  ###SEEMS TO BE SLOW, .find() potentially quicker?
			ret_val_loc += needleLocalization + ', '
			ret_val_obs += needleObservation + ', '
	return ret_val_loc[:-2] + '' if len(ret_val_loc) > 1 else ret_val_loc + '' , ret_val_obs[:-2] + '' if len(ret_val_obs) > 1 else ret_val_obs + ''

df_Extraction["Localizations_Syns"] = df_Extraction["Localizations_Found"].apply(findSynonyms,  needles_dataframe=df_LocalizationSyns_A1_A2)
print("Localization Synonyms Found")

df_Extraction["Observations_Syns"] = df_Extraction["Observations_Found"].apply(findSynonyms,  needles_dataframe=df_ObservationsSyns_B1_B2)
print("Observations Synonyms Found")

df_Extraction["UniqueWords_Syns"] = df_Extraction["UniqueWords_Found"].apply(findSynonyms,  needles_dataframe=df_UniqueWordsSyns_D1_D2)
print("Unique Word Synonyms Found")

df_Extraction["ComboWords_Syn_Localizations"], df_Extraction["ComboWords_Syn_Observations"] = zip(*df_Extraction["ComboWords_Found"].apply(addComboWordSynonyms, needles_dataframe = df_ComboWordsSyns_C1_C2_C3))
print("Combo Word Synonyms Found")


df_Extraction.replace('', np.nan, inplace=True)
df_Extraction["Localizations"] = df_Extraction[["Localizations_Found" , "Localizations_Syns", "ComboWords_Syn_Localizations"]].apply(lambda x:', '.join(x[x.notnull()]), axis = 1).str.lower()
df_Extraction = df_Extraction.drop(["Localizations_Found" , "Localizations_Syns", "ComboWords_Syn_Localizations"],axis=1)
df_Extraction["Observations"] = df_Extraction[["Observations_Found" , "Observations_Syns","ComboWords_Syn_Observations"]].apply(lambda x:', '.join(x[x.notnull()]), axis = 1).str.lower()
df_Extraction = df_Extraction.drop(["Observations_Found" , "Observations_Syns", "ComboWords_Syn_Observations"],axis=1)
df_Extraction["UniqueWords"] = df_Extraction[["UniqueWords_Found" , "UniqueWords_Syns"]].apply(lambda x:', '.join(x[x.notnull()]), axis = 1).str.lower()
df_Extraction = df_Extraction.drop(["UniqueWords_Found" , "UniqueWords_Syns"],axis=1)



def findComboWordsfromResults(haystack_dataframe, needles_dataframe):
	haystack_dataframe["ComboWords_Added"] = ""
	for index, row in haystack_dataframe.iterrows():
		Localization_Row_List = row['Localizations'].split(",")
		Localization_Row_List = [t for t in Localization_Row_List if t !=""]
		Localization_Row_List = [s.strip('*') for s in Localization_Row_List]
		Observation_Row_List  = row['Observations'].split(',')
		Observation_Row_List = [t for t in Observation_Row_List if t !=""]
		Observation_Row_List = [s.strip('*') for s in Observation_Row_List]
		for index_2, row2 in needles_dataframe.iterrows():
			if any(s in row2["C2"] for s in Localization_Row_List) and any(s in row2["C3"] for s in Observation_Row_List):
				haystack_dataframe['ComboWords_Added'].at[index] += needles_dataframe['C1'].at[index_2] + ", "


findComboWordsfromResults(df_Extraction,df_ComboWordsSyns_C1_C2_C3)
print("Combo Word Added from Localizations and Observations")


df_Extraction.replace('', np.nan, inplace=True)
df_Extraction["Combos"] = df_Extraction[["ComboWords_Found" , "ComboWords_Added"]].apply(lambda x:', '.join(x[x.notnull()]), axis = 1).str.lower()
df_Extraction["Combos"] = df_Extraction["Combos"].apply(lambda x: x.rstrip(", "))
df_Extraction = df_Extraction.drop(["ComboWords_Found" , "ComboWords_Added"],axis=1)


print("#____________SEARCHING FOR UMLS MATCHES_____________#")

df_Extraction.replace(np.nan,'', inplace=True)


umlsList = df_Crosswalk_Synonyms['umls_xref_original'].tolist()
umlsList = [str(x).lower() for x in umlsList]

DevToxList = df_Crosswalk_Synonyms['DevToxList'].replace(np.nan,'', inplace=True)
DevToxList = df_Crosswalk_Synonyms['DevToxList'].tolist()
DevToxList[:] = (value for value in DevToxList if value != '   ')
DevToxList = [str(x).lower() for x in DevToxList]

oecdList = df_Crosswalk_Synonyms['OECDList'].replace(np.nan,'', inplace=True)
oecdList = df_Crosswalk_Synonyms['OECDList'].tolist()
oecdList = [str(x).lower() for x in oecdList]


df_Extraction['UMLS'] = ""
df_Extraction["UMLS - Devtox"] = ""
df_Extraction["UMLS - OECD"] = ""
df_Extraction["UMLS - Count"] = 1

df_Extraction['DevTox'] = ""
df_Extraction["DevTox - UMLS"] = ""
df_Extraction["DevTox - OECD"] = ""
df_Extraction["DevTox - Count"] = 1

df_Extraction['OECD'] = ""
df_Extraction["OECD - UMLS"] = ""
df_Extraction["OECD - Devtox"] = ""
df_Extraction["OECD - Count"] = 1

for index, row in df_Extraction.iterrows():
	Localization_Row_List = row['Localizations'].split(", ")
	Localization_Row_List = [t for t in Localization_Row_List if t !=""]
	Localization_Row_List = [s.strip('*') for s in Localization_Row_List ]
	Observation_Row_List  = row['Observations'].split(', ')
	Observation_Row_List = [t for t in Observation_Row_List if t !=""]
	Observation_Row_List = [s.strip('*') for s in Observation_Row_List ]
	Combo_Row_List =  row['Combos'].split(', ')
	Combo_Row_List = [t for t in Combo_Row_List if t !=""]
	Combo_Row_List = [s.strip('*') for s in Combo_Row_List ]
	Unique_Row_List =  row['UniqueWords'].split(', ')
	Unique_Row_List = [t for t in Unique_Row_List if t !=""]
	Unique_Row_List = [s.strip('*') for s in Unique_Row_List ]
	LitterList = ['litter','litters','fetus','fetuses']
	DefectList = ['defect','defects','malformation','malformations','abnormality','abnormalities','variations' , 'adversely affected', 'anomaly','anomalies'] #Check Spelling 
	SexList = ['male','female']


	for UMLS_index, x in enumerate(umlsList):
		if any(s in x for s in Localization_Row_List) and any(s in x for s in Observation_Row_List):

			df_Extraction['UMLS'].at[index] =   df_Extraction['UMLS'].at[index] + " (" +str(df_Extraction['UMLS - Count'].at[index]) + ") " + str(x) + " ; "
			df_Extraction['UMLS - Devtox'].at[index] = str(df_Extraction['UMLS - Devtox'].at[index]) + " (" +str(df_Extraction['UMLS - Count'].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[UMLS_index,3]) + " ; "
			df_Extraction['UMLS - OECD'].at[index] = str(df_Extraction['UMLS - OECD'].at[index]) + " (" +str(df_Extraction['UMLS - Count'].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[UMLS_index,9]) + " ; "
			df_Extraction['UMLS - Count'].at[index] += 1
		if any(s in x for s in Combo_Row_List):
			df_Extraction['UMLS'].at[index] = df_Extraction['UMLS'].at[index] + " (" +str(df_Extraction['UMLS - Count'].at[index]) + ") " + str(x) + ";"
			df_Extraction['UMLS - Devtox'].at[index] = str(df_Extraction['UMLS - Devtox'].at[index]) + " (" +str(df_Extraction['UMLS - Count'].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[UMLS_index , 3 ]) + " ; "
			df_Extraction['UMLS - OECD'].at[index] = str(df_Extraction['UMLS - OECD'].at[index]) + " (" +str(df_Extraction['UMLS - Count'].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[UMLS_index,9]) + " ; "
			df_Extraction['UMLS - Count'].at[index] += 1
		if any(s in x for s in Unique_Row_List):
			df_Extraction['UMLS'].at[index] = df_Extraction['UMLS'].at[index] + " (" +str(df_Extraction['UMLS - Count'].at[index]) + ") " + str(x) + ";"
			df_Extraction['UMLS - Devtox'].at[index] = str(df_Extraction['UMLS - Devtox'].at[index]) + " (" +str(df_Extraction['UMLS - Count'].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[UMLS_index , 3 ]) + " ; "
			df_Extraction['UMLS - OECD'].at[index] = str(df_Extraction['UMLS - OECD'].at[index]) + " (" +str(df_Extraction['UMLS - Count'].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[UMLS_index,9]) + " ; "
			df_Extraction['UMLS - Count'].at[index] += 1
	for DevTox_index, x in enumerate(DevToxList):
		if any(s in x for s in Localization_Row_List) and any(s in x for s in Observation_Row_List):

			df_Extraction['DevTox'].at[index] =   df_Extraction['DevTox'].at[index] + " (" +str(df_Extraction["DevTox - Count"].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[DevTox_index, 3 ]) + " ; "
			df_Extraction["DevTox - UMLS"].at[index] = str(df_Extraction["DevTox - UMLS"].at[index]) + " (" +str(df_Extraction["DevTox - Count"].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[DevTox_index,1]) + " ; "
			df_Extraction["DevTox - OECD"].at[index] = str(df_Extraction["DevTox - OECD"].at[index]) + " (" +str(df_Extraction["DevTox - Count"].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[DevTox_index,9]) + " ; "
			df_Extraction["DevTox - Count"].at[index] += 1
		if any(s in x for s in Combo_Row_List):
			df_Extraction['DevTox'].at[index] = df_Extraction['DevTox'].at[index] + " (" +str(df_Extraction["DevTox - Count"].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[DevTox_index , 3 ]) + ";"
			df_Extraction["DevTox - UMLS"].at[index] = str(df_Extraction["DevTox - UMLS"].at[index]) + " (" +str(df_Extraction["DevTox - Count"].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[DevTox_index , 1 ]) + " ; "
			df_Extraction["DevTox - OECD"].at[index] = str(df_Extraction["DevTox - OECD"].at[index]) + " (" +str(df_Extraction["DevTox - Count"].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[DevTox_index,9]) + " ; "
			df_Extraction["DevTox - Count"].at[index] += 1
		if any(s in x for s in Unique_Row_List):
			df_Extraction['DevTox'].at[index] = df_Extraction['DevTox'].at[index] + " (" +str(df_Extraction["DevTox - Count"].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[DevTox_index , 3 ]) + ";"
			df_Extraction["DevTox - UMLS"].at[index] = str(df_Extraction["DevTox - UMLS"].at[index]) + " (" +str(df_Extraction["DevTox - Count"].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[DevTox_index , 1 ]) + " ; "
			df_Extraction["DevTox - OECD"].at[index] = str(df_Extraction["DevTox - OECD"].at[index]) + " (" +str(df_Extraction["DevTox - Count"].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[DevTox_index,9]) + " ; "
			df_Extraction["DevTox - Count"].at[index] += 1			

	for OECD_index, x in enumerate(oecdList):
		if any(s in x for s in Localization_Row_List) and any(s in x for s in Observation_Row_List):

			df_Extraction['OECD'].at[index] =   df_Extraction['OECD'].at[index] + " (" +str(df_Extraction['OECD - Count'].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[OECD_index,9]) + " ; "
			df_Extraction['OECD - Devtox'].at[index] = str(df_Extraction['OECD - Devtox'].at[index]) + " (" +str(df_Extraction['OECD - Count'].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[OECD_index,3]) + " ; "
			df_Extraction['OECD - UMLS'].at[index] = str(df_Extraction['OECD - UMLS'].at[index]) + " (" +str(df_Extraction['OECD - Count'].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[OECD_index,1]) + " ; "
			df_Extraction['OECD - Count'].at[index] += 1
		if any(s in x for s in Combo_Row_List):
			df_Extraction['OECD'].at[index] = df_Extraction['OECD'].at[index] + " (" +str(df_Extraction['OECD - Count'].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[OECD_index,9]) + ";"
			df_Extraction['OECD - Devtox'].at[index] = str(df_Extraction['OECD - Devtox'].at[index]) + " (" +str(df_Extraction['OECD - Count'].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[OECD_index , 3 ]) + " ; "
			df_Extraction['OECD - UMLS'].at[index] = str(df_Extraction['OECD - UMLS'].at[index]) + " (" +str(df_Extraction['OECD - Count'].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[OECD_index,1]) + " ; "
			df_Extraction['OECD - Count'].at[index] += 1
		if any(s in x for s in Unique_Row_List):
			df_Extraction['OECD'].at[index] = df_Extraction['OECD'].at[index] + " (" +str(df_Extraction['OECD - Count'].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[OECD_index,9]) + ";"
			df_Extraction['OECD - Devtox'].at[index] = str(df_Extraction['OECD - Devtox'].at[index]) + " (" +str(df_Extraction['OECD - Count'].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[OECD_index , 3 ]) + " ; "
			df_Extraction['OECD - UMLS'].at[index] = str(df_Extraction['OECD - UMLS'].at[index]) + " (" +str(df_Extraction['OECD - Count'].at[index]) + ") " + str(df_Crosswalk_Synonyms.iloc[OECD_index,1]) + " ; "
			df_Extraction['OECD - Count'].at[index] += 1			

##____________________APPLYING HARDCODES____________________### 
	for hc_index,hc_row in df_HardCodes.iterrows():
		if (hc_row['UMLS Blank?'] == "y" and df_Extraction['UMLS'].at[index] == "") or (hc_row['UMLS Blank?'] == "n"):
			includes = hc_row[0].split('and')
			not_includes = hc_row[1].split('and')
			includes_2 = []
			not_includes_2 = []
			for i in includes:
				includes_2.append(str(i).replace( "]", "").replace("[","").split(","))
			for n in not_includes:
				not_includes_2.append(str(n).replace( "]", "").replace("[","").split(","))
			includes_all_true_test = []
			not_includes_all_true_test = []
			for x in includes_2:
				if any(s in row['Effects'] for s in x):
					includes_all_true_test.append(True)
				else: includes_all_true_test.append(False)
			for x in not_includes_2:
				if any(s in row['Effects'] for s in x):
					not_includes_all_true_test.append(True)
				else: not_includes_all_true_test.append(False)
			if all(includes_all_true_test) and (not any(not_includes_all_true_test)):
				df_Extraction['UMLS'].at[index] += df_Extraction['UMLS'].at[index] + " (" +str(df_Extraction['UMLS - Count'].at[index]) + ") " + "HC_" +str(hc_row['UMLS codes']) + ";"
				df_Extraction['DevTox'].at[index] += df_Extraction['DevTox'].at[index] + " (" +str(df_Extraction['DevTox - Count'].at[index]) + ") " + "HC_" +str(hc_row['DevTox codes']) + ";"
				df_Extraction['OECD'].at[index] += df_Extraction['OECD'].at[index] + " (" +str(df_Extraction['OECD - Count'].at[index]) + ") " + "HC_" +str(hc_row['OECD codes']) + ";"

				df_Extraction['UMLS - Count'].at[index] += 1
df_Extraction["UMLS - Count"] = df_Extraction["UMLS - Count"] - 1
df_Extraction["DevTox - Count"] = df_Extraction["DevTox - Count"] - 1
df_Extraction["OECD - Count"] = df_Extraction["OECD - Count"] - 1

print("Exporting Results")

# code for deduplicating Codes
def uniqueCodes(test):
	UMLS_LIST = [x[4:] for x in ([x.strip() for x in test.split('; ') if x != ''])]
	UMLS_LIST = [x.strip() for x in UMLS_LIST]
	if UMLS_LIST != [] and UMLS_LIST[-1][-1] ==";" :
		UMLS_LIST[-1] = UMLS_LIST[-1][:-1] 
	a = list(OrderedDict.fromkeys(UMLS_LIST)) # Splits the string into UMLS codes
	b = [] # Create new list to append deduplicated UMLS codes
	for i in range(len(a)): # This loop adds back the numbers to the the deduplicated string
		number = i + 1
		value = a[i]
		b.append(f'({number}) {value}') # append new string to list
	dedupCount = (len(b))  
	c = '; '.join(b) # join list of strings together with '; '
	return(dedupCount, c) # returns deduplicated UMLS codes without the space in front of the original string

df_Extraction['Concat_UMLS'] = df_Extraction['UMLS'] + df_Extraction['DevTox - UMLS'] +  df_Extraction['OECD - UMLS'] 
df_Extraction['Concat_DevTox'] = df_Extraction["DevTox"] + df_Extraction['UMLS - Devtox'] +  df_Extraction['OECD - Devtox'] 
df_Extraction['Concat_OECD'] =   df_Extraction['OECD'] + df_Extraction['UMLS - OECD'] +  df_Extraction['DevTox - OECD'] 

df_Extraction['Deduplicated ALL UMLS'] = df_Extraction['Concat_UMLS'].apply(uniqueCodes)
df_Extraction["Deduplicated ALL UMLS Count"] = (df_Extraction['Deduplicated ALL UMLS'].str[0])
df_Extraction['Deduplicated ALL UMLS'] = (df_Extraction['Deduplicated ALL UMLS'].str[1])

df_Extraction['Deduplicated UMLS'] = df_Extraction['UMLS'].apply(uniqueCodes)
df_Extraction["Deduplicated UMLS Count"] = (df_Extraction['Deduplicated UMLS'].str[0])
df_Extraction['Deduplicated UMLS'] = (df_Extraction['Deduplicated UMLS'].str[1])

df_Extraction['Deduplicated UMLS - OECD'] = df_Extraction['UMLS - OECD'].apply(uniqueCodes)
df_Extraction["Deduplicated UMLS - OECD Count"] = (df_Extraction['Deduplicated UMLS - OECD'].str[0])
df_Extraction['Deduplicated UMLS - OECD'] = (df_Extraction['Deduplicated UMLS - OECD'].str[1])

df_Extraction['Deduplicated UMLS - Devtox'] = df_Extraction['UMLS - Devtox'].apply(uniqueCodes)
df_Extraction["Deduplicated UMLS - Devtox Count"] = (df_Extraction['Deduplicated UMLS - Devtox'].str[0])
df_Extraction['Deduplicated UMLS - Devtox'] = (df_Extraction['Deduplicated UMLS - Devtox'].str[1])


df_Extraction['Deduplicated ALL DevTox'] = df_Extraction['Concat_DevTox'].apply(uniqueCodes)
df_Extraction['Deduplicated ALL DevTox Count']  = df_Extraction['Deduplicated ALL DevTox'].str[0]
df_Extraction['Deduplicated ALL DevTox'] = df_Extraction['Deduplicated ALL DevTox'].str[1]

df_Extraction['Deduplicated DevTox'] = df_Extraction['DevTox'].apply(uniqueCodes)
df_Extraction['Deduplicated DevTox Count']  = df_Extraction['Deduplicated DevTox'].str[0]
df_Extraction['Deduplicated DevTox'] = df_Extraction['Deduplicated DevTox'].str[1]

df_Extraction['Deduplicated DevTox - UMLS'] = df_Extraction['DevTox - UMLS'].apply(uniqueCodes)
df_Extraction["Deduplicated DevTox - UMLS Count"] = (df_Extraction['Deduplicated DevTox - UMLS'].str[0])
df_Extraction['Deduplicated DevTox - UMLS'] = (df_Extraction['Deduplicated DevTox - UMLS'].str[1])

df_Extraction['Deduplicated DevTox - OECD'] = df_Extraction['DevTox - OECD'].apply(uniqueCodes)
df_Extraction['Deduplicated DevTox - OECD Count']  = df_Extraction['Deduplicated DevTox - OECD'].str[0]
df_Extraction['Deduplicated DevTox - OECD'] = df_Extraction['Deduplicated DevTox - OECD'].str[1]


df_Extraction['Deduplicated ALL OECD'] = df_Extraction['Concat_OECD'].apply(uniqueCodes)
df_Extraction['Deduplicated ALL OECD Count']  = df_Extraction['Deduplicated ALL OECD'].str[0]
df_Extraction['Deduplicated ALL OECD'] = df_Extraction['Deduplicated ALL OECD'].str[1]

df_Extraction['Deduplicated OECD'] = df_Extraction['OECD'].apply(uniqueCodes)
df_Extraction['Deduplicated OECD Count']  = df_Extraction['Deduplicated OECD'].str[0]
df_Extraction['Deduplicated OECD'] = df_Extraction['Deduplicated OECD'].str[1]

df_Extraction['Deduplicated OECD - Devtox'] = df_Extraction['OECD - Devtox'].apply(uniqueCodes)
df_Extraction['Deduplicated OECD - Devtox Count']  = df_Extraction['Deduplicated OECD - Devtox'].str[0]
df_Extraction['Deduplicated OECD - Devtox'] = df_Extraction['Deduplicated OECD - Devtox'].str[1]

df_Extraction['Deduplicated OECD - UMLS'] = df_Extraction['OECD - UMLS'].apply(uniqueCodes)
df_Extraction['Deduplicated OECD - UMLS Count']  = df_Extraction['Deduplicated OECD - UMLS'].str[0]
df_Extraction['Deduplicated OECD - UMLS'] = df_Extraction['Deduplicated OECD - UMLS'].str[1]


writer = pd.ExcelWriter(xlsxFilename + ".xlsx")

df_Extraction_Concat_Results = df_Extraction.drop(['Deduplicated UMLS - OECD','Deduplicated UMLS - Devtox',"Deduplicated UMLS - OECD Count","Deduplicated UMLS - Devtox Count",'Deduplicated DevTox - UMLS','Deduplicated DevTox - UMLS Count','Deduplicated DevTox - OECD', 'Deduplicated DevTox - OECD Count','Deduplicated ALL OECD','Deduplicated ALL OECD Count', 'Deduplicated OECD - Devtox', 'Deduplicated OECD - Devtox Count','Deduplicated OECD - UMLS', 'Deduplicated OECD - UMLS Count',"Deduplicated ALL DevTox", "Deduplicated ALL DevTox Count" ] , axis=1 )


df_Extraction_Concat_Results.to_excel(writer, sheet_name="Extraction_Concat_Results")
df_Extraction.to_excel(writer, sheet_name="Extraction_All_Results")


writer.save()

stop = timeit.default_timer()
print('Processing Time (seconds): ', stop - start)




'''____________________OLD CODE__________________________

# df_Extraction.to_excel("Extraction_Results_ConcatTest.xlsx", sheet_name="Extraction_All_Results")
# df_Extraction.to_excel("Extraction_Results_ConcatTest.xlsx", sheet_name="Extraction_Concat_Results")


### SPECIAL CASES APPLIED HERE### 
	if row['UMLS'] == "" :
		if (any(s in row['Effects'] for s in LitterList)) and (any(s in row['Effects'] for s in DefectList)) and (row['UMLS'] == "") and (re.search(r'\b' + 'dam' + r'\b', row['Effects'])== None) and (re.search(r'\b' + 'dams' + r'\b', row['Effects'])== None) and (re.search(r'\b' + 'doe' + r'\b', row['Effects'])== None) and (re.search(r'\b' + 'does' + r'\b', row['Effects'])== None):
			df_Extraction['UMLS'].at[index] = df_Extraction['UMLS'].at[index] + " (" +str(df_Extraction['UMLS - Count'].at[index]) + ") " + str('UMLS;C0000768;CUI;Congenital Abnormality') + ";"
			df_Extraction['UMLS - Count'].at[index] += 1
		if any(s in row['Effects'] for s in SexList) and (re.search(r'\b' + 'ratio' + r'\b', row['Effects'])!= None):
			df_Extraction['UMLS'].at[index] = df_Extraction['UMLS'].at[index] + " (" +str(df_Extraction['UMLS - Count'].at[index]) + ") " + str('UMLS;C0035150;CUI;Reproduction|UMLS;C4086311;CUI;Fetal Female Sex Ratio|UMLS;C4086312;CUI;Fetal Male Sex Ratio') + ";"
			df_Extraction['UMLS - Count'].at[index] += 1
		if "study information row" in row['Effects']:
			df_Extraction['UMLS'].at[index] = df_Extraction['UMLS'].at[index] + " (" +str(df_Extraction['UMLS - Count'].at[index]) + ") " + str('N/A') + ";"
			df_Extraction['UMLS - Count'].at[index] += 1
		if (re.search( r'\b' + 'rib' + r'\b' , row['Effects'])) or (re.search(r'\b' + 'ribs' + r'\b', row['Effects'] ) != None):
			df_Extraction['UMLS'].at[index] = df_Extraction['UMLS'].at[index] + " (" +str(df_Extraction['UMLS - Count'].at[index]) + ") " + str('UMLS;C0000768;CUI;Congenital Abnormality|UMLS;C0265509;CUI;Congenital anomaly of skeletal bone|UMLS;C0205394;CUI;Other') + ";"
			df_Extraction['UMLS - Count'].at[index] += 1
		if any(s in row['Effects'] for s in ['ossif','unossif']):
			df_Extraction['UMLS'].at[index] = df_Extraction['UMLS'].at[index] + " (" +str(df_Extraction['UMLS - Count'].at[index]) + ") " + str('UMLS;C0000768;CUI;Congenital Abnormality|UMLS;C0265509;CUI;Congenital anomaly of skeletal bone|UMLS;C0205394;CUI;Other') + ";"
			df_Extraction['UMLS - Count'].at[index] += 1
		if (not any(s in row['Effects'] for s in ['fetal','organ','ratio'])) and (re.search(r'\b' + 'body weight' + r'\b', row['Effects'] ) != None) and (row['Localizations'] == ""):
			df_Extraction['UMLS'].at[index] = df_Extraction['UMLS'].at[index] + " (" +str(df_Extraction['UMLS - Count'].at[index]) + ") " + str('UMLS;C0005910;CUI;Body Weight|UMLS;C3540840;CUI;Sign or Symptom|UMLS;C0005910;CUI;Body Weight') + ";"
			df_Extraction['UMLS - Count'].at[index] += 1
		if (not any(s in row['Effects'] for s in ['live fetus with','live fetuses with','live litter with'])) and ((re.search(r'\s' + 'live fetus' + r'\b', row['Effects'] ) or re.search(r'\s' + 'live fetuses' + r'\b', row['Effects'])) != None):
			df_Extraction['UMLS'].at[index] = df_Extraction['UMLS'].at[index] + " (" +str(df_Extraction['UMLS - Count'].at[index]) + ") " + str('umls;c0015954;cui;fetal viability|umls;c4086644;cui;number of live fetuses') + ";"
			df_Extraction['UMLS - Count'].at[index] += 1
		if (any(s in row['Effects'] for s in ["dead fetus","dead fetuses","non-live fetus","non-live","fetal death","fetal deaths"])) and (not any(s in row['Effects'] for s in ["dead fetus with","dead fetuses with","non-live fetus with","non-live with","fetal death with","fetal deaths with"])):
			df_Extraction['UMLS'].at[index] = df_Extraction['UMLS'].at[index] + " (" +str(df_Extraction['UMLS - Count'].at[index]) + ") " + str('umls;c0015954;cui;fetal viability|umls;c4086636;cui;number of dead fetuses') + ";"
			df_Extraction['UMLS - Count'].at[index] += 1
			
			
'''			