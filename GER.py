import os
from shutil import copyfile
from subprocess import PIPE
import subprocess
import time
import pandas as pd
import os.path
import datetime
from datetime import datetime
from datetime import timedelta
import numpy as np

def deletecopymine():

    #-------------------------------------------------------------------------------------------------------------------
    #Delete Files ------------------------------------------------------------------------------------------------------
    #-------------------------------------------------------------------------------------------------------------------

    NumberOfFiles = 0
    #GER
    for filename in os.listdir("GERData"): #Iterates through files in folder in Python directory
        if filename.endswith(".csv"): #Can use or here as well
            # print(os.path.join(filename)) #Prints files in above directory
            filetodelete = "C:/Users/Chris Norris/Documents/Priva CSV/GER/"+filename #Name of file in Priva CSV tool to delete
            if os.path.exists(filetodelete) == True: #If above path exists
                os.remove(filetodelete) #Remove file
                NumberOfFiles = NumberOfFiles + 1#Can be some issues - for example if files are deleted by other programs prior, the delete count will be low. And then if it errors it could finish even though its not done
                                                  #Not much of an alternative at this stage - and should be rare. Still a bucketload better.
    print(str(NumberOfFiles) + " Files Deleted!")

    #-------------------------------------------------------------------------------------------------------------------
    #Mine Priva --------------------------------------------------------------------------------------------------------
    #-------------------------------------------------------------------------------------------------------------------

    from datetime import datetime, timedelta

    days = 16 #<--------------------------------------------------------------------------------------------------------Change days if needed

    #Works out start date
    dt_today = str(datetime.now().strftime('%Y-%m-%d'))
    dt_minus14 = str((datetime.today() - timedelta(days)).strftime('%Y-%m-%d'))

    GER = r'cmd /K "C:\Users\Chris Norris\Desktop\PrivaDataMiner\priva-to-csv_v3\priva-to-csv\priva-to-csv.exe" -s 192.168.95.201 -f '+ dt_minus14 + ' -l '+ dt_today +' -i 300'

    print(GER)


    FilesLeft = 1 #This is just 1 or 0
    WroteCount = 0 #this keeps track of total files wrote, even if it reruns
    ErrorCount = -1 #to try and not abuse system
    SecondsToWait = 10 #expands if errors reach too high

    while FilesLeft == 1:  # will keep running until this is 0 - and will only downloaded files not written

        time.sleep(SecondsToWait)  # To not just keep hammering system
        ErrorCount = ErrorCount + 1
        print('Error Count ' + str(ErrorCount))

        if ErrorCount == 10:
            SecondsToWait = 300
            print('Keeps erroring, waiting 5 minutes before trying again')

        #Now runs Priva Tool
        env = os.environ
        x = 0
        p = subprocess.Popen(GER, env=env,shell=False,stdin=PIPE,stdout=PIPE)

        while x == 0: #Need to add in error loops - need to find the text?
            #time.sleep(0.5)
            one_line_output = str(p.stdout.readline())
            print('-------------------------------------------------------------------------------------------------------------')
            print(one_line_output)
            print('-------------------------------------------------------------------------------------------------------------')

            if r"b'Wrote" in one_line_output:
                WroteCount = WroteCount + 1
                print('Wrote File Count ' + str(WroteCount))

            if WroteCount == NumberOfFiles:  # Will close main loop when this is triggered
                FilesLeft = 0

            if one_line_output == r"b'\r\n'":
                print('Gather tool closing')
                p.terminate()
                p.kill()
                x = 1

    print(str(WroteCount) + ' out of ' + str(NumberOfFiles) + ' gathered')
    print('Finished gather successfully')


    time.sleep(5) # <-- sleep for 5

    #-------------------------------------------------------------------------------------------------------------------
    #Copy Files --------------------------------------------------------------------------------------------------------
    #-------------------------------------------------------------------------------------------------------------------

    #GER
    for filename in os.listdir("GERData"): #Iterates through files in folder in Python directory
        if filename.endswith(".csv"): #Can use or here as well
            # print(os.path.join(filename)) #Prints files in above directory
            filetocopy = "C:/Users/Chris Norris/Documents/Priva CSV/GER/"+filename #Name of file in Priva CSV tool to delete
            if os.path.exists(filetocopy) == True: #If above path exists
                copyfile(filetocopy, "GERData\\"+filename)


    print('Deleted, Mined and Copied!')

def processdata():
    file_name = 'GER_climate.csv'  # file created

    # region GatherData
    # ------------------------------------------------------------------------------------------------------------------
    # Light and Watts---------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    # This dataframe will be index and be sorted

    col_list1 = ['radiation sum (J/cm²)', 'radiation (W/m²)']  # Gets rid of all columns except these

    columnnames = ['GER Joules', 'GER Watts']  # Rename columns above to this

    lightDF = pd.read_csv('GERData\GER WEATHER RAD SENS.csv', index_col=0, low_memory=False,
                          skipinitialspace=True)  # Read File
    lightDF = lightDF[col_list1]  # This cuts down DF to col_list
    lightDF.index.names = ['Date and Time']  # This renames index column
    lightDF = lightDF.sort_values(by=['Date and Time'])  # Sorts by First Column

    lightDF.columns = columnnames  # Renames columns

    # ------------------------------------------------------------------------------------------------------------------
    # Outside Temp------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    # This dataframe is a single csv, so no if

    col_list1 = ['rain', 'wind direction deg (°)', 'wind speed (m/s)',
                 'outside temp (°C)']  # Gets rid of all columns except these

    columnnames = ['GER Rain', 'GER Wind Direction', 'GER Wind Speed',
                   'GER Outside Temp']  # Rename columns above to this

    outsideDF = pd.read_csv('GERData\GER WEATHER.csv', index_col=0, low_memory=False,
                            skipinitialspace=True)  # Read File
    outsideDF = outsideDF[col_list1]  # This cuts down DF to col_list
    outsideDF.index.names = ['Date and Time']  # This renames index column
    outsideDF.columns = columnnames  # Renames columns

    GERClimateDF = pd.merge(lightDF, outsideDF, left_index=True, right_index=True,
                            how='outer')  # Merges first two data frames together
    del lightDF
    del outsideDF

    # ------------------------------------------------------------------------------------------------------------------
    # Drain and EC and pH-----------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    # multiple CSV, so if cycles through and renames columns

    numberofreports = 5
    col_list1 = ['meas pH drain (pH)', 'meas EC drain (mS/cm)', '24h drain (l/m²)', '24h dose (l/m²)', '24h drain% (%)',
                 '24h take up (l/m²)']  # Gets rid of all columns except these

    numbercmp = 1
    for x in range(numberofreports):
        columnnames = ['GER pH Drain ' + str(numbercmp), 'GER EC Drain ' + str(numbercmp),
                       'GER 24hr Drain ' + str(numbercmp), 'GER 24hr Dose ' + str(numbercmp),
                       'GER 24hr Drain % ' + str(numbercmp), 'GER 24hr Take Up ' + str(numbercmp)]

        drainDF = pd.read_csv('GERData\GER MOIST MEAS ' + str(numbercmp) + '.csv', index_col=0, low_memory=False,
                              skipinitialspace=True)
        drainDF = drainDF[col_list1]
        drainDF.index.names = ['Date and Time']  # This renames index column
        drainDF.columns = columnnames

        GERClimateDF = pd.merge(GERClimateDF, drainDF, left_index=True, right_index=True, how='outer')
        numbercmp = numbercmp + 1

    del drainDF

    # ------------------------------------------------------------------------------------------------------------------
    # Drip (At Least Mixing Tank) EC and pH-----------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    # multiple CSV, so if cycles through and renames columns

    numberofreports = 5
    col_list1 = ['meas pH (pH)', 'meas EC (mS/cm)',
                 'meas flow m3 (m3/hr)']  # Gets rid of all columns except these

    numbercmp = 1
    for x in range(numberofreports):
        columnnames = ['GER ph Drip ' + str(numbercmp),
                       'GER EC Drip ' + str(numbercmp), 'GER Flow Rate ' + str(numbercmp)]

        dripDF = pd.read_csv('GERData\GER WAT SYS ' + str(numbercmp) + '.csv', index_col=0, low_memory=False,
                             skipinitialspace=True)
        dripDF = dripDF[col_list1]
        dripDF.index.names = ['Date and Time']  # This renames index column
        dripDF.columns = columnnames

        GERClimateDF = pd.merge(GERClimateDF, dripDF, left_index=True, right_index=True, how='outer')
        numbercmp = numbercmp + 1

    del dripDF

    # ------------------------------------------------------------------------------------------------------------------
    # Consumption NEW GER PRIVA REPORT - USED TO BE IN SECTION ABOVR-----------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    # multiple CSV, so if cycles through and renames columns

    numberofreports = 5
    col_list1 = ['consumption (m3)']  # Gets rid of all columns except these

    numbercmp = 1
    for x in range(numberofreports):
        columnnames = ['GER M3 Water Per Day '+ str(numbercmp)]

        dripDF2 = pd.read_csv('GERData\GER WAT SYS ' + str(numbercmp) + ' WATER IRRIG.csv', index_col=0, low_memory=False,
                             skipinitialspace=True)

        dripDF2 = dripDF2[col_list1]
        dripDF2.index.names = ['Date and Time']  # This renames index column
        dripDF2.columns = columnnames

        GERClimateDF = pd.merge(GERClimateDF, dripDF2, left_index=True, right_index=True, how='outer')
        numbercmp = numbercmp + 1

    del dripDF2

    # ------------------------------------------------------------------------------------------------------------------
    # Grow Scales-------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    # multiple CSV, so if cycles through and renames columns

    numberofreports = 7

    numbercmp = 6
    for x in range(numberofreports):

        col_list1 = ['meas kg slab (kg)', 'meas moist slab (%)']  # Gets rid of all columns except these
        columnnames = ['GER Slab Weight ' + str(numbercmp), 'GER Moist Slab % ' + str(numbercmp)]

        if numbercmp > 10:  # This is because the last two reports don't have Moist Slab
            col_list1 = ['meas kg slab (kg)']  # Gets rid of all columns except these
            columnnames = ['Slab Weight ' + str(numbercmp)]

        ghweightDF = pd.read_csv('GERData\GER MOIST MEAS ' + str(numbercmp) + ' SLAB WEIGHER.csv', index_col=0,
                                 low_memory=False,
                                 skipinitialspace=True)
        ghweightDF = ghweightDF[col_list1]
        ghweightDF.index.names = ['Date and Time']  # This renames index column
        ghweightDF.columns = columnnames

        GERClimateDF = pd.merge(GERClimateDF, ghweightDF, left_index=True, right_index=True, how='outer')
        numbercmp = numbercmp + 1

    del ghweightDF

    # ------------------------------------------------------------------------------------------------------------------
    # GH Temp and Humdidity and EC--------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    # multiple CSV, so if cycles through and renames columns

    numberofreports = 16
    col_list1 = ['calc heat t (°C)', 'meas CO2 (ppm)', 'meas HD (g/m3)', 'meas RH (%)',
                 'meas grh temp (°C)']  # Gets rid of all columns except these

    numbercmp = 1
    for x in range(numberofreports):
        columnnames = ['GER Calc Heat Temp ' + str(numbercmp), 'GER C02 PPM ' + str(numbercmp),
                       'GER HD ' + str(numbercmp), 'GER RH % ' + str(numbercmp),
                       'GER GH Temp ' + str(numbercmp)]

        ghtempDF = pd.read_csv('GERData\GER CMP ' + str(numbercmp) + '.csv', index_col=0, low_memory=False,
                               skipinitialspace=True)
        ghtempDF = ghtempDF[col_list1]
        ghtempDF.index.names = ['Date and Time']  # This renames index column
        ghtempDF.columns = columnnames

        GERClimateDF = pd.merge(GERClimateDF, ghtempDF, left_index=True, right_index=True, how='outer')
        numbercmp = numbercmp + 1

    del ghtempDF

    # ------------------------------------------------------------------------------------------------------------------
    # Pipe Temp---------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    # multiple CSV, plus sub csvs per compartment. This cycles up to 3 - so cmp 1 1 cmp 1 2 etc,

    numberofreports = 16
    numberofsubreports = 3

    numbercmp = 1
    for x in range(numberofreports):
        col_list1 = ['calc max wt (°C)', 'calc min wt (°C)', 'calc wt (°C)',
                     'meas wt (°C)']  # Gets rid of all columns except these
        columnnames = ['GER Calc Max WT ' + str(numbercmp), 'GER Calc Min WT ' + str(numbercmp),
                       'GER Calc WT ' + str(numbercmp), 'GER Meas WT ' + str(numbercmp)]

        if os.path.exists(
                'GERData\GER CMP ' + str(numbercmp) + ' WATER TEMP.csv') == True:  # If no sub report, do as normal

            pipetempDF = pd.read_csv('GERData\GER CMP ' + str(numbercmp) + ' WATER TEMP.csv', index_col=0,
                                     low_memory=False, skipinitialspace=True)
            pipetempDF = pipetempDF[col_list1]  # Gets rid of all columns except these
            pipetempDF.index.names = ['Date and Time']  # This renames index column
            pipetempDF.columns = columnnames

            GERClimateDF = pd.merge(GERClimateDF, pipetempDF, left_index=True, right_index=True, how='outer')

        # Now looks for sub reports - assumes column names will adjust to sub report number

        for x in range(1,
                       numberofsubreports + 1):  # For each sub report, see if you can find it. If you can do, add to dataframe. This range starts at one and end at 3.

            if os.path.exists('GERData\GER CMP ' + str(numbercmp) + ' WATER TEMP ' + str(x) + '.csv') == True:
                col_list1 = ['calc max wt ' + str(x) + ' (°C)', 'calc min wt ' + str(x) + ' (°C)',
                             'calc wt ' + str(x) + ' (°C)',
                             'meas wt ' + str(x) + ' (°C)']  # Gets rid of all columns except these




                columnnames = ['GER Calc Max WT ' + str(numbercmp) + '-' + str(x),
                               'GER Calc Min WT ' + str(numbercmp) + '-' + str(x),
                               'GER Calc WT ' + str(numbercmp) + '-' + str(x),
                               'GER Meas WT ' + str(numbercmp) + '-' + str(x)]


                # Do this due to new UVs changing names of variables----------------------------------------------------
                ReportName = 'GERData\GER CMP ' + str(numbercmp) + ' WATER TEMP ' + str(x) + '.csv'

                pipetempDF = pd.read_csv(ReportName,
                                         index_col=0, low_memory=False, skipinitialspace=True)

                if ReportName == "GERData\GER CMP 1 WATER TEMP 2.csv":
                    col_list1 = ['calc max wt ' + str(x) + ' (°C)', 'calc min wt ' + str(x) + ' (°C)',
                                 'calc wt ' + str(x)+ ' (°C)',
                                 # 'calc wt ' + str(x),
                                 'meas wt ' + str(x) + ' (°C)']

                if ReportName == "GERData\GER CMP 12 WATER TEMP 1.csv":
                    col_list1 = ['calc max wt ' + str(x) + ' (°C)', 'calc min wt ' + str(x) + ' (°C)',
                                 'calc wt ' + str(x)+ ' (°C)',
                                 #'meas wt ' + str(x)
                                 'meas wt ' + str(x) + ' (°C)']
                #-------------------------------------------------------------------------------------------------------



                print('GERData\GER CMP ' + str(numbercmp) + ' WATER TEMP ' + str(x) + '.csv')
                pipetempDF = pipetempDF[col_list1]  # Gets rid of all columns except these
                pipetempDF.index.names = ['Date and Time']  # This renames index column
                pipetempDF.columns = columnnames

                GERClimateDF = pd.merge(GERClimateDF, pipetempDF, left_index=True, right_index=True, how='outer')

        numbercmp = numbercmp + 1

    del pipetempDF


    # ------------------------------------------------------------------------------------------------------------------
    # Boiler -----------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    # multiple CSV, so if cycles through and renames columns

    numberofreports = 2
    col_list1 = ['act capacity (kW)']  # Gets rid of all columns except these

    numbercmp = 1
    for x in range(numberofreports):
        columnnames = ['GER Boiler KW ' + str(numbercmp)]

        boilerDF = pd.read_csv('GERData\GER BOILER ' + str(numbercmp) + '.csv', index_col=0, low_memory=False,
                             skipinitialspace=True)

        boilerDF = boilerDF[col_list1]
        boilerDF.index.names = ['Date and Time']  # This renames index column
        boilerDF.columns = columnnames

        GERClimateDF = pd.merge(GERClimateDF, boilerDF, left_index=True, right_index=True, how='outer')
        numbercmp = numbercmp + 1

    del boilerDF


    # ------------------------------------------------------------------------------------------------------------------
    # ECS UV Reycling EC-------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    # multiple CSV, so if cycles through and renames columns

    numberofreports = 5
    col_list1 = ['meas EC supp (mS/cm)']  # Gets rid of all columns except these

    numbercmp = 6
    for x in range(numberofreports):
        columnnames = ['Recycling EC ' + str(numbercmp)]

        dripDF = pd.read_csv('GERData\GER WAT SYS ' + str(numbercmp) + '.csv', index_col=0, low_memory=False,
                             skipinitialspace=True)
        dripDF = dripDF[col_list1]
        dripDF.index.names = ['Date and Time']  # This renames index column
        dripDF.columns = columnnames

        GERClimateDF = pd.merge(GERClimateDF, dripDF, left_index=True, right_index=True, how='outer')
        numbercmp = numbercmp + 1

    del dripDF


    # endregion
    # region Calculation&Average
    # -------------------------------------------------------------------------------------------------------------------
    # Calculated Columns-------------------------------------------------------------------------------------------------
    # -------------------------------------------------------------------------------------------------------------------
    # https://towardsdatascience.com/basic-time-series-manipulation-with-pandas-4432afee64ea
    # http://openmetric.org/til/programming/pandas-new-column-from-other/
    # https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html
    # https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior
    # https://pythontic.com/datetime/date/strftime  # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.dt.strftime.html
    # https://stackoverflow.com/questions/41167715/convert-a-common-date-format-in-an-iso-week-date-format

    # Date
    GERClimateDF.insert(0, 'Date', pd.to_datetime(GERClimateDF.index))
    GERClimateDF['Date'] = GERClimateDF['Date'].dt.date

    # GH 24 Hour Date
    t1 = timedelta(hours=6, minutes=00)

    GERClimateDF.insert(1, 'GH 24 Hr Date', pd.to_datetime(GERClimateDF.index))
    GERClimateDF['GH 24 Hr Date'] = GERClimateDF['GH 24 Hr Date'] - t1
    GERClimateDF['GH 24 Hr Date'] = GERClimateDF['GH 24 Hr Date'].dt.date

    # Isoweek
    GERClimateDF.insert(2, 'Isoweek', pd.to_datetime(GERClimateDF.index))
    GERClimateDF['Isoweek'] = GERClimateDF['Isoweek'].dt.strftime('%g%V')  # g is iso year, v is isoweek



    # Time
    GERClimateDF.insert(3, 'Time', pd.to_datetime(GERClimateDF.index))
    GERClimateDF['Time'] = GERClimateDF['Time'].dt.time

    # Hour
    GERClimateDF.insert(4, 'Hour', pd.to_datetime(GERClimateDF.index))
    GERClimateDF['Hour'] = GERClimateDF['Hour'].dt.hour





    # Code to Display TRUE when the joules change over to a new day*****************************************************
    GERClimateDF.insert(6, "CrazyIdea",pd.to_datetime(GERClimateDF.index))
    GERClimateDF['CrazyIdea'] = GERClimateDF['GER Joules']
    GERClimateDF["CrazyIdea"] = ~(
        GERClimateDF[(GERClimateDF['CrazyIdea'] < GERClimateDF["CrazyIdea"].shift()) & (GERClimateDF['Hour'] > 4) & (GERClimateDF['Hour'] < 9)].duplicated(
            subset=["Date", "CrazyIdea"], keep="first"))#Change this to determine when to toggle the night
    GERClimateDF["CrazyIdea"].fillna(False,inplace=True)

    GERClimateDF.insert(7, "NewDayDetector", 1)

    GERClimateDF['NewDayDetector'] = np.where(GERClimateDF.groupby('Date')['CrazyIdea'].cumsum() <= 1, GERClimateDF['CrazyIdea'], False)
    del GERClimateDF['CrazyIdea']


    #Calculates the Joule Date with lots of error checking**************************************************************
    GERClimateDF.insert(8, "Date Joule Day", 0)

    col1 = GERClimateDF.columns.get_loc('NewDayDetector')  # column
    col2 = GERClimateDF.columns.get_loc('Date Joule Day')  # column
    col3 = GERClimateDF.columns.get_loc('Date')  # column
    col4 = GERClimateDF.columns.get_loc('Hour')  # column`
    GERClimateDF.iloc[0, col2] = GERClimateDF.iloc[0, col3] # sets the first row to be the date, as there is nothing above

    for i in range(0, len(GERClimateDF)):  # From row 0 to last row in data frame, iterate through i
        if GERClimateDF.iat[i, col1] == True or int(GERClimateDF.iat[i, col4]) > 9: # if the New Day Detector = True or the hour is greeater than 9***************************
            GERClimateDF.iloc[i, col2] = GERClimateDF.iloc[i, col3] # add in the value of the date column
        elif GERClimateDF.iat[i, col2] == 0: # else if there is a 0
            GERClimateDF.iloc[i, col2] = GERClimateDF.iloc[i - 1, col2] # add the value above


    #
    #     # Day/Night - Create column that displays True when the the watts equal 0 for the first time in the day
    #     # https://stackoverflow.com/questions/58686797/dataframe-create-a-column-based-on-another-with-if-formulas/58687198#58687198

    # Day Night Finder https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.duplicated.html
    GERClimateDF.insert(6, "Day/Night - Toggle",
                        pd.to_datetime(GERClimateDF.index))  # Create column based on existing index
    GERClimateDF['Day/Night - Toggle'] = GERClimateDF['GER Watts']
    # GERClimateDF["Day/Night 2"] = ~(GERClimateDF[GERClimateDF["Day/Night 2"] == 0].duplicated(subset=["Date", "Day/Night 2"], keep="first"))
    GERClimateDF["Day/Night - Toggle"] = ~(
        GERClimateDF[(GERClimateDF['Day/Night - Toggle'] < 5) & (GERClimateDF['Hour'] > 14)].duplicated(
            subset=["Date", "Day/Night - Toggle"], keep="first"))#Change this to determine when to toggle the night
    GERClimateDF["Day/Night - Toggle"].fillna(False, inplace=True)


    # Day/Night - Traditional
    time_str = '20:00:00'
    time_str2 = '06:00:00'
    time_object = datetime.strptime(time_str, '%H:%M:%S').time()
    time_object2 = datetime.strptime(time_str2, '%H:%M:%S').time()


    GERClimateDF.insert(7, 'Day/Night - 6am to 8pm', pd.to_datetime(GERClimateDF.index))
    GERClimateDF['Day/Night - 6am to 8pm'] = GERClimateDF['Day/Night - 6am to 8pm'].dt.time
    GERClimateDF['Day/Night - 6am to 8pm'] = GERClimateDF['Day/Night - 6am to 8pm'].apply(lambda
                                                                    x: 'Day' if x < time_object and x >= time_object2 else 'Night')  # A lambda function can take any number of arguments, but can only have one expression. lambda arguments : expression


    #Day/Night -Creates Day/Night Column based on True False Column above
    #https://stackoverflow.com/questions/58921723/dataframe-calculated-column-based-on-2-criteria-to-find-day-night/58922076#58922076

    GERClimateDF.insert(8, 'Day/Night', GERClimateDF.index) # Create column based on existing index

    # This changes first row of Day/night to True - which toggles the code below to label it as night.
    # Else it will be day, since this data always starts at midnight, this hack should be effective.
    GERClimateDF.iloc[0, GERClimateDF.columns.get_loc('Day/Night - Toggle')] = True #loc needs an index, iloc is just the nth posistion in the column.

    #This cumulative sumns up the True/False column, resetting to 0 when there is a new day
    indicator = GERClimateDF.groupby('Date Joule Day')['Day/Night - Toggle'].cumsum()

    #where indicator is 1 (True), disply night, else day
    GERClimateDF['Day/Night'] = np.where(indicator, 'Night', 'Day')



    # GH 24 Hr Isoweek
    GERClimateDF.insert(3, 'GH 24 Hr Isoweek', pd.to_datetime(GERClimateDF['Date Joule Day']))
    GERClimateDF['GH 24 Hr Isoweek'] = GERClimateDF['GH 24 Hr Isoweek'].dt.strftime('%g%V')

    # ------------------------------------------------------------------------------------------------------------------
    # Create Average columns--------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    # https://stackoverflow.com/questions/33217636/mean-calculation-in-pandas-excluding-zeros
    # https://stackoverflow.com/questions/51109471/cannot-replace-with-method-pad-on-a-dataframe

    # GH Temp

    # GER 1
    columns = ['GER GH Temp 1', 'GER GH Temp 2']  # Columns to average
    GERClimateDF["GER 1 GH Temp"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                  skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)
    # GER 2
    columns = ['GER GH Temp 3', 'GER GH Temp 4', 'GER GH Temp 5', 'GER GH Temp 6']  # Columns to average
    GERClimateDF["GER 2 GH Temp"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                  skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)
    # GER 3
    columns = ['GER GH Temp 7', 'GER GH Temp 8', 'GER GH Temp 9', 'GER GH Temp 10']  # Columns to average
    GERClimateDF["GER 3 GH Temp"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                  skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)
    # GER 4
    columns = ['GER GH Temp 11', 'GER GH Temp 12', 'GER GH Temp 13', 'GER GH Temp 14']  # Columns to average
    GERClimateDF["GER 4 GH Temp"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                  skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)
    # GER 5
    columns = ['GER GH Temp 15', 'GER GH Temp 16']  # Columns to average
    GERClimateDF["GER 5 GH Temp"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                  skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)

    # C02 PPM

    # GER 1
    columns = ['GER C02 PPM 1', 'GER C02 PPM 2']  # Columns to average
    GERClimateDF["GER 1 C02 PPM"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                  skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)
    # GER 2
    columns = ['GER C02 PPM 3', 'GER C02 PPM 4', 'GER C02 PPM 5', 'GER C02 PPM 6']  # Columns to average
    GERClimateDF["GER 2 C02 PPM"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                  skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)
    # GER 3
    columns = ['GER C02 PPM 7', 'GER C02 PPM 8', 'GER C02 PPM 9', 'GER C02 PPM 10']  # Columns to average
    GERClimateDF["GER 3 C02 PPM"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                  skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)
    # GER 4
    columns = ['GER C02 PPM 11', 'GER C02 PPM 12', 'GER C02 PPM 13', 'GER C02 PPM 14']  # Columns to average
    GERClimateDF["GER 4 C02 PPM"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                  skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)
    # GER 5
    columns = ['GER C02 PPM 11', 'GER C02 PPM 12', 'GER C02 PPM 13', 'GER C02 PPM 14']  # Columns to average
    GERClimateDF["GER 5 C02 PPM"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                  skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)

    # GH Calc Heat Temp

    # GER 1
    columns = ['GER Calc Heat Temp 1', 'GER Calc Heat Temp 2']  # Columns to average
    GERClimateDF["GER 1 Calc Heat Temp"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                         skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)
    # GER 2
    columns = ['GER Calc Heat Temp 3', 'GER Calc Heat Temp 4', 'GER Calc Heat Temp 5',
               'GER Calc Heat Temp 6']  # Columns to average
    GERClimateDF["GER 2 Calc Heat Temp"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                         skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)
    # GER 3
    columns = ['GER Calc Heat Temp 7', 'GER Calc Heat Temp 8', 'GER Calc Heat Temp 9',
               'GER Calc Heat Temp 10']  # Columns to average
    GERClimateDF["GER 3 Calc Heat Temp"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                         skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)
    # GER 4
    columns = ['GER Calc Heat Temp 11', 'GER Calc Heat Temp 12', 'GER Calc Heat Temp 13',
               'GER Calc Heat Temp 14']  # Columns to average
    GERClimateDF["GER 4 Calc Heat Temp"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                         skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)
    # GER 5
    columns = ['GER Calc Heat Temp 15', 'GER Calc Heat Temp 16']  # Columns to average
    GERClimateDF["GER 5 Calc Heat Temp"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                         skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)

    # GH HD

    # GER 1
    columns = ['GER HD 1', 'GER HD 2']  # Columns to average
    GERClimateDF["GER 1 HD"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                             skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)
    # GER 2
    columns = ['GER HD 3', 'GER HD 4', 'GER HD 5', 'GER HD 6']  # Columns to average
    GERClimateDF["GER 2 HD"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                             skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)
    # GER 3
    columns = ['GER HD 7', 'GER HD 8', 'GER HD 9', 'GER HD 10']  # Columns to average
    GERClimateDF["GER 3 HD"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                             skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)
    # GER 4
    columns = ['GER HD 11', 'GER HD 12', 'GER HD 13', 'GER HD 14']  # Columns to average
    GERClimateDF["GER 4 HD"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                             skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)
    # GER 5
    columns = ['GER HD 15', 'GER HD 16']  # Columns to average
    GERClimateDF["GER 5 HD"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                             skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)

    # GH RH

    # GER 1
    columns = ['GER RH % 1', 'GER RH % 2']  # Columns to average
    GERClimateDF["GER 1 RH %"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                               skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)
    # GER 2
    columns = ['GER RH % 3', 'GER RH % 4', 'GER RH % 5', 'GER RH % 6']  # Columns to average
    GERClimateDF["GER 2 RH %"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                               skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)
    # GER 3
    columns = ['GER RH % 7', 'GER RH % 8', 'GER RH % 9', 'GER RH % 10']  # Columns to average
    GERClimateDF["GER 3 RH %"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                               skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)
    # GER 4
    columns = ['GER RH % 11', 'GER RH % 12', 'GER RH % 13', 'GER RH % 14']  # Columns to average
    GERClimateDF["GER 4 RH %"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                               skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)
    # GER 5
    columns = ['GER RH % 15', 'GER RH % 16']  # Columns to average
    GERClimateDF["GER 5 RH %"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                               skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)

    # GH Water Temp - Measured

    # GER 1
    columns = ['GER Meas WT 1-1', 'GER Meas WT 1-2', 'GER Meas WT 1-3', 'GER Meas WT 2-1', 'GER Meas WT 2-2',
               'GER Meas WT 2-3']  # Columns to average
    GERClimateDF["GER 1 Meas WT"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                  skipna=True)  # Average columns
    # GER 2
    columns = ['GER Meas WT 3', 'GER Meas WT 4', 'GER Meas WT 5', 'GER Meas WT 6']  # Columns to average
    GERClimateDF["GER 2 Meas WT"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                  skipna=True)  # Average columns
    # GER 3
    columns = ['GER Meas WT 7', 'GER Meas WT 8', 'GER Meas WT 9', 'GER Meas WT 10']  # Columns to average
    GERClimateDF["GER 3 Meas WT"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                  skipna=True)  # Average columns
    # GER 4
    columns = ['GER Meas WT 11-1', 'GER Meas WT 11-2', 'GER Meas WT 12-1', 'GER Meas WT 12-2', 'GER Meas WT 13',
               'GER Meas WT 14']  # Columns to average
    GERClimateDF["GER 4 Meas WT"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                  skipna=True)  # Average columns
    # GER 5
    columns = ['GER Meas WT 15-1', 'GER Meas WT 15-2', 'GER Meas WT 16']  # Columns to average
    GERClimateDF["GER 5 Meas WT"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                  skipna=True)  # Average columnsns)


    # GH Water Temp - Calculated------#############*****************

    # GER 1
    columns = ['GER Calc WT 1-1', 'GER Calc WT 1-2', 'GER Calc WT 1-3', 'GER Calc WT 2-1', 'GER Calc WT 2-2',
               'GER Calc WT 2-3']  # Columns to average
    GERClimateDF["GER 1 Calc WT"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                  skipna=True)  # Average columns
    # GER 2
    columns = ['GER Calc WT 3', 'GER Calc WT 4', 'GER Calc WT 5', 'GER Calc WT 6']  # Columns to average
    GERClimateDF["GER 2 Calc WT"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                  skipna=True)  # Average columns
    # GER 3
    columns = ['GER Calc WT 7', 'GER Calc WT 8', 'GER Calc WT 9', 'GER Calc WT 10']  # Columns to average
    GERClimateDF["GER 3 Calc WT"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                  skipna=True)  # Average columns
    # GER 4
    columns = ['GER Calc WT 11-1', 'GER Calc WT 11-2', 'GER Calc WT 12-1', 'GER Calc WT 12-2', 'GER Calc WT 13',
               'GER Calc WT 14']  # Columns to average
    GERClimateDF["GER 4 Calc WT"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                  skipna=True)  # Average columns
    # GER 5
    columns = ['GER Calc WT 15-1', 'GER Calc WT 15-2', 'GER Calc WT 16']  # Columns to average
    GERClimateDF["GER 5 Calc WT"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                  skipna=True)  # Average columnsns)



    # GH Water Temp - Max - Os INCLUDED

    # GER 1
    columns = ['GER Calc Max WT 1-1', 'GER Calc Max WT 1-2', 'GER Calc Max WT 1-3', 'GER Calc Max WT 2-1',
               'GER Calc Max WT 2-2', 'GER Calc Max WT 2-3']  # Columns to average
    GERClimateDF["GER 1 Max WT"] = GERClimateDF[columns].mean(axis=1, skipna=True)  # Average columns
    # GER 2
    columns = ['GER Calc Max WT 3', 'GER Calc Max WT 4', 'GER Calc Max WT 5', 'GER Calc Max WT 6']  # Columns to average
    GERClimateDF["GER 2 Max WT"] = GERClimateDF[columns].mean(axis=1, skipna=True)  # Average columns
    # GER 3
    columns = ['GER Calc Max WT 7', 'GER Calc Max WT 8', 'GER Calc Max WT 9',
               'GER Calc Max WT 10']  # Columns to average
    GERClimateDF["GER 3 Max WT"] = GERClimateDF[columns].mean(axis=1, skipna=True)  # Average columns
    # GER 4
    columns = ['GER Calc Max WT 11-1', 'GER Calc Max WT 11-2', 'GER Calc Max WT 12-1', 'GER Calc Max WT 12-2',
               'GER Calc Max WT 13', 'GER Calc Max WT 14']  # Columns to average
    GERClimateDF["GER 4 Max WT"] = GERClimateDF[columns].mean(axis=1, skipna=True)  # Average columns
    # GER 5
    columns = ['GER Calc Max WT 15-1', 'GER Calc Max WT 15-2', 'GER Calc Max WT 16']  # Columns to average
    GERClimateDF["GER 5 Max WT"] = GERClimateDF[columns].mean(axis=1, skipna=True)  # Average columns

    # GH Water Temp - Min - Os INCLUDED

    # GER 1
    columns = ['GER Calc Min WT 1-1', 'GER Calc Min WT 1-2', 'GER Calc Min WT 1-3', 'GER Calc Min WT 2-1',
               'GER Calc Min WT 2-2', 'GER Calc Min WT 2-3']  # Columns to average
    GERClimateDF["GER 1 Min WT"] = GERClimateDF[columns].mean(axis=1, skipna=True)  # Average columns
    # GER 2
    columns = ['GER Calc Min WT 3', 'GER Calc Min WT 4', 'GER Calc Min WT 5', 'GER Calc Min WT 6']  # Columns to average
    GERClimateDF["GER 2 Min WT"] = GERClimateDF[columns].mean(axis=1, skipna=True)  # Average columns
    # GER 3
    columns = ['GER Calc Min WT 7', 'GER Calc Min WT 8', 'GER Calc Min WT 9',
               'GER Calc Min WT 10']  # Columns to average
    GERClimateDF["GER 3 Min WT"] = GERClimateDF[columns].mean(axis=1, skipna=True)  # Average columns
    # GER 4
    columns = ['GER Calc Min WT 11-1', 'GER Calc Min WT 11-2', 'GER Calc Min WT 12-1', 'GER Calc Min WT 12-2',
               'GER Calc Min WT 13', 'GER Calc Min WT 14']  # Columns to average
    GERClimateDF["GER 4 Min WT"] = GERClimateDF[columns].mean(axis=1, skipna=True)  # Average columns
    # GER 5
    columns = ['GER Calc Min WT 15-1', 'GER Calc Min WT 15-2', 'GER Calc Min WT 16']  # Columns to average
    GERClimateDF["GER 5 Min WT"] = GERClimateDF[columns].mean(axis=1, skipna=True)  # Average columns

    # Weigh Scales and Moisture

    # GER 1
    columns = ['GER Slab Weight 6']  # Columns to average
    GERClimateDF["GER 1 Weigh Scale"] = GERClimateDF[columns]
    # GER 2
    columns = ['GER Slab Weight 7']  # Columns to average
    GERClimateDF["GER 2 Weigh Scale"] = GERClimateDF[columns]
    # GER 3
    columns = ['GER Slab Weight 8', 'Slab Weight 11']  # Columns to average
    GERClimateDF["GER 3 Weigh Scale"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                      skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)
    # GER 4
    columns = ['GER Slab Weight 9', 'Slab Weight 12']  # Columns to average
    GERClimateDF["GER 4 Weigh Scale"] = GERClimateDF[columns].replace(0, np.NaN).mean(axis=1,
                                                                                      skipna=True)  # Average columns, but replace 0s with nan and ignore (however doesn't change columns)
    # GER 5
    columns = ['GER Slab Weight 10']  # Columns to average
    GERClimateDF["GER 5 Weigh Scale"] = GERClimateDF[columns]

    # Rename Moist slabs %

    # GER 1
    GERClimateDF.rename(columns={'GER Moist Slab % 6': 'GER 1 Moist Slab %'}, inplace=True)
    # GER 2
    GERClimateDF.rename(columns={'GER Moist Slab % 7': 'GER 2 Moist Slab %'}, inplace=True)
    # GER 3
    GERClimateDF.rename(columns={'GER Moist Slab % 8': 'GER 3 Moist Slab %'}, inplace=True)
    # GER 4
    GERClimateDF.rename(columns={'GER Moist Slab % 9': 'GER 4 Moist Slab %'}, inplace=True)
    # GER 5
    GERClimateDF.rename(columns={'GER Moist Slab % 10': 'GER 5 Moist Slab %'}, inplace=True)
    
    # Rename Recycle EC

    # GER 1
    GERClimateDF.rename(columns={'Recycling EC 6': 'GER 1 Recycling EC'}, inplace=True)
    # GER 2
    GERClimateDF.rename(columns={'Recycling EC 7': 'GER 2 Recycling EC'}, inplace=True)
    # GER 3
    GERClimateDF.rename(columns={'Recycling EC 8': 'GER 3 Recycling EC'}, inplace=True)
    # GER 4
    GERClimateDF.rename(columns={'Recycling EC 9': 'GER 4 Recycling EC'}, inplace=True)
    # GER 5
    GERClimateDF.rename(columns={'Recycling EC 10': 'GER 5 Recycling EC'}, inplace=True)



    # ------------------------------------------------------------------------------------------------------------------
    # Create average data File------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------

    # Watts
    # avgdf = GERClimateDF[GERClimateDF['GER Outside Temp'] != 0].groupby('GH 24 Hr Isoweek').agg({'GER Outside Temp': 'mean'})#.rename(columns=d) #Creates sheet

    avgdf = GERClimateDF[GERClimateDF['GER Watts'] != 0].groupby('GH 24 Hr Isoweek').agg({'GER Watts': 'mean'})
    avgdf['GER Max Watts'] = GERClimateDF[GERClimateDF['GER Watts'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER Watts': 'max'})

    # Joules
    time_str = '23:55:00'
    time_object = datetime.strptime(time_str, '%H:%M:%S').time()

    avgdf['GER Joules'] = GERClimateDF[GERClimateDF['Time'] == time_object].groupby('GH 24 Hr Isoweek').agg(
        {'GER Joules': 'sum'})
    avgdf['GER Calculated 24 Hour Temp'] = avgdf['GER Joules'] / 168 * 3.6 / 100 + 17

    # Outside Temp------------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER Outside Temp - 24hr'] = GERClimateDF[GERClimateDF['GER Outside Temp'] != 0].groupby(
        'GH 24 Hr Isoweek').agg({'GER Outside Temp': 'mean'})
    # max
    avgdf['GER Outside Temp - Max'] = GERClimateDF[GERClimateDF['GER Outside Temp'] != 0].groupby(
        'GH 24 Hr Isoweek').agg({'GER Outside Temp': 'max'})
    # min
    avgdf['GER Outside Temp - Min'] = GERClimateDF[GERClimateDF['GER Outside Temp'] != 0].groupby(
        'GH 24 Hr Isoweek').agg({'GER Outside Temp': 'min'})

    # night avg
    avgdf['GER Outside Temp - Night'] = GERClimateDF[
        (GERClimateDF['GER Outside Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg({'GER Outside Temp': 'mean'})
    # night max
    avgdf['GER Outside Temp - Night Max'] = GERClimateDF[
        (GERClimateDF['GER Outside Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg({'GER Outside Temp': 'max'})
    # night min
    avgdf['GER Outside Temp - Night Min'] = GERClimateDF[
        (GERClimateDF['GER Outside Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg({'GER Outside Temp': 'min'})

    # day avg
    avgdf['GER Outside Temp - Day'] = GERClimateDF[
        (GERClimateDF['GER Outside Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER Outside Temp': 'mean'})
    # day max
    avgdf['GER Outside Temp - Day Max'] = GERClimateDF[
        (GERClimateDF['GER Outside Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER Outside Temp': 'max'})
    # day min
    avgdf['GER Outside Temp - Day Min'] = GERClimateDF[
        (GERClimateDF['GER Outside Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER Outside Temp': 'min'})

    # Day Night difference
    avgdf['GER Outside Temp - Day Night Difference'] = avgdf['GER Outside Temp - Day'] - avgdf[
        'GER Outside Temp - Night']

    # Rain--------------------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER Rain - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER Rain': 'mean'})  # don't ignore 0 for this
    # night avg
    avgdf['GER Rain - Night'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Night'].groupby('GH 24 Hr Isoweek').agg(
        {'GER Rain': 'mean'})  # don't ignore 0 for this
    # day avg
    avgdf['GER Rain - Day'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Day'].groupby('GH 24 Hr Isoweek').agg(
        {'GER Rain': 'mean'})  # don't ignore 0 for this

    # Wind Direction----------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER Wind Direction - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER Wind Direction': 'mean'})  # don't ignore 0 for this
    # night avg
    avgdf['GER Wind Direction - Night'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Night'].groupby(
        'GH 24 Hr Isoweek').agg({'GER Wind Direction': 'mean'})  # don't ignore 0 for this
    # day avg
    avgdf['GER Wind Direction - Day'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Day'].groupby(
        'GH 24 Hr Isoweek').agg({'GER Wind Direction': 'mean'})  # don't ignore 0 for this

    # Wind Speed--------------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER Wind Speed - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER Wind Speed': 'mean'})  # don't ignore 0 for this
    # night avg
    avgdf['GER Wind Speed - Night'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Night'].groupby(
        'GH 24 Hr Isoweek').agg({'GER Wind Speed': 'mean'})  # don't ignore 0 for this
    # day avg
    avgdf['GER Wind Speed - Day'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Day'].groupby('GH 24 Hr Isoweek').agg(
        {'GER Wind Speed': 'mean'})  # don't ignore 0 for this

    # Glasshouse Temp GER 1---------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 1 GH Temp - 24hr'] = GERClimateDF[GERClimateDF['GER 1 GH Temp'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 GH Temp': 'mean'})
    # max
    avgdf['GER 1 GH Temp - Max'] = GERClimateDF[GERClimateDF['GER 1 GH Temp'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 GH Temp': 'max'})
    # min
    avgdf['GER 1 GH Temp - Min'] = GERClimateDF[GERClimateDF['GER 1 GH Temp'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 GH Temp': 'min'})

    # night avg
    avgdf['GER 1 GH Temp - Night'] = GERClimateDF[
        (GERClimateDF['GER 1 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 GH Temp': 'mean'})
    # night max
    avgdf['GER 1 GH Temp - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 1 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 GH Temp': 'max'})
    # night min
    avgdf['GER 1 GH Temp - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 1 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 GH Temp': 'min'})

    # day avg
    avgdf['GER 1 GH Temp - Day'] = GERClimateDF[
        (GERClimateDF['GER 1 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 GH Temp': 'mean'})
    # day max
    avgdf['GER 1 GH Temp - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 1 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 GH Temp': 'max'})
    # day min
    avgdf['GER 1 GH Temp - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 1 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 GH Temp': 'min'})

    # Day Night difference
    avgdf['GER 1 GH Temp - Day Night Difference'] = avgdf['GER 1 GH Temp - Day'] - avgdf['GER 1 GH Temp - Night']

    # Var from Calculated
    avgdf['GER 1 GH Temp - Variation From Calculated'] = avgdf['GER 1 GH Temp - 24hr'] - avgdf[
        'GER Calculated 24 Hour Temp']

    # Glasshouse Temp GER 2---------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 2 GH Temp - 24hr'] = GERClimateDF[GERClimateDF['GER 2 GH Temp'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 GH Temp': 'mean'})
    # max
    avgdf['GER 2 GH Temp - Max'] = GERClimateDF[GERClimateDF['GER 2 GH Temp'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 GH Temp': 'max'})
    # min
    avgdf['GER 2 GH Temp - Min'] = GERClimateDF[GERClimateDF['GER 2 GH Temp'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 GH Temp': 'min'})

    # night avg
    avgdf['GER 2 GH Temp - Night'] = GERClimateDF[
        (GERClimateDF['GER 2 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 GH Temp': 'mean'})
    # night max
    avgdf['GER 2 GH Temp - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 2 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 GH Temp': 'max'})
    # night min
    avgdf['GER 2 GH Temp - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 2 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 GH Temp': 'min'})

    # day avg
    avgdf['GER 2 GH Temp - Day'] = GERClimateDF[
        (GERClimateDF['GER 2 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 GH Temp': 'mean'})
    # day max
    avgdf['GER 2 GH Temp - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 2 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 GH Temp': 'max'})
    # day min
    avgdf['GER 2 GH Temp - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 2 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 GH Temp': 'min'})

    # Day Night difference
    avgdf['GER 2 GH Temp - Day Night Difference'] = avgdf['GER 2 GH Temp - Day'] - avgdf['GER 2 GH Temp - Night']

    # Var from Calculated
    avgdf['GER 2 GH Temp - Variation From Calculated'] = avgdf['GER 2 GH Temp - 24hr'] - avgdf[
        'GER Calculated 24 Hour Temp']

    # Glasshouse Temp GER 3---------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 3 GH Temp - 24hr'] = GERClimateDF[GERClimateDF['GER 3 GH Temp'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 GH Temp': 'mean'})
    # max
    avgdf['GER 3 GH Temp - Max'] = GERClimateDF[GERClimateDF['GER 3 GH Temp'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 GH Temp': 'max'})
    # min
    avgdf['GER 3 GH Temp - Min'] = GERClimateDF[GERClimateDF['GER 3 GH Temp'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 GH Temp': 'min'})

    # night avg
    avgdf['GER 3 GH Temp - Night'] = GERClimateDF[
        (GERClimateDF['GER 3 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 GH Temp': 'mean'})
    # night max
    avgdf['GER 3 GH Temp - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 3 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 GH Temp': 'max'})
    # night min
    avgdf['GER 3 GH Temp - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 3 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 GH Temp': 'min'})

    # day avg
    avgdf['GER 3 GH Temp - Day'] = GERClimateDF[
        (GERClimateDF['GER 3 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 GH Temp': 'mean'})
    # day max
    avgdf['GER 3 GH Temp - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 3 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 GH Temp': 'max'})
    # day min
    avgdf['GER 3 GH Temp - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 3 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 GH Temp': 'min'})

    # Day Night difference
    avgdf['GER 3 GH Temp - Day Night Difference'] = avgdf['GER 3 GH Temp - Day'] - avgdf['GER 3 GH Temp - Night']

    # Var from Calculated
    avgdf['GER 3 GH Temp - Variation From Calculated'] = avgdf['GER 3 GH Temp - 24hr'] - avgdf[
        'GER Calculated 24 Hour Temp']

    # Glasshouse Temp GER 4---------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 4 GH Temp - 24hr'] = GERClimateDF[GERClimateDF['GER 4 GH Temp'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 GH Temp': 'mean'})
    # max
    avgdf['GER 4 GH Temp - Max'] = GERClimateDF[GERClimateDF['GER 4 GH Temp'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 GH Temp': 'max'})
    # min
    avgdf['GER 4 GH Temp - Min'] = GERClimateDF[GERClimateDF['GER 4 GH Temp'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 GH Temp': 'min'})

    # night avg
    avgdf['GER 4 GH Temp - Night'] = GERClimateDF[
        (GERClimateDF['GER 4 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 GH Temp': 'mean'})
    # night max
    avgdf['GER 4 GH Temp - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 4 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 GH Temp': 'max'})
    # night min
    avgdf['GER 4 GH Temp - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 4 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 GH Temp': 'min'})

    # day avg
    avgdf['GER 4 GH Temp - Day'] = GERClimateDF[
        (GERClimateDF['GER 4 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 GH Temp': 'mean'})
    # day max
    avgdf['GER 4 GH Temp - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 4 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 GH Temp': 'max'})
    # day min
    avgdf['GER 4 GH Temp - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 4 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 GH Temp': 'min'})

    # Day Night difference
    avgdf['GER 4 GH Temp - Day Night Difference'] = avgdf['GER 4 GH Temp - Day'] - avgdf['GER 4 GH Temp - Night']

    # Var from Calculated
    avgdf['GER 4 GH Temp - Variation From Calculated'] = avgdf['GER 4 GH Temp - 24hr'] - avgdf[
        'GER Calculated 24 Hour Temp']

    # Glasshouse Temp GER 5---------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 5 GH Temp - 24hr'] = GERClimateDF[GERClimateDF['GER 5 GH Temp'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 GH Temp': 'mean'})
    # max
    avgdf['GER 5 GH Temp - Max'] = GERClimateDF[GERClimateDF['GER 5 GH Temp'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 GH Temp': 'max'})
    # min
    avgdf['GER 5 GH Temp - Min'] = GERClimateDF[GERClimateDF['GER 5 GH Temp'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 GH Temp': 'min'})

    # night avg
    avgdf['GER 5 GH Temp - Night'] = GERClimateDF[
        (GERClimateDF['GER 5 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 GH Temp': 'mean'})
    # night max
    avgdf['GER 5 GH Temp - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 5 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 GH Temp': 'max'})
    # night min
    avgdf['GER 5 GH Temp - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 5 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 GH Temp': 'min'})

    # day avg
    avgdf['GER 5 GH Temp - Day'] = GERClimateDF[
        (GERClimateDF['GER 5 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 GH Temp': 'mean'})
    # day max
    avgdf['GER 5 GH Temp - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 5 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 GH Temp': 'max'})
    # day min
    avgdf['GER 5 GH Temp - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 5 GH Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 GH Temp': 'min'})

    # Day Night difference
    avgdf['GER 5 GH Temp - Day Night Difference'] = avgdf['GER 5 GH Temp - Day'] - avgdf['GER 5 GH Temp - Night']

    # Var from Calculated
    avgdf['GER 5 GH Temp - Variation From Calculated'] = avgdf['GER 3 GH Temp - 24hr'] - avgdf[
        'GER Calculated 24 Hour Temp']

    # RH GER 1----------------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 1 RH % - 24hr'] = GERClimateDF[GERClimateDF['GER 1 RH %'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 RH %': 'mean'})
    # max
    avgdf['GER 1 RH % - Max'] = GERClimateDF[GERClimateDF['GER 1 RH %'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 RH %': 'max'})
    # min
    avgdf['GER 1 RH % - Min'] = GERClimateDF[GERClimateDF['GER 1 RH %'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 RH %': 'min'})

    # night avg
    avgdf['GER 1 RH % - Night'] = GERClimateDF[
        (GERClimateDF['GER 1 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 RH %': 'mean'})
    # night max
    avgdf['GER 1 RH % - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 1 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 RH %': 'max'})
    # night min
    avgdf['GER 1 RH % - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 1 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 RH %': 'min'})

    # day avg
    avgdf['GER 1 RH % - Day'] = GERClimateDF[
        (GERClimateDF['GER 1 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 RH %': 'mean'})
    # day max
    avgdf['GER 1 RH % - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 1 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 RH %': 'max'})
    # day min
    avgdf['GER 1 RH % - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 1 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 RH %': 'min'})

    # Day Night difference
    avgdf['GER 1 RH % - Day Night Difference'] = avgdf['GER 1 RH % - Day'] - avgdf['GER 1 RH % - Night']

    # RH GER 2----------------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 2 RH % - 24hr'] = GERClimateDF[GERClimateDF['GER 2 RH %'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 RH %': 'mean'})
    # max
    avgdf['GER 2 RH % - Max'] = GERClimateDF[GERClimateDF['GER 2 RH %'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 RH %': 'max'})
    # min
    avgdf['GER 2 RH % - Min'] = GERClimateDF[GERClimateDF['GER 2 RH %'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 RH %': 'min'})

    # night avg
    avgdf['GER 2 RH % - Night'] = GERClimateDF[
        (GERClimateDF['GER 2 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 RH %': 'mean'})
    # night max
    avgdf['GER 2 RH % - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 2 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 RH %': 'max'})
    # night min
    avgdf['GER 2 RH % - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 2 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 RH %': 'min'})

    # day avg
    avgdf['GER 2 RH % - Day'] = GERClimateDF[
        (GERClimateDF['GER 2 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 RH %': 'mean'})
    # day max
    avgdf['GER 2 RH % - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 2 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 RH %': 'max'})
    # day min
    avgdf['GER 2 RH % - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 2 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 RH %': 'min'})

    # Day Night difference
    avgdf['GER 2 RH % - Day Night Difference'] = avgdf['GER 2 RH % - Day'] - avgdf['GER 2 RH % - Night']

    # RH GER 3----------------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 3 RH % - 24hr'] = GERClimateDF[GERClimateDF['GER 3 RH %'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 RH %': 'mean'})
    # max
    avgdf['GER 3 RH % - Max'] = GERClimateDF[GERClimateDF['GER 3 RH %'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 RH %': 'max'})
    # min
    avgdf['GER 3 RH % - Min'] = GERClimateDF[GERClimateDF['GER 3 RH %'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 RH %': 'min'})

    # night avg
    avgdf['GER 3 RH % - Night'] = GERClimateDF[
        (GERClimateDF['GER 3 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 RH %': 'mean'})
    # night max
    avgdf['GER 3 RH % - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 3 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 RH %': 'max'})
    # night min
    avgdf['GER 3 RH % - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 3 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 RH %': 'min'})

    # day avg
    avgdf['GER 3 RH % - Day'] = GERClimateDF[
        (GERClimateDF['GER 3 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 RH %': 'mean'})
    # day max
    avgdf['GER 3 RH % - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 3 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 RH %': 'max'})
    # day min
    avgdf['GER 3 RH % - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 3 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 RH %': 'min'})

    # Day Night difference
    avgdf['GER 3 RH % - Day Night Difference'] = avgdf['GER 3 RH % - Day'] - avgdf['GER 3 RH % - Night']

    # RH GER 4----------------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 4 RH % - 24hr'] = GERClimateDF[GERClimateDF['GER 4 RH %'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 RH %': 'mean'})
    # max
    avgdf['GER 4 RH % - Max'] = GERClimateDF[GERClimateDF['GER 4 RH %'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 RH %': 'max'})
    # min
    avgdf['GER 4 RH % - Min'] = GERClimateDF[GERClimateDF['GER 4 RH %'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 RH %': 'min'})

    # night avg
    avgdf['GER 4 RH % - Night'] = GERClimateDF[
        (GERClimateDF['GER 4 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 RH %': 'mean'})
    # night max
    avgdf['GER 4 RH % - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 4 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 RH %': 'max'})
    # night min
    avgdf['GER 4 RH % - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 4 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 RH %': 'min'})

    # day avg
    avgdf['GER 4 RH % - Day'] = GERClimateDF[
        (GERClimateDF['GER 4 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 RH %': 'mean'})
    # day max
    avgdf['GER 4 RH % - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 4 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 RH %': 'max'})
    # day min
    avgdf['GER 4 RH % - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 4 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 RH %': 'min'})

    # Day Night difference
    avgdf['GER 4 RH % - Day Night Difference'] = avgdf['GER 4 RH % - Day'] - avgdf['GER 4 RH % - Night']

    # RH GER 5----------------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 5 RH % - 24hr'] = GERClimateDF[GERClimateDF['GER 5 RH %'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 RH %': 'mean'})
    # max
    avgdf['GER 5 RH % - Max'] = GERClimateDF[GERClimateDF['GER 5 RH %'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 RH %': 'max'})
    # min
    avgdf['GER 5 RH % - Min'] = GERClimateDF[GERClimateDF['GER 5 RH %'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 RH %': 'min'})

    # night avg
    avgdf['GER 5 RH % - Night'] = GERClimateDF[
        (GERClimateDF['GER 5 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 RH %': 'mean'})
    # night max
    avgdf['GER 5 RH % - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 5 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 RH %': 'max'})
    # night min
    avgdf['GER 5 RH % - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 5 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 RH %': 'min'})

    # day avg
    avgdf['GER 5 RH % - Day'] = GERClimateDF[
        (GERClimateDF['GER 5 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 RH %': 'mean'})
    # day max
    avgdf['GER 5 RH % - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 5 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 RH %': 'max'})
    # day min
    avgdf['GER 5 RH % - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 5 RH %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 RH %': 'min'})

    # Day Night difference
    avgdf['GER 5 RH % - Day Night Difference'] = avgdf['GER 5 RH % - Day'] - avgdf['GER 5 RH % - Night']

    # HD GER 1----------------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 1 HD - 24hr'] = GERClimateDF[GERClimateDF['GER 1 HD'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 HD': 'mean'})
    # max
    avgdf['GER 1 HD - Max'] = GERClimateDF[GERClimateDF['GER 1 HD'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 HD': 'max'})
    # min
    avgdf['GER 1 HD - Min'] = GERClimateDF[GERClimateDF['GER 1 HD'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 HD': 'min'})

    # night avg
    avgdf['GER 1 HD - Night'] = GERClimateDF[
        (GERClimateDF['GER 1 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 HD': 'mean'})
    # night max
    avgdf['GER 1 HD - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 1 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 HD': 'max'})
    # night min
    avgdf['GER 1 HD - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 1 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 HD': 'min'})

    # day avg
    avgdf['GER 1 HD - Day'] = GERClimateDF[
        (GERClimateDF['GER 1 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 HD': 'mean'})
    # day max
    avgdf['GER 1 HD - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 1 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 HD': 'max'})
    # day min
    avgdf['GER 1 HD - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 1 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 HD': 'min'})

    # Day Night difference
    avgdf['GER 1 HD - Day Night Difference'] = avgdf['GER 1 HD - Day'] - avgdf['GER 1 HD - Night']

    # HD GER 2----------------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 2 HD - 24hr'] = GERClimateDF[GERClimateDF['GER 2 HD'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 HD': 'mean'})
    # max
    avgdf['GER 2 HD - Max'] = GERClimateDF[GERClimateDF['GER 2 HD'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 HD': 'max'})
    # min
    avgdf['GER 2 HD - Min'] = GERClimateDF[GERClimateDF['GER 2 HD'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 HD': 'min'})

    # night avg
    avgdf['GER 2 HD - Night'] = GERClimateDF[
        (GERClimateDF['GER 2 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 HD': 'mean'})
    # night max
    avgdf['GER 2 HD - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 2 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 HD': 'max'})
    # night min
    avgdf['GER 2 HD - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 2 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 HD': 'min'})

    # day avg
    avgdf['GER 2 HD - Day'] = GERClimateDF[
        (GERClimateDF['GER 2 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 HD': 'mean'})
    # day max
    avgdf['GER 2 HD - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 2 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 HD': 'max'})
    # day min
    avgdf['GER 2 HD - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 2 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 HD': 'min'})

    # Day Night difference
    avgdf['GER 2 HD - Day Night Difference'] = avgdf['GER 2 HD - Day'] - avgdf['GER 2 HD - Night']

    # HD GER 3----------------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 3 HD - 24hr'] = GERClimateDF[GERClimateDF['GER 3 HD'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 HD': 'mean'})
    # max
    avgdf['GER 3 HD - Max'] = GERClimateDF[GERClimateDF['GER 3 HD'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 HD': 'max'})
    # min
    avgdf['GER 3 HD - Min'] = GERClimateDF[GERClimateDF['GER 3 HD'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 HD': 'min'})

    # night avg
    avgdf['GER 3 HD - Night'] = GERClimateDF[
        (GERClimateDF['GER 3 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 HD': 'mean'})
    # night max
    avgdf['GER 3 HD - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 3 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 HD': 'max'})
    # night min
    avgdf['GER 3 HD - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 3 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 HD': 'min'})

    # day avg
    avgdf['GER 3 HD - Day'] = GERClimateDF[
        (GERClimateDF['GER 3 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 HD': 'mean'})
    # day max
    avgdf['GER 3 HD - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 3 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 HD': 'max'})
    # day min
    avgdf['GER 3 HD - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 3 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 HD': 'min'})

    # Day Night difference
    avgdf['GER 3 HD - Day Night Difference'] = avgdf['GER 3 HD - Day'] - avgdf['GER 3 HD - Night']

    # HD GER 4----------------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 4 HD - 24hr'] = GERClimateDF[GERClimateDF['GER 4 HD'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 HD': 'mean'})
    # max
    avgdf['GER 4 HD - Max'] = GERClimateDF[GERClimateDF['GER 4 HD'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 HD': 'max'})
    # min
    avgdf['GER 4 HD - Min'] = GERClimateDF[GERClimateDF['GER 4 HD'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 HD': 'min'})

    # night avg
    avgdf['GER 4 HD - Night'] = GERClimateDF[
        (GERClimateDF['GER 4 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 HD': 'mean'})
    # night max
    avgdf['GER 4 HD - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 4 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 HD': 'max'})
    # night min
    avgdf['GER 4 HD - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 4 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 HD': 'min'})

    # day avg
    avgdf['GER 4 HD - Day'] = GERClimateDF[
        (GERClimateDF['GER 4 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 HD': 'mean'})
    # day max
    avgdf['GER 4 HD - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 4 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 HD': 'max'})
    # day min
    avgdf['GER 4 HD - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 4 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 HD': 'min'})

    # Day Night difference
    avgdf['GER 4 HD - Day Night Difference'] = avgdf['GER 4 HD - Day'] - avgdf['GER 4 HD - Night']

    # HD GER 5----------------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 5 HD - 24hr'] = GERClimateDF[GERClimateDF['GER 5 HD'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 HD': 'mean'})
    # max
    avgdf['GER 5 HD - Max'] = GERClimateDF[GERClimateDF['GER 5 HD'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 HD': 'max'})
    # min
    avgdf['GER 5 HD - Min'] = GERClimateDF[GERClimateDF['GER 5 HD'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 HD': 'min'})

    # night avg
    avgdf['GER 5 HD - Night'] = GERClimateDF[
        (GERClimateDF['GER 5 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 HD': 'mean'})
    # night max
    avgdf['GER 5 HD - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 5 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 HD': 'max'})
    # night min
    avgdf['GER 5 HD - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 5 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 HD': 'min'})

    # day avg
    avgdf['GER 5 HD - Day'] = GERClimateDF[
        (GERClimateDF['GER 5 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 HD': 'mean'})
    # day max
    avgdf['GER 5 HD - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 5 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 HD': 'max'})
    # day min
    avgdf['GER 5 HD - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 5 HD'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 HD': 'min'})

    # Day Night difference
    avgdf['GER 5 HD - Day Night Difference'] = avgdf['GER 5 HD - Day'] - avgdf['GER 5 HD - Night']

    # C02 GER 1---------------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 1 C02 PPM - 24hr'] = GERClimateDF[GERClimateDF['GER 1 C02 PPM'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 C02 PPM': 'mean'})
    # max
    avgdf['GER 1 C02 PPM - Max'] = GERClimateDF[GERClimateDF['GER 1 C02 PPM'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 C02 PPM': 'max'})
    # min
    avgdf['GER 1 C02 PPM - Min'] = GERClimateDF[GERClimateDF['GER 1 C02 PPM'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 C02 PPM': 'min'})

    # night avg
    avgdf['GER 1 C02 PPM - Night'] = GERClimateDF[
        (GERClimateDF['GER 1 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 C02 PPM': 'mean'})
    # night max
    avgdf['GER 1 C02 PPM - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 1 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 C02 PPM': 'max'})
    # night min
    avgdf['GER 1 C02 PPM - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 1 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 C02 PPM': 'min'})

    # day avg
    avgdf['GER 1 C02 PPM - Day'] = GERClimateDF[
        (GERClimateDF['GER 1 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 C02 PPM': 'mean'})
    # day max
    avgdf['GER 1 C02 PPM - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 1 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 C02 PPM': 'max'})
    # day min
    avgdf['GER 1 C02 PPM - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 1 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 C02 PPM': 'min'})

    # Day Night difference
    avgdf['GER 1 C02 PPM - Day Night Difference'] = avgdf['GER 1 C02 PPM - Day'] - avgdf['GER 1 C02 PPM - Night']

    # C02 GER 2---------------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 2 C02 PPM - 24hr'] = GERClimateDF[GERClimateDF['GER 2 C02 PPM'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 C02 PPM': 'mean'})
    # max
    avgdf['GER 2 C02 PPM - Max'] = GERClimateDF[GERClimateDF['GER 2 C02 PPM'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 C02 PPM': 'max'})
    # min
    avgdf['GER 2 C02 PPM - Min'] = GERClimateDF[GERClimateDF['GER 2 C02 PPM'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 C02 PPM': 'min'})

    # night avg
    avgdf['GER 2 C02 PPM - Night'] = GERClimateDF[
        (GERClimateDF['GER 2 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 C02 PPM': 'mean'})
    # night max
    avgdf['GER 2 C02 PPM - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 2 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 C02 PPM': 'max'})
    # night min
    avgdf['GER 2 C02 PPM - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 2 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 C02 PPM': 'min'})

    # day avg
    avgdf['GER 2 C02 PPM - Day'] = GERClimateDF[
        (GERClimateDF['GER 2 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 C02 PPM': 'mean'})
    # day max
    avgdf['GER 2 C02 PPM - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 2 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 C02 PPM': 'max'})
    # day min
    avgdf['GER 2 C02 PPM - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 2 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 C02 PPM': 'min'})

    # Day Night difference
    avgdf['GER 2 C02 PPM - Day Night Difference'] = avgdf['GER 2 C02 PPM - Day'] - avgdf['GER 2 C02 PPM - Night']

    # C02 GER 3---------------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 3 C02 PPM - 24hr'] = GERClimateDF[GERClimateDF['GER 3 C02 PPM'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 C02 PPM': 'mean'})
    # max
    avgdf['GER 3 C02 PPM - Max'] = GERClimateDF[GERClimateDF['GER 3 C02 PPM'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 C02 PPM': 'max'})
    # min
    avgdf['GER 3 C02 PPM - Min'] = GERClimateDF[GERClimateDF['GER 3 C02 PPM'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 C02 PPM': 'min'})

    # night avg
    avgdf['GER 3 C02 PPM - Night'] = GERClimateDF[
        (GERClimateDF['GER 3 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 C02 PPM': 'mean'})
    # night max
    avgdf['GER 3 C02 PPM - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 3 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 C02 PPM': 'max'})
    # night min
    avgdf['GER 3 C02 PPM - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 3 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 C02 PPM': 'min'})

    # day avg
    avgdf['GER 3 C02 PPM - Day'] = GERClimateDF[
        (GERClimateDF['GER 3 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 C02 PPM': 'mean'})
    # day max
    avgdf['GER 3 C02 PPM - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 3 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 C02 PPM': 'max'})
    # day min
    avgdf['GER 3 C02 PPM - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 3 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 C02 PPM': 'min'})

    # Day Night difference
    avgdf['GER 3 C02 PPM - Day Night Difference'] = avgdf['GER 3 C02 PPM - Day'] - avgdf['GER 3 C02 PPM - Night']

    # C02 GER 4---------------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 4 C02 PPM - 24hr'] = GERClimateDF[GERClimateDF['GER 4 C02 PPM'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 C02 PPM': 'mean'})
    # max
    avgdf['GER 4 C02 PPM - Max'] = GERClimateDF[GERClimateDF['GER 4 C02 PPM'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 C02 PPM': 'max'})
    # min
    avgdf['GER 4 C02 PPM - Min'] = GERClimateDF[GERClimateDF['GER 4 C02 PPM'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 C02 PPM': 'min'})

    # night avg
    avgdf['GER 4 C02 PPM - Night'] = GERClimateDF[
        (GERClimateDF['GER 4 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 C02 PPM': 'mean'})
    # night max
    avgdf['GER 4 C02 PPM - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 4 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 C02 PPM': 'max'})
    # night min
    avgdf['GER 4 C02 PPM - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 4 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 C02 PPM': 'min'})

    # day avg
    avgdf['GER 4 C02 PPM - Day'] = GERClimateDF[
        (GERClimateDF['GER 4 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 C02 PPM': 'mean'})
    # day max
    avgdf['GER 4 C02 PPM - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 4 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 C02 PPM': 'max'})
    # day min
    avgdf['GER 4 C02 PPM - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 4 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 C02 PPM': 'min'})

    # Day Night difference
    avgdf['GER 4 C02 PPM - Day Night Difference'] = avgdf['GER 4 C02 PPM - Day'] - avgdf['GER 4 C02 PPM - Night']

    # C02 GER 5---------------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 5 C02 PPM - 24hr'] = GERClimateDF[GERClimateDF['GER 5 C02 PPM'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 C02 PPM': 'mean'})
    # max
    avgdf['GER 5 C02 PPM - Max'] = GERClimateDF[GERClimateDF['GER 5 C02 PPM'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 C02 PPM': 'max'})
    # min
    avgdf['GER 5 C02 PPM - Min'] = GERClimateDF[GERClimateDF['GER 5 C02 PPM'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 C02 PPM': 'min'})

    # night avg
    avgdf['GER 5 C02 PPM - Night'] = GERClimateDF[
        (GERClimateDF['GER 5 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 C02 PPM': 'mean'})
    # night max
    avgdf['GER 5 C02 PPM - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 5 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 C02 PPM': 'max'})
    # night min
    avgdf['GER 5 C02 PPM - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 5 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 C02 PPM': 'min'})

    # day avg
    avgdf['GER 5 C02 PPM - Day'] = GERClimateDF[
        (GERClimateDF['GER 5 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 C02 PPM': 'mean'})
    # day max
    avgdf['GER 5 C02 PPM - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 5 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 C02 PPM': 'max'})
    # day min
    avgdf['GER 5 C02 PPM - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 5 C02 PPM'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 C02 PPM': 'min'})

    # Day Night difference
    avgdf['GER 5 C02 PPM - Day Night Difference'] = avgdf['GER 5 C02 PPM - Day'] - avgdf['GER 5 C02 PPM - Night']

    # Calc Heat Temp GER 1----------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 1 Calc Heat Temp - 24hr'] = GERClimateDF[GERClimateDF['GER 1 Calc Heat Temp'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Calc Heat Temp': 'mean'})
    # max
    avgdf['GER 1 Calc Heat Temp - Max'] = GERClimateDF[GERClimateDF['GER 1 Calc Heat Temp'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Calc Heat Temp': 'max'})
    # min
    avgdf['GER 1 Calc Heat Temp - Min'] = GERClimateDF[GERClimateDF['GER 1 Calc Heat Temp'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Calc Heat Temp': 'min'})

    # night avg
    avgdf['GER 1 Calc Heat Temp - Night'] = GERClimateDF[
        (GERClimateDF['GER 1 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Calc Heat Temp': 'mean'})
    # night max
    avgdf['GER 1 Calc Heat Temp - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 1 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Calc Heat Temp': 'max'})
    # night min
    avgdf['GER 1 Calc Heat Temp - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 1 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Calc Heat Temp': 'min'})

    # day avg
    avgdf['GER 1 Calc Heat Temp - Day'] = GERClimateDF[
        (GERClimateDF['GER 1 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Calc Heat Temp': 'mean'})
    # day max
    avgdf['GER 1 Calc Heat Temp - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 1 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Calc Heat Temp': 'max'})
    # day min
    avgdf['GER 1 Calc Heat Temp - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 1 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Calc Heat Temp': 'min'})

    # Day Night difference
    avgdf['GER 1 Calc Heat Temp - Day Night Difference'] = avgdf['GER 1 Calc Heat Temp - Day'] - avgdf[
        'GER 1 Calc Heat Temp - Night']

    # Calc Heat Temp GER 2----------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 2 Calc Heat Temp - 24hr'] = GERClimateDF[GERClimateDF['GER 2 Calc Heat Temp'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Calc Heat Temp': 'mean'})
    # max
    avgdf['GER 2 Calc Heat Temp - Max'] = GERClimateDF[GERClimateDF['GER 2 Calc Heat Temp'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Calc Heat Temp': 'max'})
    # min
    avgdf['GER 2 Calc Heat Temp - Min'] = GERClimateDF[GERClimateDF['GER 2 Calc Heat Temp'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Calc Heat Temp': 'min'})

    # night avg
    avgdf['GER 2 Calc Heat Temp - Night'] = GERClimateDF[
        (GERClimateDF['GER 2 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Calc Heat Temp': 'mean'})
    # night max
    avgdf['GER 2 Calc Heat Temp - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 2 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Calc Heat Temp': 'max'})
    # night min
    avgdf['GER 2 Calc Heat Temp - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 2 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Calc Heat Temp': 'min'})

    # day avg
    avgdf['GER 2 Calc Heat Temp - Day'] = GERClimateDF[
        (GERClimateDF['GER 2 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Calc Heat Temp': 'mean'})
    # day max
    avgdf['GER 2 Calc Heat Temp - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 2 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Calc Heat Temp': 'max'})
    # day min
    avgdf['GER 2 Calc Heat Temp - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 2 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Calc Heat Temp': 'min'})

    # Day Night difference
    avgdf['GER 2 Calc Heat Temp - Day Night Difference'] = avgdf['GER 2 Calc Heat Temp - Day'] - avgdf[
        'GER 2 Calc Heat Temp - Night']

    # Calc Heat Temp GER 3----------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 3 Calc Heat Temp - 24hr'] = GERClimateDF[GERClimateDF['GER 3 Calc Heat Temp'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Calc Heat Temp': 'mean'})
    # max
    avgdf['GER 3 Calc Heat Temp - Max'] = GERClimateDF[GERClimateDF['GER 3 Calc Heat Temp'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Calc Heat Temp': 'max'})
    # min
    avgdf['GER 3 Calc Heat Temp - Min'] = GERClimateDF[GERClimateDF['GER 3 Calc Heat Temp'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Calc Heat Temp': 'min'})

    # night avg
    avgdf['GER 3 Calc Heat Temp - Night'] = GERClimateDF[
        (GERClimateDF['GER 3 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Calc Heat Temp': 'mean'})
    # night max
    avgdf['GER 3 Calc Heat Temp - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 3 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Calc Heat Temp': 'max'})
    # night min
    avgdf['GER 3 Calc Heat Temp - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 3 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Calc Heat Temp': 'min'})

    # day avg
    avgdf['GER 3 Calc Heat Temp - Day'] = GERClimateDF[
        (GERClimateDF['GER 3 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Calc Heat Temp': 'mean'})
    # day max
    avgdf['GER 3 Calc Heat Temp - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 3 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Calc Heat Temp': 'max'})
    # day min
    avgdf['GER 3 Calc Heat Temp - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 3 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Calc Heat Temp': 'min'})

    # Day Night difference
    avgdf['GER 3 Calc Heat Temp - Day Night Difference'] = avgdf['GER 3 Calc Heat Temp - Day'] - avgdf[
        'GER 3 Calc Heat Temp - Night']

    # Calc Heat Temp GER 4----------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 4 Calc Heat Temp - 24hr'] = GERClimateDF[GERClimateDF['GER 4 Calc Heat Temp'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Calc Heat Temp': 'mean'})
    # max
    avgdf['GER 4 Calc Heat Temp - Max'] = GERClimateDF[GERClimateDF['GER 4 Calc Heat Temp'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Calc Heat Temp': 'max'})
    # min
    avgdf['GER 4 Calc Heat Temp - Min'] = GERClimateDF[GERClimateDF['GER 4 Calc Heat Temp'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Calc Heat Temp': 'min'})

    # night avg
    avgdf['GER 4 Calc Heat Temp - Night'] = GERClimateDF[
        (GERClimateDF['GER 4 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Calc Heat Temp': 'mean'})
    # night max
    avgdf['GER 4 Calc Heat Temp - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 4 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Calc Heat Temp': 'max'})
    # night min
    avgdf['GER 4 Calc Heat Temp - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 4 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Calc Heat Temp': 'min'})

    # day avg
    avgdf['GER 4 Calc Heat Temp - Day'] = GERClimateDF[
        (GERClimateDF['GER 4 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Calc Heat Temp': 'mean'})
    # day max
    avgdf['GER 4 Calc Heat Temp - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 4 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Calc Heat Temp': 'max'})
    # day min
    avgdf['GER 4 Calc Heat Temp - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 4 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Calc Heat Temp': 'min'})

    # Day Night difference
    avgdf['GER 4 Calc Heat Temp - Day Night Difference'] = avgdf['GER 4 Calc Heat Temp - Day'] - avgdf[
        'GER 4 Calc Heat Temp - Night']

    # Calc Heat Temp GER 5----------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 5 Calc Heat Temp - 24hr'] = GERClimateDF[GERClimateDF['GER 5 Calc Heat Temp'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Calc Heat Temp': 'mean'})
    # max
    avgdf['GER 5 Calc Heat Temp - Max'] = GERClimateDF[GERClimateDF['GER 5 Calc Heat Temp'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Calc Heat Temp': 'max'})
    # min
    avgdf['GER 5 Calc Heat Temp - Min'] = GERClimateDF[GERClimateDF['GER 5 Calc Heat Temp'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Calc Heat Temp': 'min'})

    # night avg
    avgdf['GER 5 Calc Heat Temp - Night'] = GERClimateDF[
        (GERClimateDF['GER 5 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Calc Heat Temp': 'mean'})
    # night max
    avgdf['GER 5 Calc Heat Temp - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 5 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Calc Heat Temp': 'max'})
    # night min
    avgdf['GER 5 Calc Heat Temp - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 5 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Calc Heat Temp': 'min'})

    # day avg
    avgdf['GER 5 Calc Heat Temp - Day'] = GERClimateDF[
        (GERClimateDF['GER 5 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Calc Heat Temp': 'mean'})
    # day max
    avgdf['GER 5 Calc Heat Temp - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 5 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Calc Heat Temp': 'max'})
    # day min
    avgdf['GER 5 Calc Heat Temp - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 5 Calc Heat Temp'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Calc Heat Temp': 'min'})

    # Day Night difference
    avgdf['GER 5 Calc Heat Temp - Day Night Difference'] = avgdf['GER 5 Calc Heat Temp - Day'] - avgdf[
        'GER 5 Calc Heat Temp - Night']

    # Pipe WT Measured GER 1--------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 1 Meas WT - 24hr'] = GERClimateDF[GERClimateDF['GER 1 Meas WT'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 Meas WT': 'mean'})
    # max
    avgdf['GER 1 Meas WT - Max'] = GERClimateDF[GERClimateDF['GER 1 Meas WT'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 Meas WT': 'max'})
    # min
    avgdf['GER 1 Meas WT - Min'] = GERClimateDF[GERClimateDF['GER 1 Meas WT'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 Meas WT': 'min'})

    # night avg
    avgdf['GER 1 Meas WT - Night'] = GERClimateDF[
        (GERClimateDF['GER 1 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 Meas WT': 'mean'})
    # night max
    avgdf['GER 1 Meas WT - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 1 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 Meas WT': 'max'})
    # night min
    avgdf['GER 1 Meas WT - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 1 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 Meas WT': 'min'})

    # day avg
    avgdf['GER 1 Meas WT - Day'] = GERClimateDF[
        (GERClimateDF['GER 1 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 Meas WT': 'mean'})
    # day max
    avgdf['GER 1 Meas WT - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 1 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 Meas WT': 'max'})
    # day min
    avgdf['GER 1 Meas WT - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 1 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 Meas WT': 'min'})

    # Day Night difference
    avgdf['GER 1 Meas WT - Day Night Difference'] = avgdf['GER 1 Meas WT - Day'] - avgdf['GER 1 Meas WT - Night']

    # Pipe WT Measured GER 2--------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 2 Meas WT - 24hr'] = GERClimateDF[GERClimateDF['GER 2 Meas WT'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 Meas WT': 'mean'})
    # max
    avgdf['GER 2 Meas WT - Max'] = GERClimateDF[GERClimateDF['GER 2 Meas WT'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 Meas WT': 'max'})
    # min
    avgdf['GER 2 Meas WT - Min'] = GERClimateDF[GERClimateDF['GER 2 Meas WT'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 Meas WT': 'min'})

    # night avg
    avgdf['GER 2 Meas WT - Night'] = GERClimateDF[
        (GERClimateDF['GER 2 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 Meas WT': 'mean'})
    # night max
    avgdf['GER 2 Meas WT - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 2 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 Meas WT': 'max'})
    # night min
    avgdf['GER 2 Meas WT - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 2 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 Meas WT': 'min'})

    # day avg
    avgdf['GER 2 Meas WT - Day'] = GERClimateDF[
        (GERClimateDF['GER 2 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 Meas WT': 'mean'})
    # day max
    avgdf['GER 2 Meas WT - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 2 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 Meas WT': 'max'})
    # day min
    avgdf['GER 2 Meas WT - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 2 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 Meas WT': 'min'})

    # Day Night difference
    avgdf['GER 2 Meas WT - Day Night Difference'] = avgdf['GER 2 Meas WT - Day'] - avgdf['GER 2 Meas WT - Night']

    # #Pipe WT Measured GER 3-------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 3 Meas WT - 24hr'] = GERClimateDF[GERClimateDF['GER 3 Meas WT'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 Meas WT': 'mean'})
    # max
    avgdf['GER 3 Meas WT - Max'] = GERClimateDF[GERClimateDF['GER 3 Meas WT'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 Meas WT': 'max'})
    # min
    avgdf['GER 3 Meas WT - Min'] = GERClimateDF[GERClimateDF['GER 3 Meas WT'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 Meas WT': 'min'})

    # night avg
    avgdf['GER 3 Meas WT - Night'] = GERClimateDF[
        (GERClimateDF['GER 3 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 Meas WT': 'mean'})
    # night max
    avgdf['GER 3 Meas WT - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 3 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 Meas WT': 'max'})
    # night min
    avgdf['GER 3 Meas WT - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 3 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 Meas WT': 'min'})

    # day avg
    avgdf['GER 3 Meas WT - Day'] = GERClimateDF[
        (GERClimateDF['GER 3 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 Meas WT': 'mean'})
    # day max
    avgdf['GER 3 Meas WT - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 3 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 Meas WT': 'max'})
    # day min
    avgdf['GER 3 Meas WT - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 3 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 Meas WT': 'min'})

    # Day Night difference
    avgdf['GER 3 Meas WT - Day Night Difference'] = avgdf['GER 3 Meas WT - Day'] - avgdf['GER 3 Meas WT - Night']

    # Pipe WT Measured GER 4--------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 4 Meas WT - 24hr'] = GERClimateDF[GERClimateDF['GER 4 Meas WT'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 Meas WT': 'mean'})
    # max
    avgdf['GER 4 Meas WT - Max'] = GERClimateDF[GERClimateDF['GER 4 Meas WT'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 Meas WT': 'max'})
    # min
    avgdf['GER 4 Meas WT - Min'] = GERClimateDF[GERClimateDF['GER 4 Meas WT'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 Meas WT': 'min'})

    # night avg
    avgdf['GER 4 Meas WT - Night'] = GERClimateDF[
        (GERClimateDF['GER 4 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 Meas WT': 'mean'})
    # night max
    avgdf['GER 4 Meas WT - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 4 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 Meas WT': 'max'})
    # night min
    avgdf['GER 4 Meas WT - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 4 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 Meas WT': 'min'})

    # day avg
    avgdf['GER 4 Meas WT - Day'] = GERClimateDF[
        (GERClimateDF['GER 4 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 Meas WT': 'mean'})
    # day max
    avgdf['GER 4 Meas WT - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 4 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 Meas WT': 'max'})
    # day min
    avgdf['GER 4 Meas WT - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 4 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 Meas WT': 'min'})

    # Day Night difference
    avgdf['GER 4 Meas WT - Day Night Difference'] = avgdf['GER 4 Meas WT - Day'] - avgdf['GER 4 Meas WT - Night']

    # Pipe WT Measured GER 5--------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 5 Meas WT - 24hr'] = GERClimateDF[GERClimateDF['GER 5 Meas WT'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 Meas WT': 'mean'})
    # max
    avgdf['GER 5 Meas WT - Max'] = GERClimateDF[GERClimateDF['GER 5 Meas WT'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 Meas WT': 'max'})
    # min
    avgdf['GER 5 Meas WT - Min'] = GERClimateDF[GERClimateDF['GER 5 Meas WT'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 Meas WT': 'min'})

    # night avg
    avgdf['GER 5 Meas WT - Night'] = GERClimateDF[
        (GERClimateDF['GER 5 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 Meas WT': 'mean'})
    # night max
    avgdf['GER 5 Meas WT - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 5 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 Meas WT': 'max'})
    # night min
    avgdf['GER 5 Meas WT - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 5 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 Meas WT': 'min'})

    # day avg
    avgdf['GER 5 Meas WT - Day'] = GERClimateDF[
        (GERClimateDF['GER 5 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 Meas WT': 'mean'})
    # day max
    avgdf['GER 5 Meas WT - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 5 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 Meas WT': 'max'})
    # day min
    avgdf['GER 5 Meas WT - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 5 Meas WT'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 Meas WT': 'min'})

    # Day Night difference
    avgdf['GER 5 Meas WT - Day Night Difference'] = avgdf['GER 5 Meas WT - Day'] - avgdf['GER 5 Meas WT - Night']

    # Pipe Temp Max Calc GER 1------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 1 Max WT - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 Max WT': 'mean'})  # don't ignore 0 for this
    # night avg
    avgdf['GER 1 Max WT - Night'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Night'].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 Max WT': 'mean'})  # don't ignore 0 for this
    # day avg
    avgdf['GER 1 Max WT - Day'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Day'].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 Max WT': 'mean'})  # don't ignore 0 for this
    # Day Night difference
    avgdf['GER 1 Max WT - Day Night Difference'] = avgdf['GER 1 Max WT - Day'] - avgdf['GER 1 Max WT - Night']

    # Pipe Temp Max Calc GER 2------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 2 Max WT - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 Max WT': 'mean'})  # don't ignore 0 for this
    # night avg
    avgdf['GER 2 Max WT - Night'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Night'].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 Max WT': 'mean'})  # don't ignore 0 for this
    # day avg
    avgdf['GER 2 Max WT - Day'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Day'].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 Max WT': 'mean'})  # don't ignore 0 for this
    # Day Night difference
    avgdf['GER 2 Max WT - Day Night Difference'] = avgdf['GER 2 Max WT - Day'] - avgdf['GER 2 Max WT - Night']

    # Pipe Temp Max Calc GER 3------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 3 Max WT - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 Max WT': 'mean'})  # don't ignore 0 for this
    # night avg
    avgdf['GER 3 Max WT - Night'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Night'].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 Max WT': 'mean'})  # don't ignore 0 for this
    # day avg
    avgdf['GER 3 Max WT - Day'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Day'].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 Max WT': 'mean'})  # don't ignore 0 for this
    # Day Night difference
    avgdf['GER 3 Max WT - Day Night Difference'] = avgdf['GER 3 Max WT - Day'] - avgdf['GER 3 Max WT - Night']

    # Pipe Temp Max Calc GER 4------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 4 Max WT - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 Max WT': 'mean'})  # don't ignore 0 for this
    # night avg
    avgdf['GER 4 Max WT - Night'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Night'].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 Max WT': 'mean'})  # don't ignore 0 for this
    # day avg
    avgdf['GER 4 Max WT - Day'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Day'].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 Max WT': 'mean'})  # don't ignore 0 for this
    # Day Night difference
    avgdf['GER 4 Max WT - Day Night Difference'] = avgdf['GER 4 Max WT - Day'] - avgdf['GER 4 Max WT - Night']

    # Pipe Temp Max Calc GER 5------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 5 Max WT - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 Max WT': 'mean'})  # don't ignore 0 for this
    # night avg
    avgdf['GER 5 Max WT - Night'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Night'].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 Max WT': 'mean'})  # don't ignore 0 for this
    # day avg
    avgdf['GER 5 Max WT - Day'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Day'].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 Max WT': 'mean'})  # don't ignore 0 for this
    # Day Night difference
    avgdf['GER 5 Max WT - Day Night Difference'] = avgdf['GER 5 Max WT - Day'] - avgdf['GER 5 Max WT - Night']

    # Pipe Temp Min Calc GER 1------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 1 Min WT - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 Min WT': 'mean'})  # don't ignore 0 for this
    # night avg
    avgdf['GER 1 Min WT - Night'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Night'].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 Min WT': 'mean'})  # don't ignore 0 for this
    # day avg
    avgdf['GER 1 Min WT - Day'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Day'].groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 Min WT': 'mean'})  # don't ignore 0 for this
    # Day Night difference
    avgdf['GER 1 Min WT - Day Night Difference'] = avgdf['GER 1 Min WT - Day'] - avgdf['GER 1 Min WT - Night']

    # Pipe Temp Min Calc GER 2------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 2 Min WT - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 Min WT': 'mean'})  # don't ignore 0 for this
    # night avg
    avgdf['GER 2 Min WT - Night'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Night'].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 Min WT': 'mean'})  # don't ignore 0 for this
    # day avg
    avgdf['GER 2 Min WT - Day'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Day'].groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 Min WT': 'mean'})  # don't ignore 0 for this
    # Day Night difference
    avgdf['GER 2 Min WT - Day Night Difference'] = avgdf['GER 2 Min WT - Day'] - avgdf['GER 2 Min WT - Night']

    # Pipe Temp Min Calc GER 3------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 3 Min WT - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 Min WT': 'mean'})  # don't ignore 0 for this
    # night avg
    avgdf['GER 3 Min WT - Night'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Night'].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 Min WT': 'mean'})  # don't ignore 0 for this
    # day avg
    avgdf['GER 3 Min WT - Day'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Day'].groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 Min WT': 'mean'})  # don't ignore 0 for this
    # Day Night difference
    avgdf['GER 3 Min WT - Day Night Difference'] = avgdf['GER 3 Min WT - Day'] - avgdf['GER 3 Min WT - Night']

    # Pipe Temp Min Calc GER 4------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 4 Min WT - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 Min WT': 'mean'})  # don't ignore 0 for this
    # night avg
    avgdf['GER 4 Min WT - Night'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Night'].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 Min WT': 'mean'})  # don't ignore 0 for this
    # day avg
    avgdf['GER 4 Min WT - Day'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Day'].groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 Min WT': 'mean'})  # don't ignore 0 for this
    # Day Night difference
    avgdf['GER 4 Min WT - Day Night Difference'] = avgdf['GER 4 Min WT - Day'] - avgdf['GER 4 Min WT - Night']

    # Pipe Temp Min Calc GER 5------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 5 Min WT - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 Min WT': 'mean'})  # don't ignore 0 for this
    # night avg
    avgdf['GER 5 Min WT - Night'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Night'].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 Min WT': 'mean'})  # don't ignore 0 for this
    # day avg
    avgdf['GER 5 Min WT - Day'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Day'].groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 Min WT': 'mean'})  # don't ignore 0 for this
    # Day Night difference
    avgdf['GER 5 Min WT - Day Night Difference'] = avgdf['GER 5 Min WT - Day'] - avgdf['GER 5 Min WT - Night']

    # Weigh Scale GER 1-------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 1 Weigh Scale - 24hr'] = GERClimateDF[GERClimateDF['GER 1 Weigh Scale'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Weigh Scale': 'mean'})
    # max
    avgdf['GER 1 Weigh Scale - Max'] = GERClimateDF[GERClimateDF['GER 1 Weigh Scale'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Weigh Scale': 'max'})
    # min
    avgdf['GER 1 Weigh Scale - Min'] = GERClimateDF[GERClimateDF['GER 1 Weigh Scale'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Weigh Scale': 'min'})

    # night avg
    avgdf['GER 1 Weigh Scale - Night'] = GERClimateDF[
        (GERClimateDF['GER 1 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Weigh Scale': 'mean'})
    # night max
    avgdf['GER 1 Weigh Scale - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 1 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Weigh Scale': 'max'})
    # night min
    avgdf['GER 1 Weigh Scale - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 1 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Weigh Scale': 'min'})

    # day avg
    avgdf['GER 1 Weigh Scale - Day'] = GERClimateDF[
        (GERClimateDF['GER 1 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Weigh Scale': 'mean'})
    # day max
    avgdf['GER 1 Weigh Scale - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 1 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Weigh Scale': 'max'})
    # day min
    avgdf['GER 1 Weigh Scale - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 1 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Weigh Scale': 'min'})

    # Day Night difference
    avgdf['GER 1 Weigh Scale - Day Night Difference'] = avgdf['GER 1 Weigh Scale - Day'] - avgdf[
        'GER 1 Weigh Scale - Night']

    # Weigh Scale GER 2-------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 2 Weigh Scale - 24hr'] = GERClimateDF[GERClimateDF['GER 2 Weigh Scale'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Weigh Scale': 'mean'})
    # max
    avgdf['GER 2 Weigh Scale - Max'] = GERClimateDF[GERClimateDF['GER 2 Weigh Scale'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Weigh Scale': 'max'})
    # min
    avgdf['GER 2 Weigh Scale - Min'] = GERClimateDF[GERClimateDF['GER 2 Weigh Scale'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Weigh Scale': 'min'})

    # night avg
    avgdf['GER 2 Weigh Scale - Night'] = GERClimateDF[
        (GERClimateDF['GER 2 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Weigh Scale': 'mean'})
    # night max
    avgdf['GER 2 Weigh Scale - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 2 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Weigh Scale': 'max'})
    # night min
    avgdf['GER 2 Weigh Scale - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 2 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Weigh Scale': 'min'})

    # day avg
    avgdf['GER 2 Weigh Scale - Day'] = GERClimateDF[
        (GERClimateDF['GER 2 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Weigh Scale': 'mean'})
    # day max
    avgdf['GER 2 Weigh Scale - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 2 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Weigh Scale': 'max'})
    # day min
    avgdf['GER 2 Weigh Scale - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 2 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Weigh Scale': 'min'})

    # Day Night difference
    avgdf['GER 2 Weigh Scale - Day Night Difference'] = avgdf['GER 2 Weigh Scale - Day'] - avgdf[
        'GER 2 Weigh Scale - Night']

    # #Weigh Scale GER 3------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 3 Weigh Scale - 24hr'] = GERClimateDF[GERClimateDF['GER 3 Weigh Scale'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Weigh Scale': 'mean'})
    # max
    avgdf['GER 3 Weigh Scale - Max'] = GERClimateDF[GERClimateDF['GER 3 Weigh Scale'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Weigh Scale': 'max'})
    # min
    avgdf['GER 3 Weigh Scale - Min'] = GERClimateDF[GERClimateDF['GER 3 Weigh Scale'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Weigh Scale': 'min'})

    # night avg
    avgdf['GER 3 Weigh Scale - Night'] = GERClimateDF[
        (GERClimateDF['GER 3 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Weigh Scale': 'mean'})
    # night max
    avgdf['GER 3 Weigh Scale - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 3 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Weigh Scale': 'max'})
    # night min
    avgdf['GER 3 Weigh Scale - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 3 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Weigh Scale': 'min'})

    # day avg
    avgdf['GER 3 Weigh Scale - Day'] = GERClimateDF[
        (GERClimateDF['GER 3 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Weigh Scale': 'mean'})
    # day max
    avgdf['GER 3 Weigh Scale - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 3 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Weigh Scale': 'max'})
    # day min
    avgdf['GER 3 Weigh Scale - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 3 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Weigh Scale': 'min'})

    # Day Night difference
    avgdf['GER 3 Weigh Scale - Day Night Difference'] = avgdf['GER 3 Weigh Scale - Day'] - avgdf[
        'GER 3 Weigh Scale - Night']

    # Weigh Scale GER 4-------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 4 Weigh Scale - 24hr'] = GERClimateDF[GERClimateDF['GER 4 Weigh Scale'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Weigh Scale': 'mean'})
    # max
    avgdf['GER 4 Weigh Scale - Max'] = GERClimateDF[GERClimateDF['GER 4 Weigh Scale'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Weigh Scale': 'max'})
    # min
    avgdf['GER 4 Weigh Scale - Min'] = GERClimateDF[GERClimateDF['GER 4 Weigh Scale'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Weigh Scale': 'min'})

    # night avg
    avgdf['GER 4 Weigh Scale - Night'] = GERClimateDF[
        (GERClimateDF['GER 4 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Weigh Scale': 'mean'})
    # night max
    avgdf['GER 4 Weigh Scale - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 4 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Weigh Scale': 'max'})
    # night min
    avgdf['GER 4 Weigh Scale - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 4 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Weigh Scale': 'min'})

    # day avg
    avgdf['GER 4 Weigh Scale - Day'] = GERClimateDF[
        (GERClimateDF['GER 4 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Weigh Scale': 'mean'})
    # day max
    avgdf['GER 4 Weigh Scale - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 4 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Weigh Scale': 'max'})
    # day min
    avgdf['GER 4 Weigh Scale - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 4 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Weigh Scale': 'min'})

    # Day Night difference
    avgdf['GER 4 Weigh Scale - Day Night Difference'] = avgdf['GER 4 Weigh Scale - Day'] - avgdf[
        'GER 4 Weigh Scale - Night']

    # Weigh Scale GER 5-------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 5 Weigh Scale - 24hr'] = GERClimateDF[GERClimateDF['GER 5 Weigh Scale'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Weigh Scale': 'mean'})
    # max
    avgdf['GER 5 Weigh Scale - Max'] = GERClimateDF[GERClimateDF['GER 5 Weigh Scale'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Weigh Scale': 'max'})
    # min
    avgdf['GER 5 Weigh Scale - Min'] = GERClimateDF[GERClimateDF['GER 5 Weigh Scale'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Weigh Scale': 'min'})

    # night avg
    avgdf['GER 5 Weigh Scale - Night'] = GERClimateDF[
        (GERClimateDF['GER 5 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Weigh Scale': 'mean'})
    # night max
    avgdf['GER 5 Weigh Scale - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 5 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Weigh Scale': 'max'})
    # night min
    avgdf['GER 5 Weigh Scale - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 5 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Weigh Scale': 'min'})

    # day avg
    avgdf['GER 5 Weigh Scale - Day'] = GERClimateDF[
        (GERClimateDF['GER 5 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Weigh Scale': 'mean'})
    # day max
    avgdf['GER 5 Weigh Scale - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 5 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Weigh Scale': 'max'})
    # day min
    avgdf['GER 5 Weigh Scale - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 5 Weigh Scale'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Weigh Scale': 'min'})

    # Day Night difference
    avgdf['GER 5 Weigh Scale - Day Night Difference'] = avgdf['GER 5 Weigh Scale - Day'] - avgdf[
        'GER 5 Weigh Scale - Night']

    # Moist Slab % GER 1------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 1 Moist Slab % - 24hr'] = GERClimateDF[GERClimateDF['GER 1 Moist Slab %'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Moist Slab %': 'mean'})
    # max
    avgdf['GER 1 Moist Slab % - Max'] = GERClimateDF[GERClimateDF['GER 1 Moist Slab %'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Moist Slab %': 'max'})
    # min
    avgdf['GER 1 Moist Slab % - Min'] = GERClimateDF[GERClimateDF['GER 1 Moist Slab %'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Moist Slab %': 'min'})

    # night avg
    avgdf['GER 1 Moist Slab % - Night'] = GERClimateDF[
        (GERClimateDF['GER 1 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Moist Slab %': 'mean'})
    # night max
    avgdf['GER 1 Moist Slab % - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 1 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Moist Slab %': 'max'})
    # night min
    avgdf['GER 1 Moist Slab % - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 1 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Moist Slab %': 'min'})

    # day avg
    avgdf['GER 1 Moist Slab % - Day'] = GERClimateDF[
        (GERClimateDF['GER 1 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Moist Slab %': 'mean'})
    # day max
    avgdf['GER 1 Moist Slab % - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 1 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Moist Slab %': 'max'})
    # day min
    avgdf['GER 1 Moist Slab % - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 1 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 1 Moist Slab %': 'min'})

    # Day Night difference
    avgdf['GER 1 Moist Slab % - Day Night Difference'] = avgdf['GER 1 Moist Slab % - Day'] - avgdf[
        'GER 1 Moist Slab % - Night']

    # Moist Slab % GER 2------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 2 Moist Slab % - 24hr'] = GERClimateDF[GERClimateDF['GER 2 Moist Slab %'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Moist Slab %': 'mean'})
    # max
    avgdf['GER 2 Moist Slab % - Max'] = GERClimateDF[GERClimateDF['GER 2 Moist Slab %'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Moist Slab %': 'max'})
    # min
    avgdf['GER 2 Moist Slab % - Min'] = GERClimateDF[GERClimateDF['GER 2 Moist Slab %'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Moist Slab %': 'min'})

    # night avg
    avgdf['GER 2 Moist Slab % - Night'] = GERClimateDF[
        (GERClimateDF['GER 2 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Moist Slab %': 'mean'})
    # night max
    avgdf['GER 2 Moist Slab % - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 2 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Moist Slab %': 'max'})
    # night min
    avgdf['GER 2 Moist Slab % - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 2 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Moist Slab %': 'min'})

    # day avg
    avgdf['GER 2 Moist Slab % - Day'] = GERClimateDF[
        (GERClimateDF['GER 2 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Moist Slab %': 'mean'})
    # day max
    avgdf['GER 2 Moist Slab % - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 2 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Moist Slab %': 'max'})
    # day min
    avgdf['GER 2 Moist Slab % - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 2 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 2 Moist Slab %': 'min'})

    # Day Night difference
    avgdf['GER 2 Moist Slab % - Day Night Difference'] = avgdf['GER 2 Moist Slab % - Day'] - avgdf[
        'GER 2 Moist Slab % - Night']

    # #Moist Slab % GER 3-----------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 3 Moist Slab % - 24hr'] = GERClimateDF[GERClimateDF['GER 3 Moist Slab %'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Moist Slab %': 'mean'})
    # max
    avgdf['GER 3 Moist Slab % - Max'] = GERClimateDF[GERClimateDF['GER 3 Moist Slab %'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Moist Slab %': 'max'})
    # min
    avgdf['GER 3 Moist Slab % - Min'] = GERClimateDF[GERClimateDF['GER 3 Moist Slab %'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Moist Slab %': 'min'})

    # night avg
    avgdf['GER 3 Moist Slab % - Night'] = GERClimateDF[
        (GERClimateDF['GER 3 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Moist Slab %': 'mean'})
    # night max
    avgdf['GER 3 Moist Slab % - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 3 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Moist Slab %': 'max'})
    # night min
    avgdf['GER 3 Moist Slab % - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 3 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Moist Slab %': 'min'})

    # day avg
    avgdf['GER 3 Moist Slab % - Day'] = GERClimateDF[
        (GERClimateDF['GER 3 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Moist Slab %': 'mean'})
    # day max
    avgdf['GER 3 Moist Slab % - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 3 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Moist Slab %': 'max'})
    # day min
    avgdf['GER 3 Moist Slab % - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 3 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 3 Moist Slab %': 'min'})

    # Day Night difference
    avgdf['GER 3 Moist Slab % - Day Night Difference'] = avgdf['GER 3 Moist Slab % - Day'] - avgdf[
        'GER 3 Moist Slab % - Night']

    # Moist Slab % GER 4------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 4 Moist Slab % - 24hr'] = GERClimateDF[GERClimateDF['GER 4 Moist Slab %'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Moist Slab %': 'mean'})
    # max
    avgdf['GER 4 Moist Slab % - Max'] = GERClimateDF[GERClimateDF['GER 4 Moist Slab %'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Moist Slab %': 'max'})
    # min
    avgdf['GER 4 Moist Slab % - Min'] = GERClimateDF[GERClimateDF['GER 4 Moist Slab %'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Moist Slab %': 'min'})

    # night avg
    avgdf['GER 4 Moist Slab % - Night'] = GERClimateDF[
        (GERClimateDF['GER 4 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Moist Slab %': 'mean'})
    # night max
    avgdf['GER 4 Moist Slab % - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 4 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Moist Slab %': 'max'})
    # night min
    avgdf['GER 4 Moist Slab % - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 4 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Moist Slab %': 'min'})

    # day avg
    avgdf['GER 4 Moist Slab % - Day'] = GERClimateDF[
        (GERClimateDF['GER 4 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Moist Slab %': 'mean'})
    # day max
    avgdf['GER 4 Moist Slab % - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 4 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Moist Slab %': 'max'})
    # day min
    avgdf['GER 4 Moist Slab % - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 4 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 4 Moist Slab %': 'min'})

    # Day Night difference
    avgdf['GER 4 Moist Slab % - Day Night Difference'] = avgdf['GER 4 Moist Slab % - Day'] - avgdf[
        'GER 4 Moist Slab % - Night']

    # Moist Slab % GER 5------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 5 Moist Slab % - 24hr'] = GERClimateDF[GERClimateDF['GER 5 Moist Slab %'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Moist Slab %': 'mean'})
    # max
    avgdf['GER 5 Moist Slab % - Max'] = GERClimateDF[GERClimateDF['GER 5 Moist Slab %'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Moist Slab %': 'max'})
    # min
    avgdf['GER 5 Moist Slab % - Min'] = GERClimateDF[GERClimateDF['GER 5 Moist Slab %'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Moist Slab %': 'min'})

    # night avg
    avgdf['GER 5 Moist Slab % - Night'] = GERClimateDF[
        (GERClimateDF['GER 5 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Moist Slab %': 'mean'})
    # night max
    avgdf['GER 5 Moist Slab % - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 5 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Moist Slab %': 'max'})
    # night min
    avgdf['GER 5 Moist Slab % - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 5 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Moist Slab %': 'min'})

    # day avg
    avgdf['GER 5 Moist Slab % - Day'] = GERClimateDF[
        (GERClimateDF['GER 5 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Moist Slab %': 'mean'})
    # day max
    avgdf['GER 5 Moist Slab % - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 5 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Moist Slab %': 'max'})
    # day min
    avgdf['GER 5 Moist Slab % - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 5 Moist Slab %'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 5 Moist Slab %': 'min'})

    # Day Night difference
    avgdf['GER 5 Moist Slab % - Day Night Difference'] = avgdf['GER 5 Moist Slab % - Day'] - avgdf[
        'GER 5 Moist Slab % - Night']

    # pH Drip GER 1-----------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 1 ph Drip - 24hr'] = GERClimateDF[GERClimateDF['GER ph Drip 1'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 1': 'mean'})
    # max
    avgdf['GER 1 ph Drip - Max'] = GERClimateDF[GERClimateDF['GER ph Drip 1'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 1': 'max'})
    # min
    avgdf['GER 1 ph Drip - Min'] = GERClimateDF[GERClimateDF['GER ph Drip 1'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 1': 'min'})

    # night avg
    avgdf['GER 1 ph Drip - Night'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 1'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 1': 'mean'})
    # night max
    avgdf['GER 1 ph Drip - Night Max'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 1'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 1': 'max'})
    # night min
    avgdf['GER 1 ph Drip - Night Min'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 1'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 1': 'min'})

    # day avg
    avgdf['GER 1 ph Drip - Day'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 1'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 1': 'mean'})
    # day max
    avgdf['GER 1 ph Drip - Day Max'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 1'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 1': 'max'})
    # day min
    avgdf['GER 1 ph Drip - Day Min'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 1'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 1': 'min'})

    # Day Night difference
    avgdf['GER 1 ph Drip - Day Night Difference'] = avgdf['GER 1 ph Drip - Day'] - avgdf['GER 1 ph Drip - Night']

    # pH Drip GER 2-----------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 2 ph Drip - 24hr'] = GERClimateDF[GERClimateDF['GER ph Drip 2'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 2': 'mean'})
    # max
    avgdf['GER 2 ph Drip - Max'] = GERClimateDF[GERClimateDF['GER ph Drip 2'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 2': 'max'})
    # min
    avgdf['GER 2 ph Drip - Min'] = GERClimateDF[GERClimateDF['GER ph Drip 2'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 2': 'min'})

    # night avg
    avgdf['GER 2 ph Drip - Night'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 2'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 2': 'mean'})
    # night max
    avgdf['GER 2 ph Drip - Night Max'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 2'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 2': 'max'})
    # night min
    avgdf['GER 2 ph Drip - Night Min'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 2'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 2': 'min'})

    # day avg
    avgdf['GER 2 ph Drip - Day'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 2'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 2': 'mean'})
    # day max
    avgdf['GER 2 ph Drip - Day Max'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 2'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 2': 'max'})
    # day min
    avgdf['GER 2 ph Drip - Day Min'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 2'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 2': 'min'})

    # Day Night difference
    avgdf['GER 2 ph Drip - Day Night Difference'] = avgdf['GER 2 ph Drip - Day'] - avgdf['GER 2 ph Drip - Night']

    # pH Drip GER 3-----------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 3 ph Drip - 24hr'] = GERClimateDF[GERClimateDF['GER ph Drip 3'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 3': 'mean'})
    # max
    avgdf['GER 3 ph Drip - Max'] = GERClimateDF[GERClimateDF['GER ph Drip 3'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 3': 'max'})
    # min
    avgdf['GER 3 ph Drip - Min'] = GERClimateDF[GERClimateDF['GER ph Drip 3'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 3': 'min'})

    # night avg
    avgdf['GER 3 ph Drip - Night'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 3'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 3': 'mean'})
    # night max
    avgdf['GER 3 ph Drip - Night Max'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 3'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 3': 'max'})
    # night min
    avgdf['GER 3 ph Drip - Night Min'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 3'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 3': 'min'})

    # day avg
    avgdf['GER 3 ph Drip - Day'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 3'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 3': 'mean'})
    # day max
    avgdf['GER 3 ph Drip - Day Max'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 3'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 3': 'max'})
    # day min
    avgdf['GER 3 ph Drip - Day Min'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 3'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 3': 'min'})

    # Day Night difference
    avgdf['GER 3 ph Drip - Day Night Difference'] = avgdf['GER 3 ph Drip - Day'] - avgdf['GER 3 ph Drip - Night']

    # pH Drip GER 4-----------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 4 ph Drip - 24hr'] = GERClimateDF[GERClimateDF['GER ph Drip 4'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 4': 'mean'})
    # max
    avgdf['GER 4 ph Drip - Max'] = GERClimateDF[GERClimateDF['GER ph Drip 4'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 4': 'max'})
    # min
    avgdf['GER 4 ph Drip - Min'] = GERClimateDF[GERClimateDF['GER ph Drip 4'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 4': 'min'})

    # night avg
    avgdf['GER 4 ph Drip - Night'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 4'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 4': 'mean'})
    # night max
    avgdf['GER 4 ph Drip - Night Max'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 4'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 4': 'max'})
    # night min
    avgdf['GER 4 ph Drip - Night Min'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 4'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 4': 'min'})

    # day avg
    avgdf['GER 4 ph Drip - Day'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 4'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 4': 'mean'})
    # day max
    avgdf['GER 4 ph Drip - Day Max'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 4'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 4': 'max'})
    # day min
    avgdf['GER 4 ph Drip - Day Min'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 4'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 4': 'min'})

    # Day Night difference
    avgdf['GER 4 ph Drip - Day Night Difference'] = avgdf['GER 4 ph Drip - Day'] - avgdf['GER 4 ph Drip - Night']

    # pH Drip GER 5-----------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 5 ph Drip - 24hr'] = GERClimateDF[GERClimateDF['GER ph Drip 5'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 5': 'mean'})
    # max
    avgdf['GER 5 ph Drip - Max'] = GERClimateDF[GERClimateDF['GER ph Drip 5'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 5': 'max'})
    # min
    avgdf['GER 5 ph Drip - Min'] = GERClimateDF[GERClimateDF['GER ph Drip 5'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 5': 'min'})

    # night avg
    avgdf['GER 5 ph Drip - Night'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 5'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 5': 'mean'})
    # night max
    avgdf['GER 5 ph Drip - Night Max'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 5'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 5': 'max'})
    # night min
    avgdf['GER 5 ph Drip - Night Min'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 5'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 5': 'min'})

    # day avg
    avgdf['GER 5 ph Drip - Day'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 5'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 5': 'mean'})
    # day max
    avgdf['GER 5 ph Drip - Day Max'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 5'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 5': 'max'})
    # day min
    avgdf['GER 5 ph Drip - Day Min'] = GERClimateDF[
        (GERClimateDF['GER ph Drip 5'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER ph Drip 5': 'min'})

    # Day Night difference
    avgdf['GER 5 ph Drip - Day Night Difference'] = avgdf['GER 5 ph Drip - Day'] - avgdf['GER 5 ph Drip - Night']

    # EC Drip GER 1-----------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 1 EC Drip - 24hr'] = GERClimateDF[GERClimateDF['GER EC Drip 1'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 1': 'mean'})
    # max
    avgdf['GER 1 EC Drip - Max'] = GERClimateDF[GERClimateDF['GER EC Drip 1'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 1': 'max'})
    # min
    avgdf['GER 1 EC Drip - Min'] = GERClimateDF[GERClimateDF['GER EC Drip 1'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 1': 'min'})

    # night avg
    avgdf['GER 1 EC Drip - Night'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 1'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 1': 'mean'})
    # night max
    avgdf['GER 1 EC Drip - Night Max'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 1'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 1': 'max'})
    # night min
    avgdf['GER 1 EC Drip - Night Min'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 1'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 1': 'min'})

    # day avg
    avgdf['GER 1 EC Drip - Day'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 1'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 1': 'mean'})
    # day max
    avgdf['GER 1 EC Drip - Day Max'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 1'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 1': 'max'})
    # day min
    avgdf['GER 1 EC Drip - Day Min'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 1'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 1': 'min'})

    # Day Night difference
    avgdf['GER 1 EC Drip - Day Night Difference'] = avgdf['GER 1 EC Drip - Day'] - avgdf['GER 1 EC Drip - Night']

    # EC Drip GER 2-----------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 2 EC Drip - 24hr'] = GERClimateDF[GERClimateDF['GER EC Drip 2'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 2': 'mean'})
    # max
    avgdf['GER 2 EC Drip - Max'] = GERClimateDF[GERClimateDF['GER EC Drip 2'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 2': 'max'})
    # min
    avgdf['GER 2 EC Drip - Min'] = GERClimateDF[GERClimateDF['GER EC Drip 2'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 2': 'min'})

    # night avg
    avgdf['GER 2 EC Drip - Night'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 2'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 2': 'mean'})
    # night max
    avgdf['GER 2 EC Drip - Night Max'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 2'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 2': 'max'})
    # night min
    avgdf['GER 2 EC Drip - Night Min'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 2'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 2': 'min'})

    # day avg
    avgdf['GER 2 EC Drip - Day'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 2'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 2': 'mean'})
    # day max
    avgdf['GER 2 EC Drip - Day Max'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 2'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 2': 'max'})
    # day min
    avgdf['GER 2 EC Drip - Day Min'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 2'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 2': 'min'})

    # Day Night difference
    avgdf['GER 2 EC Drip - Day Night Difference'] = avgdf['GER 2 EC Drip - Day'] - avgdf['GER 2 EC Drip - Night']

    # EC Drip GER 3-----------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 3 EC Drip - 24hr'] = GERClimateDF[GERClimateDF['GER EC Drip 3'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 3': 'mean'})
    # max
    avgdf['GER 3 EC Drip - Max'] = GERClimateDF[GERClimateDF['GER EC Drip 3'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 3': 'max'})
    # min
    avgdf['GER 3 EC Drip - Min'] = GERClimateDF[GERClimateDF['GER EC Drip 3'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 3': 'min'})

    # night avg
    avgdf['GER 3 EC Drip - Night'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 3'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 3': 'mean'})
    # night max
    avgdf['GER 3 EC Drip - Night Max'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 3'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 3': 'max'})
    # night min
    avgdf['GER 3 EC Drip - Night Min'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 3'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 3': 'min'})

    # day avg
    avgdf['GER 3 EC Drip - Day'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 3'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 3': 'mean'})
    # day max
    avgdf['GER 3 EC Drip - Day Max'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 3'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 3': 'max'})
    # day min
    avgdf['GER 3 EC Drip - Day Min'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 3'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 3': 'min'})

    # Day Night difference
    avgdf['GER 3 EC Drip - Day Night Difference'] = avgdf['GER 3 EC Drip - Day'] - avgdf['GER 3 EC Drip - Night']

    # EC Drip GER 4-----------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 4 EC Drip - 24hr'] = GERClimateDF[GERClimateDF['GER EC Drip 4'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 4': 'mean'})
    # max
    avgdf['GER 4 EC Drip - Max'] = GERClimateDF[GERClimateDF['GER EC Drip 4'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 4': 'max'})
    # min
    avgdf['GER 4 EC Drip - Min'] = GERClimateDF[GERClimateDF['GER EC Drip 4'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 4': 'min'})

    # night avg
    avgdf['GER 4 EC Drip - Night'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 4'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 4': 'mean'})
    # night max
    avgdf['GER 4 EC Drip - Night Max'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 4'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 4': 'max'})
    # night min
    avgdf['GER 4 EC Drip - Night Min'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 4'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 4': 'min'})

    # day avg
    avgdf['GER 4 EC Drip - Day'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 4'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 4': 'mean'})
    # day max
    avgdf['GER 4 EC Drip - Day Max'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 4'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 4': 'max'})
    # day min
    avgdf['GER 4 EC Drip - Day Min'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 4'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 4': 'min'})

    # Day Night difference
    avgdf['GER 4 EC Drip - Day Night Difference'] = avgdf['GER 4 EC Drip - Day'] - avgdf['GER 4 EC Drip - Night']

    # EC Drip GER 5-----------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 5 EC Drip - 24hr'] = GERClimateDF[GERClimateDF['GER EC Drip 5'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 5': 'mean'})
    # max
    avgdf['GER 5 EC Drip - Max'] = GERClimateDF[GERClimateDF['GER EC Drip 5'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 5': 'max'})
    # min
    avgdf['GER 5 EC Drip - Min'] = GERClimateDF[GERClimateDF['GER EC Drip 5'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 5': 'min'})

    # night avg
    avgdf['GER 5 EC Drip - Night'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 5'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 5': 'mean'})
    # night max
    avgdf['GER 5 EC Drip - Night Max'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 5'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 5': 'max'})
    # night min
    avgdf['GER 5 EC Drip - Night Min'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 5'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 5': 'min'})

    # day avg
    avgdf['GER 5 EC Drip - Day'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 5'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 5': 'mean'})
    # day max
    avgdf['GER 5 EC Drip - Day Max'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 5'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 5': 'max'})
    # day min
    avgdf['GER 5 EC Drip - Day Min'] = GERClimateDF[
        (GERClimateDF['GER EC Drip 5'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drip 5': 'min'})

    # Day Night difference
    avgdf['GER 5 EC Drip - Day Night Difference'] = avgdf['GER 5 EC Drip - Day'] - avgdf['GER 5 EC Drip - Night']

    # Flow Rate GER 1--------------------------------------------------------------------------------------------------- UNSURE IF WE SHOULD BE REMOVING 0s

    # avg
    avgdf['GER 1 Flow Rate - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 1': 'mean'})  # don't ignore 0 for this

    # max
    avgdf['GER 1 Flow Rate - Max'] = GERClimateDF[GERClimateDF['GER Flow Rate 1'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 1': 'max'})
    # min
    avgdf['GER 1 Flow Rate - Min'] = GERClimateDF[GERClimateDF['GER Flow Rate 1'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 1': 'min'})

    # night avg
    avgdf['GER 1 Flow Rate - Night'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Night'].groupby(
        'GH 24 Hr Isoweek').agg({'GER Flow Rate 1': 'mean'})
    # night max
    avgdf['GER 1 Flow Rate - Night Max'] = GERClimateDF[
        (GERClimateDF['GER Flow Rate 1'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 1': 'max'})
    # night min
    avgdf['GER 1 Flow Rate - Night Min'] = GERClimateDF[
        (GERClimateDF['GER Flow Rate 1'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 1': 'min'})

    # day avg
    avgdf['GER 1 Flow Rate - Day'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Day'].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 1': 'mean'})
    # day max
    avgdf['GER 1 Flow Rate - Day Max'] = GERClimateDF[
        (GERClimateDF['GER Flow Rate 1'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 1': 'max'})
    # day min
    avgdf['GER 1 Flow Rate - Day Min'] = GERClimateDF[
        (GERClimateDF['GER Flow Rate 1'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 1': 'min'})

    # Day Night difference
    avgdf['GER 1 Flow Rate - Day Night Difference'] = avgdf['GER 1 Flow Rate - Day'] - avgdf['GER 1 Flow Rate - Night']

    # Flow Rate GER 2---------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 2 Flow Rate - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 2': 'mean'})  # don't ignore 0 for this
    # max
    avgdf['GER 2 Flow Rate - Max'] = GERClimateDF[GERClimateDF['GER Flow Rate 2'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 2': 'max'})
    # min
    avgdf['GER 2 Flow Rate - Min'] = GERClimateDF[GERClimateDF['GER Flow Rate 2'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 2': 'min'})

    # night avg
    avgdf['GER 2 Flow Rate - Night'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Night'].groupby(
        'GH 24 Hr Isoweek').agg({'GER Flow Rate 2': 'mean'})
    # night max
    avgdf['GER 2 Flow Rate - Night Max'] = GERClimateDF[
        (GERClimateDF['GER Flow Rate 2'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 2': 'max'})
    # night min
    avgdf['GER 2 Flow Rate - Night Min'] = GERClimateDF[
        (GERClimateDF['GER Flow Rate 2'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 2': 'min'})

    # day avg
    avgdf['GER 2 Flow Rate - Day'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Day'].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 2': 'mean'})
    # day max
    avgdf['GER 2 Flow Rate - Day Max'] = GERClimateDF[
        (GERClimateDF['GER Flow Rate 2'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 2': 'max'})
    # day min
    avgdf['GER 2 Flow Rate - Day Min'] = GERClimateDF[
        (GERClimateDF['GER Flow Rate 2'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 2': 'min'})

    # Day Night difference
    avgdf['GER 2 Flow Rate - Day Night Difference'] = avgdf['GER 2 Flow Rate - Day'] - avgdf['GER 2 Flow Rate - Night']

    # Flow Rate GER 3---------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 3 Flow Rate - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 3': 'mean'})  # don't ignore 0 for this
    # max
    avgdf['GER 3 Flow Rate - Max'] = GERClimateDF[GERClimateDF['GER Flow Rate 3'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 3': 'max'})
    # min
    avgdf['GER 3 Flow Rate - Min'] = GERClimateDF[GERClimateDF['GER Flow Rate 3'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 3': 'min'})

    # night avg
    avgdf['GER 3 Flow Rate - Night'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Night'].groupby(
        'GH 24 Hr Isoweek').agg({'GER Flow Rate 3': 'mean'})
    # night max
    avgdf['GER 3 Flow Rate - Night Max'] = GERClimateDF[
        (GERClimateDF['GER Flow Rate 3'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 3': 'max'})
    # night min
    avgdf['GER 3 Flow Rate - Night Min'] = GERClimateDF[
        (GERClimateDF['GER Flow Rate 3'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 3': 'min'})

    # day avg
    avgdf['GER 3 Flow Rate - Day'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Day'].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 3': 'mean'})
    # day max
    avgdf['GER 3 Flow Rate - Day Max'] = GERClimateDF[
        (GERClimateDF['GER Flow Rate 3'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 3': 'max'})
    # day min
    avgdf['GER 3 Flow Rate - Day Min'] = GERClimateDF[
        (GERClimateDF['GER Flow Rate 3'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 3': 'min'})

    # Day Night difference
    avgdf['GER 3 Flow Rate - Day Night Difference'] = avgdf['GER 3 Flow Rate - Day'] - avgdf['GER 3 Flow Rate - Night']

    # Flow Rate GER 4---------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 4 Flow Rate - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 4': 'mean'})  # don't ignore 0 for this
    # max
    avgdf['GER 4 Flow Rate - Max'] = GERClimateDF[GERClimateDF['GER Flow Rate 4'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 4': 'max'})
    # min
    avgdf['GER 4 Flow Rate - Min'] = GERClimateDF[GERClimateDF['GER Flow Rate 4'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 4': 'min'})

    # night avg
    avgdf['GER 4 Flow Rate - Night'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Night'].groupby(
        'GH 24 Hr Isoweek').agg({'GER Flow Rate 4': 'mean'})
    # night max
    avgdf['GER 4 Flow Rate - Night Max'] = GERClimateDF[
        (GERClimateDF['GER Flow Rate 4'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 4': 'max'})
    # night min
    avgdf['GER 4 Flow Rate - Night Min'] = GERClimateDF[
        (GERClimateDF['GER Flow Rate 4'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 4': 'min'})

    # day avg
    avgdf['GER 4 Flow Rate - Day'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Day'].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 4': 'mean'})
    # day max
    avgdf['GER 4 Flow Rate - Day Max'] = GERClimateDF[
        (GERClimateDF['GER Flow Rate 4'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 4': 'max'})
    # day min
    avgdf['GER 4 Flow Rate - Day Min'] = GERClimateDF[
        (GERClimateDF['GER Flow Rate 4'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 4': 'min'})

    # Day Night difference
    avgdf['GER 4 Flow Rate - Day Night Difference'] = avgdf['GER 4 Flow Rate - Day'] - avgdf['GER 4 Flow Rate - Night']

    # Flow Rate GER 5---------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 5 Flow Rate - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 5': 'mean'})  # don't ignore 0 for this
    # max
    avgdf['GER 5 Flow Rate - Max'] = GERClimateDF[GERClimateDF['GER Flow Rate 5'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 5': 'max'})
    # min
    avgdf['GER 5 Flow Rate - Min'] = GERClimateDF[GERClimateDF['GER Flow Rate 5'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 5': 'min'})

    # night avg
    avgdf['GER 5 Flow Rate - Night'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Night'].groupby(
        'GH 24 Hr Isoweek').agg({'GER Flow Rate 5': 'mean'})
    # night max
    avgdf['GER 5 Flow Rate - Night Max'] = GERClimateDF[
        (GERClimateDF['GER Flow Rate 5'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 5': 'max'})
    # night min
    avgdf['GER 5 Flow Rate - Night Min'] = GERClimateDF[
        (GERClimateDF['GER Flow Rate 5'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 5': 'min'})

    # day avg
    avgdf['GER 5 Flow Rate - Day'] = GERClimateDF[GERClimateDF['Day/Night'] == 'Day'].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 5': 'mean'})
    # day max
    avgdf['GER 5 Flow Rate - Day Max'] = GERClimateDF[
        (GERClimateDF['GER Flow Rate 5'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 5': 'max'})
    # day min
    avgdf['GER 5 Flow Rate - Day Min'] = GERClimateDF[
        (GERClimateDF['GER Flow Rate 5'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER Flow Rate 5': 'min'})

    # Day Night difference
    avgdf['GER 5 Flow Rate - Day Night Difference'] = avgdf['GER 5 Flow Rate - Day'] - avgdf['GER 5 Flow Rate - Night']

    # pH Drain GER 1----------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 1 pH Drain - 24hr'] = GERClimateDF[GERClimateDF['GER pH Drain 1'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 1': 'mean'})
    # max
    avgdf['GER 1 pH Drain - Max'] = GERClimateDF[GERClimateDF['GER pH Drain 1'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 1': 'max'})
    # min
    avgdf['GER 1 pH Drain - Min'] = GERClimateDF[GERClimateDF['GER pH Drain 1'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 1': 'min'})

    # night avg
    avgdf['GER 1 pH Drain - Night'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 1'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 1': 'mean'})
    # night max
    avgdf['GER 1 pH Drain - Night Max'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 1'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 1': 'max'})
    # night min
    avgdf['GER 1 pH Drain - Night Min'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 1'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 1': 'min'})

    # day avg
    avgdf['GER 1 pH Drain - Day'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 1'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 1': 'mean'})
    # day max
    avgdf['GER 1 pH Drain - Day Max'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 1'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 1': 'max'})
    # day min
    avgdf['GER 1 pH Drain - Day Min'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 1'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 1': 'min'})

    # Day Night difference
    avgdf['GER 1 pH Drain - Day Night Difference'] = avgdf['GER 1 pH Drain - Day'] - avgdf['GER 1 pH Drain - Night']

    # pH Drain GER 2----------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 2 pH Drain - 24hr'] = GERClimateDF[GERClimateDF['GER pH Drain 2'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 2': 'mean'})
    # max
    avgdf['GER 2 pH Drain - Max'] = GERClimateDF[GERClimateDF['GER pH Drain 2'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 2': 'max'})
    # min
    avgdf['GER 2 pH Drain - Min'] = GERClimateDF[GERClimateDF['GER pH Drain 2'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 2': 'min'})

    # night avg
    avgdf['GER 2 pH Drain - Night'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 2'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 2': 'mean'})
    # night max
    avgdf['GER 2 pH Drain - Night Max'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 2'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 2': 'max'})
    # night min
    avgdf['GER 2 pH Drain - Night Min'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 2'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 2': 'min'})

    # day avg
    avgdf['GER 2 pH Drain - Day'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 2'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 2': 'mean'})
    # day max
    avgdf['GER 2 pH Drain - Day Max'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 2'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 2': 'max'})
    # day min
    avgdf['GER 2 pH Drain - Day Min'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 2'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 2': 'min'})

    # Day Night difference
    avgdf['GER 2 pH Drain - Day Night Difference'] = avgdf['GER 2 pH Drain - Day'] - avgdf['GER 2 pH Drain - Night']

    # pH Drain GER 3----------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 3 pH Drain - 24hr'] = GERClimateDF[GERClimateDF['GER pH Drain 3'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 3': 'mean'})
    # max
    avgdf['GER 3 pH Drain - Max'] = GERClimateDF[GERClimateDF['GER pH Drain 3'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 3': 'max'})
    # min
    avgdf['GER 3 pH Drain - Min'] = GERClimateDF[GERClimateDF['GER pH Drain 3'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 3': 'min'})

    # night avg
    avgdf['GER 3 pH Drain - Night'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 3'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 3': 'mean'})
    # night max
    avgdf['GER 3 pH Drain - Night Max'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 3'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 3': 'max'})
    # night min
    avgdf['GER 3 pH Drain - Night Min'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 3'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 3': 'min'})

    # day avg
    avgdf['GER 3 pH Drain - Day'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 3'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 3': 'mean'})
    # day max
    avgdf['GER 3 pH Drain - Day Max'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 3'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 3': 'max'})
    # day min
    avgdf['GER 3 pH Drain - Day Min'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 3'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 3': 'min'})

    # Day Night difference
    avgdf['GER 3 pH Drain - Day Night Difference'] = avgdf['GER 3 pH Drain - Day'] - avgdf['GER 3 pH Drain - Night']

    # pH Drain GER 4----------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 4 pH Drain - 24hr'] = GERClimateDF[GERClimateDF['GER pH Drain 4'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 4': 'mean'})
    # max
    avgdf['GER 4 pH Drain - Max'] = GERClimateDF[GERClimateDF['GER pH Drain 4'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 4': 'max'})
    # min
    avgdf['GER 4 pH Drain - Min'] = GERClimateDF[GERClimateDF['GER pH Drain 4'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 4': 'min'})

    # night avg
    avgdf['GER 4 pH Drain - Night'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 4'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 4': 'mean'})
    # night max
    avgdf['GER 4 pH Drain - Night Max'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 4'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 4': 'max'})
    # night min
    avgdf['GER 4 pH Drain - Night Min'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 4'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 4': 'min'})

    # day avg
    avgdf['GER 4 pH Drain - Day'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 4'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 4': 'mean'})
    # day max
    avgdf['GER 4 pH Drain - Day Max'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 4'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 4': 'max'})
    # day min
    avgdf['GER 4 pH Drain - Day Min'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 4'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 4': 'min'})

    # Day Night difference
    avgdf['GER 4 pH Drain - Day Night Difference'] = avgdf['GER 4 pH Drain - Day'] - avgdf['GER 4 pH Drain - Night']

    # pH Drain GER 5----------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 5 pH Drain - 24hr'] = GERClimateDF[GERClimateDF['GER pH Drain 5'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 5': 'mean'})
    # max
    avgdf['GER 5 pH Drain - Max'] = GERClimateDF[GERClimateDF['GER pH Drain 5'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 5': 'max'})
    # min
    avgdf['GER 5 pH Drain - Min'] = GERClimateDF[GERClimateDF['GER pH Drain 5'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 5': 'min'})

    # night avg
    avgdf['GER 5 pH Drain - Night'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 5'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 5': 'mean'})
    # night max
    avgdf['GER 5 pH Drain - Night Max'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 5'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 5': 'max'})
    # night min
    avgdf['GER 5 pH Drain - Night Min'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 5'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 5': 'min'})

    # day avg
    avgdf['GER 5 pH Drain - Day'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 5'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 5': 'mean'})
    # day max
    avgdf['GER 5 pH Drain - Day Max'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 5'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 5': 'max'})
    # day min
    avgdf['GER 5 pH Drain - Day Min'] = GERClimateDF[
        (GERClimateDF['GER pH Drain 5'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER pH Drain 5': 'min'})

    # Day Night difference
    avgdf['GER 5 pH Drain - Day Night Difference'] = avgdf['GER 5 pH Drain - Day'] - avgdf['GER 5 pH Drain - Night']

    # EC Drain GER 1----------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 1 EC Drain - 24hr'] = GERClimateDF[GERClimateDF['GER EC Drain 1'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 1': 'mean'})
    # max
    avgdf['GER 1 EC Drain - Max'] = GERClimateDF[GERClimateDF['GER EC Drain 1'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 1': 'max'})
    # min
    avgdf['GER 1 EC Drain - Min'] = GERClimateDF[GERClimateDF['GER EC Drain 1'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 1': 'min'})

    # night avg
    avgdf['GER 1 EC Drain - Night'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 1'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 1': 'mean'})
    # night max
    avgdf['GER 1 EC Drain - Night Max'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 1'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 1': 'max'})
    # night min
    avgdf['GER 1 EC Drain - Night Min'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 1'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 1': 'min'})

    # day avg
    avgdf['GER 1 EC Drain - Day'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 1'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 1': 'mean'})
    # day max
    avgdf['GER 1 EC Drain - Day Max'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 1'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 1': 'max'})
    # day min
    avgdf['GER 1 EC Drain - Day Min'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 1'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 1': 'min'})

    # Day Night difference
    avgdf['GER 1 EC Drain - Day Night Difference'] = avgdf['GER 1 EC Drain - Day'] - avgdf['GER 1 EC Drain - Night']

    # EC Drain GER 2----------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 2 EC Drain - 24hr'] = GERClimateDF[GERClimateDF['GER EC Drain 2'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 2': 'mean'})
    # max
    avgdf['GER 2 EC Drain - Max'] = GERClimateDF[GERClimateDF['GER EC Drain 2'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 2': 'max'})
    # min
    avgdf['GER 2 EC Drain - Min'] = GERClimateDF[GERClimateDF['GER EC Drain 2'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 2': 'min'})

    # night avg
    avgdf['GER 2 EC Drain - Night'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 2'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 2': 'mean'})
    # night max
    avgdf['GER 2 EC Drain - Night Max'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 2'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 2': 'max'})
    # night min
    avgdf['GER 2 EC Drain - Night Min'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 2'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 2': 'min'})

    # day avg
    avgdf['GER 2 EC Drain - Day'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 2'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 2': 'mean'})
    # day max
    avgdf['GER 2 EC Drain - Day Max'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 2'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 2': 'max'})
    # day min
    avgdf['GER 2 EC Drain - Day Min'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 2'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 2': 'min'})

    # Day Night difference
    avgdf['GER 2 EC Drain - Day Night Difference'] = avgdf['GER 2 EC Drain - Day'] - avgdf['GER 2 EC Drain - Night']

    # EC Drain GER 3----------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 3 EC Drain - 24hr'] = GERClimateDF[GERClimateDF['GER EC Drain 3'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 3': 'mean'})
    # max
    avgdf['GER 3 EC Drain - Max'] = GERClimateDF[GERClimateDF['GER EC Drain 3'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 3': 'max'})
    # min
    avgdf['GER 3 EC Drain - Min'] = GERClimateDF[GERClimateDF['GER EC Drain 3'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 3': 'min'})

    # night avg
    avgdf['GER 3 EC Drain - Night'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 3'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 3': 'mean'})
    # night max
    avgdf['GER 3 EC Drain - Night Max'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 3'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 3': 'max'})
    # night min
    avgdf['GER 3 EC Drain - Night Min'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 3'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 3': 'min'})

    # day avg
    avgdf['GER 3 EC Drain - Day'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 3'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 3': 'mean'})
    # day max
    avgdf['GER 3 EC Drain - Day Max'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 3'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 3': 'max'})
    # day min
    avgdf['GER 3 EC Drain - Day Min'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 3'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 3': 'min'})

    # Day Night difference
    avgdf['GER 3 EC Drain - Day Night Difference'] = avgdf['GER 3 EC Drain - Day'] - avgdf['GER 3 EC Drain - Night']

    # EC Drain GER 4----------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 4 EC Drain - 24hr'] = GERClimateDF[GERClimateDF['GER EC Drain 4'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 4': 'mean'})
    # max
    avgdf['GER 4 EC Drain - Max'] = GERClimateDF[GERClimateDF['GER EC Drain 4'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 4': 'max'})
    # min
    avgdf['GER 4 EC Drain - Min'] = GERClimateDF[GERClimateDF['GER EC Drain 4'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 4': 'min'})

    # night avg
    avgdf['GER 4 EC Drain - Night'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 4'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 4': 'mean'})
    # night max
    avgdf['GER 4 EC Drain - Night Max'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 4'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 4': 'max'})
    # night min
    avgdf['GER 4 EC Drain - Night Min'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 4'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 4': 'min'})

    # day avg
    avgdf['GER 4 EC Drain - Day'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 4'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 4': 'mean'})
    # day max
    avgdf['GER 4 EC Drain - Day Max'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 4'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 4': 'max'})
    # day min
    avgdf['GER 4 EC Drain - Day Min'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 4'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 4': 'min'})

    # Day Night difference
    avgdf['GER 4 EC Drain - Day Night Difference'] = avgdf['GER 4 EC Drain - Day'] - avgdf['GER 4 EC Drain - Night']

    # EC Drain GER 5----------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 5 EC Drain - 24hr'] = GERClimateDF[GERClimateDF['GER EC Drain 5'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 5': 'mean'})
    # max
    avgdf['GER 5 EC Drain - Max'] = GERClimateDF[GERClimateDF['GER EC Drain 5'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 5': 'max'})
    # min
    avgdf['GER 5 EC Drain - Min'] = GERClimateDF[GERClimateDF['GER EC Drain 5'] != 0].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 5': 'min'})

    # night avg
    avgdf['GER 5 EC Drain - Night'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 5'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 5': 'mean'})
    # night max
    avgdf['GER 5 EC Drain - Night Max'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 5'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 5': 'max'})
    # night min
    avgdf['GER 5 EC Drain - Night Min'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 5'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 5': 'min'})

    # day avg
    avgdf['GER 5 EC Drain - Day'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 5'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 5': 'mean'})
    # day max
    avgdf['GER 5 EC Drain - Day Max'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 5'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 5': 'max'})
    # day min
    avgdf['GER 5 EC Drain - Day Min'] = GERClimateDF[
        (GERClimateDF['GER EC Drain 5'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby('GH 24 Hr Isoweek').agg(
        {'GER EC Drain 5': 'min'})

    # Day Night difference
    avgdf['GER 5 EC Drain - Day Night Difference'] = avgdf['GER 5 EC Drain - Day'] - avgdf['GER 5 EC Drain - Night']

    # 24hr Drain GER 1 (Drain l/m2)-------------------------------------------------------------------------------------
    time_str = '05:00:00'
    time_object = datetime.strptime(time_str, '%H:%M:%S').time()

    avgdf['GER 1 24hr Drain - 24hr - avg'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Drain 1'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Drain 1': 'mean'})  # this is what is on crop rego
    avgdf['GER 1 24hr Drain - 24hr - sum'] = GERClimateDF[GERClimateDF['Time'] == time_object].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Drain 1': 'sum'})
    avgdf['GER 1 24hr Drain - 24hr - max'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Drain 1'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Drain 1': 'max'})
    avgdf['GER 1 24hr Drain - 24hr - min'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Drain 1'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Drain 1': 'min'})

    # 24hr Drain GER 2 (Drain l/m2)-------------------------------------------------------------------------------------
    time_str = '05:00:00'
    time_object = datetime.strptime(time_str, '%H:%M:%S').time()

    avgdf['GER 2 24hr Drain - 24hr - avg'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Drain 2'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Drain 2': 'mean'})  # this is what is on crop rego
    avgdf['GER 2 24hr Drain - 24hr - sum'] = GERClimateDF[GERClimateDF['Time'] == time_object].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Drain 2': 'sum'})
    avgdf['GER 2 24hr Drain - 24hr - max'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Drain 2'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Drain 2': 'max'})
    avgdf['GER 2 24hr Drain - 24hr - min'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Drain 2'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Drain 2': 'min'})

    # 24hr Drain GER 3 (Drain l/m2)-------------------------------------------------------------------------------------
    time_str = '05:00:00'
    time_object = datetime.strptime(time_str, '%H:%M:%S').time()

    avgdf['GER 3 24hr Drain - 24hr - avg'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Drain 3'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Drain 3': 'mean'})  # this is what is on crop rego
    avgdf['GER 3 24hr Drain - 24hr - sum'] = GERClimateDF[GERClimateDF['Time'] == time_object].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Drain 3': 'sum'})
    avgdf['GER 3 24hr Drain - 24hr - max'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Drain 3'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Drain 3': 'max'})
    avgdf['GER 3 24hr Drain - 24hr - min'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Drain 3'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Drain 3': 'min'})

    # 24hr Drain GER 4 (Drain l/m2)-------------------------------------------------------------------------------------
    time_str = '05:00:00'
    time_object = datetime.strptime(time_str, '%H:%M:%S').time()

    avgdf['GER 4 24hr Drain - 24hr - avg'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Drain 4'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Drain 4': 'mean'})  # this is what is on crop rego
    avgdf['GER 4 24hr Drain - 24hr - sum'] = GERClimateDF[GERClimateDF['Time'] == time_object].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Drain 4': 'sum'})
    avgdf['GER 4 24hr Drain - 24hr - max'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Drain 4'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Drain 4': 'max'})
    avgdf['GER 4 24hr Drain - 24hr - min'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Drain 4'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Drain 4': 'min'})

    # 24hr Drain GER 5 (Drain l/m2)-------------------------------------------------------------------------------------
    time_str = '05:00:00'
    time_object = datetime.strptime(time_str, '%H:%M:%S').time()

    avgdf['GER 5 24hr Drain - 24hr - avg'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Drain 5'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Drain 5': 'mean'})  # this is what is on crop rego
    avgdf['GER 5 24hr Drain - 24hr - sum'] = GERClimateDF[GERClimateDF['Time'] == time_object].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Drain 5': 'sum'})
    avgdf['GER 5 24hr Drain - 24hr - max'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Drain 5'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Drain 5': 'max'})
    avgdf['GER 5 24hr Drain - 24hr - min'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Drain 5'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Drain 5': 'min'})

    # 24hr Dose GER 1 (Dose l/m2)---------------------------------------------------------------------------------------
    time_str = '05:00:00'
    time_object = datetime.strptime(time_str, '%H:%M:%S').time()

    avgdf['GER 1 24hr Dose - 24hr - avg'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Dose 1'] != 0)].groupby('GH 24 Hr Isoweek').agg(
        {'GER 24hr Dose 1': 'mean'})  # this is what is on crop rego
    avgdf['GER 1 24hr Dose - 24hr - sum'] = GERClimateDF[GERClimateDF['Time'] == time_object].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Dose 1': 'sum'})
    avgdf['GER 1 24hr Dose - 24hr - max'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Dose 1'] != 0)].groupby('GH 24 Hr Isoweek').agg(
        {'GER 24hr Dose 1': 'max'})
    avgdf['GER 1 24hr Dose - 24hr - min'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Dose 1'] != 0)].groupby('GH 24 Hr Isoweek').agg(
        {'GER 24hr Dose 1': 'min'})

    # 24hr Dose GER 2 (Dose l/m2)---------------------------------------------------------------------------------------
    time_str = '05:00:00'
    time_object = datetime.strptime(time_str, '%H:%M:%S').time()

    avgdf['GER 2 24hr Dose - 24hr - avg'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Dose 2'] != 0)].groupby('GH 24 Hr Isoweek').agg(
        {'GER 24hr Dose 2': 'mean'})  # this is what is on crop rego
    avgdf['GER 2 24hr Dose - 24hr - sum'] = GERClimateDF[GERClimateDF['Time'] == time_object].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Dose 2': 'sum'})
    avgdf['GER 2 24hr Dose - 24hr - max'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Dose 2'] != 0)].groupby('GH 24 Hr Isoweek').agg(
        {'GER 24hr Dose 2': 'max'})
    avgdf['GER 2 24hr Dose - 24hr - min'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Dose 2'] != 0)].groupby('GH 24 Hr Isoweek').agg(
        {'GER 24hr Dose 2': 'min'})

    # 24hr Dose GER 3 (Dose l/m2)---------------------------------------------------------------------------------------
    time_str = '05:00:00'
    time_object = datetime.strptime(time_str, '%H:%M:%S').time()

    avgdf['GER 3 24hr Dose - 24hr - avg'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Dose 3'] != 0)].groupby('GH 24 Hr Isoweek').agg(
        {'GER 24hr Dose 3': 'mean'})  # this is what is on crop rego
    avgdf['GER 3 24hr Dose - 24hr - sum'] = GERClimateDF[GERClimateDF['Time'] == time_object].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Dose 3': 'sum'})
    avgdf['GER 3 24hr Dose - 24hr - max'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Dose 3'] != 0)].groupby('GH 24 Hr Isoweek').agg(
        {'GER 24hr Dose 3': 'max'})
    avgdf['GER 3 24hr Dose - 24hr - min'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Dose 3'] != 0)].groupby('GH 24 Hr Isoweek').agg(
        {'GER 24hr Dose 3': 'min'})

    # 24hr Dose GER 4 (Dose l/m2)---------------------------------------------------------------------------------------
    time_str = '05:00:00'
    time_object = datetime.strptime(time_str, '%H:%M:%S').time()

    avgdf['GER 4 24hr Dose - 24hr - avg'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Dose 4'] != 0)].groupby('GH 24 Hr Isoweek').agg(
        {'GER 24hr Dose 4': 'mean'})  # this is what is on crop rego
    avgdf['GER 4 24hr Dose - 24hr - sum'] = GERClimateDF[GERClimateDF['Time'] == time_object].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Dose 4': 'sum'})
    avgdf['GER 4 24hr Dose - 24hr - max'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Dose 4'] != 0)].groupby('GH 24 Hr Isoweek').agg(
        {'GER 24hr Dose 4': 'max'})
    avgdf['GER 4 24hr Dose - 24hr - min'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Dose 4'] != 0)].groupby('GH 24 Hr Isoweek').agg(
        {'GER 24hr Dose 4': 'min'})

    # 24hr Dose GER 5 (Dose l/m2)---------------------------------------------------------------------------------------
    time_str = '05:00:00'
    time_object = datetime.strptime(time_str, '%H:%M:%S').time()

    avgdf['GER 5 24hr Dose - 24hr - avg'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Dose 5'] != 0)].groupby('GH 24 Hr Isoweek').agg(
        {'GER 24hr Dose 5': 'mean'})  # this is what is on crop rego
    avgdf['GER 5 24hr Dose - 24hr - sum'] = GERClimateDF[GERClimateDF['Time'] == time_object].groupby(
        'GH 24 Hr Isoweek').agg({'GER 24hr Dose 5': 'sum'})
    avgdf['GER 5 24hr Dose - 24hr - max'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Dose 5'] != 0)].groupby('GH 24 Hr Isoweek').agg(
        {'GER 24hr Dose 5': 'max'})
    avgdf['GER 5 24hr Dose - 24hr - min'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER 24hr Dose 5'] != 0)].groupby('GH 24 Hr Isoweek').agg(
        {'GER 24hr Dose 5': 'min'})

    # M3 Water Per Day GER 1 (Dose l/m2)--------------------------------------------------------------------------------
    time_str = '05:00:00'
    time_object = datetime.strptime(time_str, '%H:%M:%S').time()

    avgdf['GER 1 M3 Water Per Day - 24hr - avg'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER M3 Water Per Day 1'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER M3 Water Per Day 1': 'mean'})  # this is what is on crop rego
    avgdf['GER 1 M3 Water Per Day - 24hr - sum'] = GERClimateDF[GERClimateDF['Time'] == time_object].groupby(
        'GH 24 Hr Isoweek').agg({'GER M3 Water Per Day 1': 'sum'})
    avgdf['GER 1 M3 Water Per Day - 24hr - max'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER M3 Water Per Day 1'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER M3 Water Per Day 1': 'max'})
    avgdf['GER 1 M3 Water Per Day - 24hr - min'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER M3 Water Per Day 1'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER M3 Water Per Day 1': 'min'})

    # M3 Water Per Day GER 2 (Dose l/m2)--------------------------------------------------------------------------------
    time_str = '05:00:00'
    time_object = datetime.strptime(time_str, '%H:%M:%S').time()

    avgdf['GER 2 M3 Water Per Day - 24hr - avg'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER M3 Water Per Day 2'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER M3 Water Per Day 2': 'mean'})  # this is what is on crop rego
    avgdf['GER 2 M3 Water Per Day - 24hr - sum'] = GERClimateDF[GERClimateDF['Time'] == time_object].groupby(
        'GH 24 Hr Isoweek').agg({'GER M3 Water Per Day 2': 'sum'})
    avgdf['GER 2 M3 Water Per Day - 24hr - max'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER M3 Water Per Day 2'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER M3 Water Per Day 2': 'max'})
    avgdf['GER 2 M3 Water Per Day - 24hr - min'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER M3 Water Per Day 2'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER M3 Water Per Day 2': 'min'})

    # M3 Water Per Day GER 3 (Dose l/m2)--------------------------------------------------------------------------------
    time_str = '05:00:00'
    time_object = datetime.strptime(time_str, '%H:%M:%S').time()

    avgdf['GER 3 M3 Water Per Day - 24hr - avg'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER M3 Water Per Day 3'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER M3 Water Per Day 3': 'mean'})  # this is what is on crop rego
    avgdf['GER 3 M3 Water Per Day - 24hr - sum'] = GERClimateDF[GERClimateDF['Time'] == time_object].groupby(
        'GH 24 Hr Isoweek').agg({'GER M3 Water Per Day 3': 'sum'})
    avgdf['GER 3 M3 Water Per Day - 24hr - max'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER M3 Water Per Day 3'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER M3 Water Per Day 3': 'max'})
    avgdf['GER 3 M3 Water Per Day - 24hr - min'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER M3 Water Per Day 3'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER M3 Water Per Day 3': 'min'})

    # M3 Water Per Day GER 4 (Dose l/m2)--------------------------------------------------------------------------------
    time_str = '05:00:00'
    time_object = datetime.strptime(time_str, '%H:%M:%S').time()

    avgdf['GER 4 M3 Water Per Day - 24hr - avg'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER M3 Water Per Day 4'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER M3 Water Per Day 4': 'mean'})  # this is what is on crop rego
    avgdf['GER 4 M3 Water Per Day - 24hr - sum'] = GERClimateDF[GERClimateDF['Time'] == time_object].groupby(
        'GH 24 Hr Isoweek').agg({'GER M3 Water Per Day 4': 'sum'})
    avgdf['GER 4 M3 Water Per Day - 24hr - max'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER M3 Water Per Day 4'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER M3 Water Per Day 4': 'max'})
    avgdf['GER 4 M3 Water Per Day - 24hr - min'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER M3 Water Per Day 4'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER M3 Water Per Day 4': 'min'})

    # M3 Water Per Day GER 5 (Dose l/m2)--------------------------------------------------------------------------------
    time_str = '05:00:00'
    time_object = datetime.strptime(time_str, '%H:%M:%S').time()

    avgdf['GER 5 M3 Water Per Day - 24hr - avg'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER M3 Water Per Day 5'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER M3 Water Per Day 5': 'mean'})  # this is what is on crop rego
    avgdf['GER 5 M3 Water Per Day - 24hr - sum'] = GERClimateDF[GERClimateDF['Time'] == time_object].groupby(
        'GH 24 Hr Isoweek').agg({'GER M3 Water Per Day 5': 'sum'})
    avgdf['GER 5 M3 Water Per Day - 24hr - max'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER M3 Water Per Day 5'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER M3 Water Per Day 5': 'max'})
    avgdf['GER 5 M3 Water Per Day - 24hr - min'] = GERClimateDF[
        (GERClimateDF['Time'] == time_object) & (GERClimateDF['GER M3 Water Per Day 5'] != 0)].groupby(
        'GH 24 Hr Isoweek').agg({'GER M3 Water Per Day 5': 'min'})

    # 24hr Drain % GER 1------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 1 24hr Drain % - 24hr'] = GERClimateDF[GERClimateDF['GER 24hr Drain % 1'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 1': 'mean'})
    # max
    avgdf['GER 1 24hr Drain % - Max'] = GERClimateDF[GERClimateDF['GER 24hr Drain % 1'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 1': 'max'})
    # min
    avgdf['GER 1 24hr Drain % - Min'] = GERClimateDF[GERClimateDF['GER 24hr Drain % 1'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 1': 'min'})

    # night avg
    avgdf['GER 1 24hr Drain % - Night'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 1'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 1': 'mean'})
    # night max
    avgdf['GER 1 24hr Drain % - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 1'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 1': 'max'})
    # night min
    avgdf['GER 1 24hr Drain % - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 1'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 1': 'min'})

    # day avg
    avgdf['GER 1 24hr Drain % - Day'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 1'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 1': 'mean'})
    # day max
    avgdf['GER 1 24hr Drain % - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 1'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 1': 'max'})
    # day min
    avgdf['GER 1 24hr Drain % - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 1'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 1': 'min'})

    # Day Night difference
    avgdf['GER 1 24hr Drain % - Day Night Difference'] = avgdf['GER 1 24hr Drain % - Day'] - avgdf[
        'GER 1 24hr Drain % - Night']

    # 24hr Drain % GER 2------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 2 24hr Drain % - 24hr'] = GERClimateDF[GERClimateDF['GER 24hr Drain % 2'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 2': 'mean'})
    # max
    avgdf['GER 2 24hr Drain % - Max'] = GERClimateDF[GERClimateDF['GER 24hr Drain % 2'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 2': 'max'})
    # min
    avgdf['GER 2 24hr Drain % - Min'] = GERClimateDF[GERClimateDF['GER 24hr Drain % 2'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 2': 'min'})

    # night avg
    avgdf['GER 2 24hr Drain % - Night'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 2'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 2': 'mean'})
    # night max
    avgdf['GER 2 24hr Drain % - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 2'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 2': 'max'})
    # night min
    avgdf['GER 2 24hr Drain % - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 2'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 2': 'min'})

    # day avg
    avgdf['GER 2 24hr Drain % - Day'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 2'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 2': 'mean'})
    # day max
    avgdf['GER 2 24hr Drain % - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 2'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 2': 'max'})
    # day min
    avgdf['GER 2 24hr Drain % - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 2'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 2': 'min'})

    # Day Night difference
    avgdf['GER 2 24hr Drain % - Day Night Difference'] = avgdf['GER 2 24hr Drain % - Day'] - avgdf[
        'GER 2 24hr Drain % - Night']

    # 24hr Drain % GER 3------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 3 24hr Drain % - 24hr'] = GERClimateDF[GERClimateDF['GER 24hr Drain % 3'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 3': 'mean'})
    # max
    avgdf['GER 3 24hr Drain % - Max'] = GERClimateDF[GERClimateDF['GER 24hr Drain % 3'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 3': 'max'})
    # min
    avgdf['GER 3 24hr Drain % - Min'] = GERClimateDF[GERClimateDF['GER 24hr Drain % 3'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 3': 'min'})

    # night avg
    avgdf['GER 3 24hr Drain % - Night'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 3'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 3': 'mean'})
    # night max
    avgdf['GER 3 24hr Drain % - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 3'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 3': 'max'})
    # night min
    avgdf['GER 3 24hr Drain % - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 3'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 3': 'min'})

    # day avg
    avgdf['GER 3 24hr Drain % - Day'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 3'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 3': 'mean'})
    # day max
    avgdf['GER 3 24hr Drain % - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 3'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 3': 'max'})
    # day min
    avgdf['GER 3 24hr Drain % - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 3'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 3': 'min'})

    # Day Night difference
    avgdf['GER 3 24hr Drain % - Day Night Difference'] = avgdf['GER 3 24hr Drain % - Day'] - avgdf[
        'GER 3 24hr Drain % - Night']

    # 24hr Drain % GER 4------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 4 24hr Drain % - 24hr'] = GERClimateDF[GERClimateDF['GER 24hr Drain % 4'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 4': 'mean'})
    # max
    avgdf['GER 4 24hr Drain % - Max'] = GERClimateDF[GERClimateDF['GER 24hr Drain % 4'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 4': 'max'})
    # min
    avgdf['GER 4 24hr Drain % - Min'] = GERClimateDF[GERClimateDF['GER 24hr Drain % 4'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 4': 'min'})

    # night avg
    avgdf['GER 4 24hr Drain % - Night'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 4'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 4': 'mean'})
    # night max
    avgdf['GER 4 24hr Drain % - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 4'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 4': 'max'})
    # night min
    avgdf['GER 4 24hr Drain % - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 4'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 4': 'min'})

    # day avg
    avgdf['GER 4 24hr Drain % - Day'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 4'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 4': 'mean'})
    # day max
    avgdf['GER 4 24hr Drain % - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 4'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 4': 'max'})
    # day min
    avgdf['GER 4 24hr Drain % - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 4'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 4': 'min'})

    # Day Night difference
    avgdf['GER 4 24hr Drain % - Day Night Difference'] = avgdf['GER 4 24hr Drain % - Day'] - avgdf[
        'GER 4 24hr Drain % - Night']

    # 24hr Drain % GER 5------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 5 24hr Drain % - 24hr'] = GERClimateDF[GERClimateDF['GER 24hr Drain % 5'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 5': 'mean'})
    # max
    avgdf['GER 5 24hr Drain % - Max'] = GERClimateDF[GERClimateDF['GER 24hr Drain % 5'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 5': 'max'})
    # min
    avgdf['GER 5 24hr Drain % - Min'] = GERClimateDF[GERClimateDF['GER 24hr Drain % 5'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 5': 'min'})

    # night avg
    avgdf['GER 5 24hr Drain % - Night'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 5'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 5': 'mean'})
    # night max
    avgdf['GER 5 24hr Drain % - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 5'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 5': 'max'})
    # night min
    avgdf['GER 5 24hr Drain % - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 5'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 5': 'min'})

    # day avg
    avgdf['GER 5 24hr Drain % - Day'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 5'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 5': 'mean'})
    # day max
    avgdf['GER 5 24hr Drain % - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 5'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 5': 'max'})
    # day min
    avgdf['GER 5 24hr Drain % - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 24hr Drain % 5'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Drain % 5': 'min'})

    # Day Night difference
    avgdf['GER 5 24hr Drain % - Day Night Difference'] = avgdf['GER 5 24hr Drain % - Day'] - avgdf[
        'GER 5 24hr Drain % - Night']

    # 24hr Take Up GER 1------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 1 24hr Take Up - 24hr'] = GERClimateDF[GERClimateDF['GER 24hr Take Up 1'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 1': 'mean'})
    # max
    avgdf['GER 1 24hr Take Up - Max'] = GERClimateDF[GERClimateDF['GER 24hr Take Up 1'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 1': 'max'})
    # min
    avgdf['GER 1 24hr Take Up - Min'] = GERClimateDF[GERClimateDF['GER 24hr Take Up 1'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 1': 'min'})

    # night avg
    avgdf['GER 1 24hr Take Up - Night'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 1'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 1': 'mean'})
    # night max
    avgdf['GER 1 24hr Take Up - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 1'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 1': 'max'})
    # night min
    avgdf['GER 1 24hr Take Up - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 1'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 1': 'min'})

    # day avg
    avgdf['GER 1 24hr Take Up - Day'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 1'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 1': 'mean'})
    # day max
    avgdf['GER 1 24hr Take Up - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 1'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 1': 'max'})
    # day min
    avgdf['GER 1 24hr Take Up - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 1'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 1': 'min'})

    # Day Night difference
    avgdf['GER 1 24hr Take Up - Day Night Difference'] = avgdf['GER 1 24hr Take Up - Day'] - avgdf[
        'GER 1 24hr Take Up - Night']

    # 24hr Take Up GER 2------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 2 24hr Take Up - 24hr'] = GERClimateDF[GERClimateDF['GER 24hr Take Up 2'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 2': 'mean'})
    # max
    avgdf['GER 2 24hr Take Up - Max'] = GERClimateDF[GERClimateDF['GER 24hr Take Up 2'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 2': 'max'})
    # min
    avgdf['GER 2 24hr Take Up - Min'] = GERClimateDF[GERClimateDF['GER 24hr Take Up 2'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 2': 'min'})

    # night avg
    avgdf['GER 2 24hr Take Up - Night'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 2'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 2': 'mean'})
    # night max
    avgdf['GER 2 24hr Take Up - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 2'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 2': 'max'})
    # night min
    avgdf['GER 2 24hr Take Up - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 2'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 2': 'min'})

    # day avg
    avgdf['GER 2 24hr Take Up - Day'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 2'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 2': 'mean'})
    # day max
    avgdf['GER 2 24hr Take Up - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 2'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 2': 'max'})
    # day min
    avgdf['GER 2 24hr Take Up - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 2'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 2': 'min'})

    # Day Night difference
    avgdf['GER 2 24hr Take Up - Day Night Difference'] = avgdf['GER 2 24hr Take Up - Day'] - avgdf[
        'GER 2 24hr Take Up - Night']

    # 24hr Take Up GER 3------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 3 24hr Take Up - 24hr'] = GERClimateDF[GERClimateDF['GER 24hr Take Up 3'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 3': 'mean'})
    # max
    avgdf['GER 3 24hr Take Up - Max'] = GERClimateDF[GERClimateDF['GER 24hr Take Up 3'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 3': 'max'})
    # min
    avgdf['GER 3 24hr Take Up - Min'] = GERClimateDF[GERClimateDF['GER 24hr Take Up 3'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 3': 'min'})

    # night avg
    avgdf['GER 3 24hr Take Up - Night'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 3'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 3': 'mean'})
    # night max
    avgdf['GER 3 24hr Take Up - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 3'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 3': 'max'})
    # night min
    avgdf['GER 3 24hr Take Up - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 3'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 3': 'min'})

    # day avg
    avgdf['GER 3 24hr Take Up - Day'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 3'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 3': 'mean'})
    # day max
    avgdf['GER 3 24hr Take Up - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 3'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 3': 'max'})
    # day min
    avgdf['GER 3 24hr Take Up - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 3'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 3': 'min'})

    # Day Night difference
    avgdf['GER 3 24hr Take Up - Day Night Difference'] = avgdf['GER 3 24hr Take Up - Day'] - avgdf[
        'GER 3 24hr Take Up - Night']

    # 24hr Take Up GER 4------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 4 24hr Take Up - 24hr'] = GERClimateDF[GERClimateDF['GER 24hr Take Up 4'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 4': 'mean'})
    # max
    avgdf['GER 4 24hr Take Up - Max'] = GERClimateDF[GERClimateDF['GER 24hr Take Up 4'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 4': 'max'})
    # min
    avgdf['GER 4 24hr Take Up - Min'] = GERClimateDF[GERClimateDF['GER 24hr Take Up 4'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 4': 'min'})

    # night avg
    avgdf['GER 4 24hr Take Up - Night'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 4'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 4': 'mean'})
    # night max
    avgdf['GER 4 24hr Take Up - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 4'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 4': 'max'})
    # night min
    avgdf['GER 4 24hr Take Up - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 4'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 4': 'min'})

    # day avg
    avgdf['GER 4 24hr Take Up - Day'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 4'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 4': 'mean'})
    # day max
    avgdf['GER 4 24hr Take Up - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 4'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 4': 'max'})
    # day min
    avgdf['GER 4 24hr Take Up - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 4'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 4': 'min'})

    # Day Night difference
    avgdf['GER 4 24hr Take Up - Day Night Difference'] = avgdf['GER 4 24hr Take Up - Day'] - avgdf[
        'GER 4 24hr Take Up - Night']

    # 24hr Take Up GER 5------------------------------------------------------------------------------------------------

    # avg
    avgdf['GER 5 24hr Take Up - 24hr'] = GERClimateDF[GERClimateDF['GER 24hr Take Up 5'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 5': 'mean'})
    # max
    avgdf['GER 5 24hr Take Up - Max'] = GERClimateDF[GERClimateDF['GER 24hr Take Up 5'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 5': 'max'})
    # min
    avgdf['GER 5 24hr Take Up - Min'] = GERClimateDF[GERClimateDF['GER 24hr Take Up 5'] != 0].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 5': 'min'})

    # night avg
    avgdf['GER 5 24hr Take Up - Night'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 5'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 5': 'mean'})
    # night max
    avgdf['GER 5 24hr Take Up - Night Max'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 5'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 5': 'max'})
    # night min
    avgdf['GER 5 24hr Take Up - Night Min'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 5'] != 0) & (GERClimateDF['Day/Night'] == 'Night')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 5': 'min'})

    # day avg
    avgdf['GER 5 24hr Take Up - Day'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 5'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 5': 'mean'})
    # day max
    avgdf['GER 5 24hr Take Up - Day Max'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 5'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 5': 'max'})
    # day min
    avgdf['GER 5 24hr Take Up - Day Min'] = GERClimateDF[
        (GERClimateDF['GER 24hr Take Up 5'] != 0) & (GERClimateDF['Day/Night'] == 'Day')].groupby(
        'GH 24 Hr Isoweek').agg(
        {'GER 24hr Take Up 5': 'min'})

    # Day Night difference
    avgdf['GER 5 24hr Take Up - Day Night Difference'] = avgdf['GER 5 24hr Take Up - Day'] - avgdf[
        'GER 5 24hr Take Up - Night']
    
    
    #Recycling EC-------------------------------------------------------------------------------------------------------
    #Just using average for this as I am lazy

    avgdf['GER 1 Recycling EC - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER 1 Recycling EC': 'mean'})  # don't ignore 0 for this

    avgdf['GER 2 Recycling EC - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER 2 Recycling EC': 'mean'})  # don't ignore 0 for this

    avgdf['GER 3 Recycling EC - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER 3 Recycling EC': 'mean'})  # don't ignore 0 for this

    avgdf['GER 4 Recycling EC - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER 4 Recycling EC': 'mean'})  # don't ignore 0 for this

    avgdf['GER 5 Recycling EC - 24hr'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER 5 Recycling EC': 'mean'})  # don't ignore 0 for this


    # Boiler------------------------------------------------------------------------------------------------------------

    avgdf['GER Boiler KW 1 - sum'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER Boiler KW 1': 'sum'})

    avgdf['GER Boiler KW 2 - sum'] = GERClimateDF.groupby('GH 24 Hr Isoweek').agg(
        {'GER Boiler KW 2': 'sum'})

    avgdf['GER Boiler KW Combined - sum'] = avgdf['GER Boiler KW 1 - sum'] + avgdf['GER Boiler KW 2 - sum']
    #Save sheets

    # print(GERClimateDF)
    # print(avgdf)
    GERClimateDF.to_csv(file_name)
    avgdf.to_csv('GER_avg.csv')
    # Deletes all index rows except the Isoweek for last week

    LastWeekIso = str((datetime.today() - timedelta(7)).strftime('%g%V'))
    print(LastWeekIso)
    avgdf = avgdf[avgdf.index == LastWeekIso]



    #Excel Module
    avgdf.to_csv(r'C:\Users\Chris Norris\T&G Global Ltd\Covered Crops Crop Registration - Documents\Crop Registration\3. Culture Data\Weekly Climate Data\Weekly GER Climate.csv', mode='a', header=False)

    with pd.ExcelWriter('demo.xlsx', engine="openpyxl",
                        mode='a') as writer:
        avgdf.to_excel(writer, sheet_name='Sheet1') #Issue was that index was string



def lookforcolumnsfile():
    # Loads Database, Uses First Col as Index, Mixed Data Types, No Space in Front Of Col Titles
    df = pd.read_csv('GERData\GER CMP 1 WATER TEMP 1.csv', index_col=0, low_memory=False, skipinitialspace=True)
    print(df.columns.values)

deletecopymine()
# lookforcolumnsfile()
processdata()

print('Done')



