# February 2016 - CPD

## Firearm Discharge, Open + Closed

Current data sets used **"/raw dump Feburary2016"** as raw data source. _Folder name is currently misspelled._

### Notes from original spreadsheets:

+ FOIA 15-6588
+ Complaint Date from 26 Sep 2006 to 30 Nov 2015
+ Source Data: Auto CR
+ Bureau of Internal Affairs
+ Analytical Section
+ 1/12/2016

### Categories

+ 18A - Firearm Discharge with Hits - On Duty
+ 18B - Firearm Discharge with Hits - Off Duty
+ 20A - Shots Fired No Hits

### Original Worksheets

+ **Incident Data**
+ **Involved Member Data**: Some log numbers have up to 10 involved members. Combined all into 1 field. Member names are listed alphabetically, separated by " + ".
+ **CPD Witness Data**: Some log numbers have up to 12 involved members. Combined all into 1 field. Member names are listed alphabetically, separated by " + ".
+ **CPD Reporting Party Data**: Removed two duplicate rows.
+ **Incident Address Data**: Removed datetimes duplicates. Some log numbers (e.g. 1067572 and 1021311 in 18A) have two addresses listed - recorded first address, ignored the other.

### Other Notes

+ Cleaned data does not include the following files:
  + 15-6588 Kalven F.pdf
  + crms - 05j complaint and investigator(1).xls
  + crms - 05j cpd witness, reporting party, victim(1).xls
  + crms - 05j Officer(1).xls
+ Consider "-" as blanks or not found.
+ "Closed at IPRA" - There are a number of Closed 1/1/9999 datetimes. **Are these currently under investigation?**
+ [dat_feb2016_officer] tab contains officer information that corresponds to "Involved Member" in [dat_feb2016]
