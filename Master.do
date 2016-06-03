****Shooting for Invisible Institute
   *Contact: bocarba@uchicago.edu
  clear
 set more off
********************************************************************************
**** Data from FOIA received in April 2016 *************************************
********************************************************************************
clear
 cd "~/Dropbox/Research/Police_CPD/Git/CPDB/Shootings/Raw/FOIA_April2016"
  *Convert file in Stata format
 foreach v in 2012 2013 2014 2015{
  import excel "218 Resp SS_`v'.xls", sheet("Sheet1") firstrow case(upper) allstring clear
  gen foiayear="FOIA_April2016"_`v'
  sort COMPLAINT_NUMBER
  save "Extract/foia_apr2016_`v'.dta",replace
 }
 
 clear
 import excel "218 Resp SS_2016.xls", sheet("Sheet1") firstrow case(upper) allstring clear
  gen foiayear="FOIA_April2016"_2016
  tostring DISCIPLINE_CODE,replace 
  replace DISCIPLINE_CODE="" if DISCIPLINE_CODE=="."
  sort COMPLAINT_NUMBER 
 save "Extract/foia_apr2016_2016.dta",replace
 
 **Append datasets
 clear
 use "Extract/foia_apr2016_2012.dta"
 foreach v in 2013 2014 2015 2016 {
  append using "Extract/foia_apr2016_`v'.dta"  
 }   
 *****>Merge with victims data
     *XXXXXXXXXXXXX
     *XXXXXXXXXXXXX
 save "Extract/foia_apr2016.dta",replace 
 
 ***SHORT DATASET
 ***Keep FIREARM DISCHARGE WITH HITS ON or OFF DUTY
 /*
 keep if CURRENT_CATEGORY_CODE=="18A"|CURRENT_CATEGORY_CODE=="18B" | ///
   INITIAL_CATEGORY_CODE=="18A"|INITIAL_CATEGORY_CODE=="18B" | ///
   CURRENT_CATEGORY_CODE=="20A"|CURRENT_CATEGORY_CODE=="20B" | ///
   INITIAL_CATEGORY_CODE=="20A"|INITIAL_CATEGORY_CODE=="20B" | ///
   CURRENT_CATEGORY_CODE=="20C"|CURRENT_CATEGORY_CODE=="20D" | ///
   INITIAL_CATEGORY_CODE=="20C"|INITIAL_CATEGORY_CODE=="20D" | ///
   CURRENT_CATEGORY_CODE=="20Z"|INITIAL_CATEGORY_CODE=="20Z" 
 */
 keep COMPLAINT_NUMBER CURRENT_CATEGORY_CODE INITIAL_CATEGORY_CODE ///
           CURRENT_CATEGORY      INITIAL_CATEGORY      ///
        NOTIFICATION_DATE foia 
        
  gen str4 NOTIFICATION_YEAR  = substr(NOTIFICATION_DATE,6,10)
  gen str3 NOTIFICATION_MONTH = substr(NOTIFICATION_DATE,3,5)      
  gen str2 NOTIFICATION_DAY   = substr(NOTIFICATION_DATE,1,2) 
  
  gen NOTIFICATION_MONTH_num=1  if NOTIFICATION_MONTH=="jan" 
    replace NOTIFICATION_MONTH_num=2  if NOTIFICATION_MONTH=="feb"  
    replace NOTIFICATION_MONTH_num=3  if NOTIFICATION_MONTH=="mar"  
    replace NOTIFICATION_MONTH_num=4  if NOTIFICATION_MONTH=="apr"  
    replace NOTIFICATION_MONTH_num=5  if NOTIFICATION_MONTH=="may"  
    replace NOTIFICATION_MONTH_num=6  if NOTIFICATION_MONTH=="jun"  
    replace NOTIFICATION_MONTH_num=7  if NOTIFICATION_MONTH=="jul"  
    replace NOTIFICATION_MONTH_num=8  if NOTIFICATION_MONTH=="aug"  
    replace NOTIFICATION_MONTH_num=9  if NOTIFICATION_MONTH=="sep"  
    replace NOTIFICATION_MONTH_num=10 if NOTIFICATION_MONTH=="oct"  
    replace NOTIFICATION_MONTH_num=11 if NOTIFICATION_MONTH=="nov"  
    replace NOTIFICATION_MONTH_num=12 if NOTIFICATION_MONTH=="dec" 
 
 drop NOTIFICATION_DATE
 destring COMPLAINT_NUMBER,replace
 destring NOTIFICATION_YEAR,replace 
 destring NOTIFICATION_DAY,replace
 gen NOTIFICATION_DATE=mdy(NOTIFICATION_MONTH_num,NOTIFICATION_DAY,NOTIFICATION_YEAR)
 format NOTIFICATION_DATE %td
 
 
 keep COMPLAINT_NUMBER CURRENT_CATEGORY_CODE INITIAL_CATEGORY_CODE ///
           CURRENT_CATEGORY      INITIAL_CATEGORY      ///
        NOTIFICATION_DATE NOTIFICATION_YEAR foia 
 save "Extract/foia_apr2016_short.dta",replace 

********************************************************************************
**** Data from FOIA received in May 2016 ***************************************
********************************************************************************
clear
 cd "~/Dropbox/Research/Police_CPD/Git/CPDB/Shootings/Raw/FOIA_May2016"
  *Convert file in Stata format
  clear
  
  foreach v in 2013 2014 2015 2016 {
 import excel "qry_IIFOIA_16056217_2016 FINAL.xls", sheet("qry_II_217_Data163_`v'") firstrow case(upper) allstring clear
 gen foiayear="FOIA_May2016_`v'"
 sort COMPLAINT_NUMBER
 save "Extract/foia_may2016_`v'.dta",replace
 } 
 
 foreach j in Parties Incid{
  foreach v in 2012 2011 2010 2009 200807 {
   import excel "qry_IIFOIA_16056217_2016 FINAL.xls", sheet("qry_II_217_`v'_`j'") firstrow case(upper) allstring clear
   gen foiayear="FOIA_May2016_`v'"
   duplicates drop 
   drop if COMPLAINT_NUMBER==""
   sort COMPLAINT_NUMBER
   save "Extract/foia_may2016_`v'_`j'.dta",replace
  }
 }
 
 **Append datasets
 clear
 use "Extract/foia_may2016_2013.dta"
 foreach v in 2014 2015 2016 {
  append using "Extract/foia_may2016_`v'.dta" 
 } 
 
 foreach j in Parties Incid{
  foreach v in 2012 2011 2010 2009 200807 {
   append using "Extract/foia_may2016_`v'_`j'.dta"
  }
 } 
 save "Extract/foia_may2016.dta",replace
 
 ***SHORT DATASET
 keep COMPLAINT_NUMBER CURRENT_CATEGORY_CODE INITIAL_CATEGORY_CODE ///
           CURRENT_CATEGORY      INITIAL_CATEGORY      ///
        COMPLAINT_DATE foia
  rename COMPLAINT_DATE NOTIFICATION_DATE
  
  gen str4 NOTIFICATION_YEAR  = substr(NOTIFICATION_DATE,6,10)
  gen str3 NOTIFICATION_MONTH = substr(NOTIFICATION_DATE,3,5)      
  gen str2 NOTIFICATION_DAY   = substr(NOTIFICATION_DATE,1,2) 
  
    
  gen NOTIFICATION_MONTH_num=1  if NOTIFICATION_MONTH=="jan" 
    replace NOTIFICATION_MONTH_num=2  if NOTIFICATION_MONTH=="feb"  
    replace NOTIFICATION_MONTH_num=3  if NOTIFICATION_MONTH=="mar"  
    replace NOTIFICATION_MONTH_num=4  if NOTIFICATION_MONTH=="apr"  
    replace NOTIFICATION_MONTH_num=5  if NOTIFICATION_MONTH=="may"  
    replace NOTIFICATION_MONTH_num=6  if NOTIFICATION_MONTH=="jun"  
    replace NOTIFICATION_MONTH_num=7  if NOTIFICATION_MONTH=="jul"  
    replace NOTIFICATION_MONTH_num=8  if NOTIFICATION_MONTH=="aug"  
    replace NOTIFICATION_MONTH_num=9  if NOTIFICATION_MONTH=="sep"  
    replace NOTIFICATION_MONTH_num=10 if NOTIFICATION_MONTH=="oct"   
    replace NOTIFICATION_MONTH_num=11 if NOTIFICATION_MONTH=="nov"  
    replace NOTIFICATION_MONTH_num=12 if NOTIFICATION_MONTH=="dec" 
 
 drop NOTIFICATION_DATE
 destring COMPLAINT_NUMBER,replace
 destring NOTIFICATION_YEAR,replace 
 destring NOTIFICATION_DAY,replace
 gen NOTIFICATION_DATE=mdy(NOTIFICATION_MONTH_num,NOTIFICATION_DAY,NOTIFICATION_YEAR)
 format NOTIFICATION_DATE %td
 
 
 keep COMPLAINT_NUMBER CURRENT_CATEGORY_CODE INITIAL_CATEGORY_CODE ///
           CURRENT_CATEGORY      INITIAL_CATEGORY      ///
        NOTIFICATION_DATE NOTIFICATION_YEAR foia 

 save "Extract/foia_may2016_short.dta",replace  
  
********************************************************************************
**** Data from FOIA received in february 2016 **********************************
********************************************************************************
clear
 cd "~/Dropbox/Research/Police_CPD/Git/CPDB/Shootings/Raw/FOIA_february2016"
   *Convert file in Stata format
   
   clear
 foreach v in 18A 18B 20A{  
   import excel "U-Files_rename/`v' - 1 - Incident.xls", sheet("Sheet1") firstrow case(upper) clear
   drop if LOGNO==.
   sort LOGNO
   save "Extract/`v'_1_Incidents.dta",replace

   clear
   import excel "U-Files_rename/`v' - 3 -  Involved Member.xls", sheet("Sheet1") firstrow case(upper) clear
   drop if LOGNO==.
   sort LOGNO 
   save "Extract/`v'_3_Involved.dta",replace

   clear
   import excel "U-Files_rename/`v' - 4 -  cpd witness.xls", sheet("Sheet1") firstrow case(upper) clear
   drop if LOGNO==.
   sort LOGNO 
   save "Extract/`v'_4_Witness.dta",replace

   clear
   import excel "U-Files_rename/`v' - 5 -  cpd Reporting Party.xls", sheet("Sheet1") firstrow case(upper) clear
   drop if LOGNO==.
   sort LOGNO
   save "Extract/`v'_5_ReportingParty.dta",replace   
   }
 
foreach v in 18A 18B 20A{  
   clear
   use "Extract/`v'_1_Incidents.dta"
 merge m:m LOGNO using "Extract/`v'_3_Involved.dta"
 drop _merge
 merge m:m LOGNO using "Extract/`v'_4_Witness.dta"   
 drop _merge
 merge m:m LOGNO using "Extract/`v'_5_ReportingParty.dta"
 gen foiayear="FOIA_february2016"_`v'
 save "Extract/foia_february2016_`v'.dta",replace
}
 
 **Append datasets
 clear
 use "Extract/foia_february2016_18A.dta"
 foreach v in 18B 20A {
  append using "Extract/foia_february2016_`v'.dta"  
 } 
 drop C D E F J K
 
 save "Extract/foia_february2016.dta",replace

 ***SHORT DATASET
 keep LOGNO INITIALCATEGORY TEAMASSIGNEDDATE foia
  rename LOGNO        COMPLAINT_NUMBER
  rename INITIALCATEGORY INITIAL_CATEGORY_CODE 
  rename TEAMASSIGNEDDATE NOTIFICATION_DATE
 
  gen str4 NOTIFICATION_YEAR  = substr(NOTIFICATION_DATE,1,4)
  gen str2 NOTIFICATION_MONTH = substr(NOTIFICATION_DATE,5,6)      
  gen str2 NOTIFICATION_DAY   = substr(NOTIFICATION_DATE,7,8) 

 drop NOTIFICATION_DATE
 replace NOTIFICATION_YEAR="" if NOTIFICATION_YEAR=="-" 
 destring COMPLAINT_NUMBER,replace
 destring NOTIFICATION_YEAR,replace 
 destring NOTIFICATION_DAY,replace
 destring NOTIFICATION_MONTH,replace
 gen NOTIFICATION_DATE=mdy(NOTIFICATION_MONTH,NOTIFICATION_DAY,NOTIFICATION_YEAR)
 format NOTIFICATION_DATE %td
 
 
 keep COMPLAINT_NUMBER INITIAL_CATEGORY_CODE ///
        NOTIFICATION_DATE NOTIFICATION_YEAR foia 
  
 save "Extract/foia_february2016_short.dta",replace  
 


********************************************************************************
clear
set more off
    use "~/Dropbox/Research/Police_CPD/Git/CPDB/Shootings/Raw/FOIA_April2016/Extract/foia_apr2016_short.dta"
 append using "~/Dropbox/Research/Police_CPD/Git/CPDB/Shootings/Raw/FOIA_May2016/Extract/foia_may2016_short.dta"
 append using "~/Dropbox/Research/Police_CPD/Git/CPDB/Shootings/Raw/FOIA_February2016/Extract/foia_february2016_short.dta"
 
 gen str2 start_code_2   = substr(INITIAL_CATEGORY_CODE,1,2)
 gen str1 start_code_1   = substr(INITIAL_CATEGORY_CODE,1,1)
 
 keep if start_code_2 =="18"|start_code_2 =="20"
 *|start_code_1 =="S"
 drop start*

 drop NOTIFICATION_YEAR NOTIFICATION_DATE
 egen rec=tag(COMPLAINT_NUMBER foiayear)
 bys COMPLAINT_NUMBER : egen sum_rec=sum(rec)
 
export delimited using "/Users/bocarba/Dropbox/Research/Police_CPD/Git/CPDB/Shootings/Shootings.csv", replace 

********************************************************************************
*April & May
clear
cd "~/Dropbox/Research/Police_CPD/Git/CPDB/Shootings/Raw"
 use "FOIA_April2016/Extract/foia_apr2016.dta"
 keep COMPLAINT_NUMBER INITIAL_CATEGORY_CODE ///
	  INITIAL_CATEGORY CURRENT_CATEGORY_CODE ///
	  CURRENT_CATEGORY CURRENT_STATUS ///
	  FINDING_CODE BEAT STREET ADDRESS ///
	  LOCATION_CODE  ACCSUEDOFFICER_FNAME ///
	  ACCUSEDOFFICER_LNAME ACCUSED_STAR ///
	  ACCUSED_UNIT  ACCUSED_DETAIL ///
	  ACCUSED_APPOINTMENT_DATE ACCUSED_POSITION ///
	  INVOLVED_OFFICER_LNAME INVOLVED_OFFICER_FNAME ///
	  INVOLVED_OFFICER_UNIT INVOLVED_OFFICER_DETAIL ///
	  INVOLVED_OFFICER_POSITION INVOLVED_OFFICER_AGE ///
	  INVOLVED_OFFICER_SEX INVOLVED_OFFICER_RACE ///
	  INCIDENT_TIME_START INCIDENT_TIME_END NOTIFICATION_DATE LOCATION_CODE	  
	  
	  gen foia="apr2016"
	  label variable foia "FOIA month"
	  duplicates drop	  
 save "temp_apr2016.dta",replace
 
 clear
	use "FOIA_May2016/Extract/foia_may2016.dta"
	rename 	COMPLAINT_NUMBER				COMPLAINT_NUMBER
	rename 	BEAT							BEAT
	rename 	STREET							STREET
	rename 	ADDRESS							ADDRESS
	rename 	LOCATION_CODE					LOCATION_CODE
	rename 	INCIDENT_TIME_START				INCIDENT_TIME_START
	rename 	INCIDENT_TIME_END				INCIDENT_TIME_END
	rename 	COMPLAINT_DATE					NOTIFICATION_DATE
	rename 	ACCUSED_FNAME	 				ACCSUEDOFFICER_FNAME
	rename 	ACCUSED_LNAME	 				ACCUSEDOFFICER_LNAME
	rename 	ACCUSED_STAR_NO	 				ACCUSED_STAR
	rename 	ACCUSED_ASSIGNMENT	 			ACCUSED_UNIT
	rename 	ACCUSED_DETAIL	 				ACCUSED_DETAIL
	rename 	ACCUSED_APPOINTMENT_DATE	 	ACCUSED_APPOINTMENT_DATE
	rename 	ACCUSED_POSITION	 			ACCUSED_POSITION
	rename 	INITIAL_CATEGORY_CODE			INITIAL_CATEGORY_CODE
	rename 	INITIAL_CATEGORY				INITIAL_CATEGORY
	rename 	CURRENT_CATEGORY_CODE			CURRENT_CATEGORY_CODE
	rename 	CURRENT_CATEGORY				CURRENT_CATEGORY
	rename 	CURRENT_STATUS					CURRENT_STATUS
	rename 	FINDING_CODE					FINDING_CODE
	rename 	INVOVLED_PARTY_LNAME			INVOLVED_OFFICER_LNAME
	rename 	INVOVLED_PARTY_FNAME			INVOLVED_OFFICER_FNAME
	rename 	INVOVLED_PARTY_ASSIGNMENT		INVOLVED_OFFICER_UNIT
	rename 	INVOVLED_PARTY_DETAIL			INVOLVED_OFFICER_DETAIL
	rename 	INVOVLED_PARTY_POSITION			INVOLVED_OFFICER_POSITION
	rename 	INVOVLED_PARTY_AGE				INVOLVED_OFFICER_AGE
	rename 	INVOVLED_PARTY_SEX				INVOLVED_OFFICER_SEX
	rename 	INVOVLED_PARTY_RACE				INVOLVED_OFFICER_RACE
	rename 	INVOVLED_PARTY_TYPE				INVOLVED_OFFICER_TYPE
	rename 	INVOVLED_PARTY_DESCRIPTION		INVOLVED_OFFICER_DESCRIPTION
	
	 keep COMPLAINT_NUMBER INITIAL_CATEGORY_CODE ///
	  INITIAL_CATEGORY CURRENT_CATEGORY_CODE ///
	  CURRENT_CATEGORY CURRENT_STATUS ///
	  FINDING_CODE BEAT STREET ADDRESS ///
	  LOCATION_CODE  ACCSUEDOFFICER_FNAME ///
	  ACCUSEDOFFICER_LNAME ACCUSED_STAR ///
	  ACCUSED_UNIT  ACCUSED_DETAIL ///
	  ACCUSED_APPOINTMENT_DATE ACCUSED_POSITION ///
	  INVOLVED_OFFICER_LNAME INVOLVED_OFFICER_FNAME ///
	  INVOLVED_OFFICER_UNIT INVOLVED_OFFICER_DETAIL ///
	  INVOLVED_OFFICER_POSITION INVOLVED_OFFICER_AGE ///
	  INVOLVED_OFFICER_SEX INVOLVED_OFFICER_RACE ///
	  INCIDENT_TIME_START INCIDENT_TIME_END NOTIFICATION_DATE ///
	  LOCATION_CODE	 INVOLVED_OFFICER_DESCRIPTION INVOLVED_OFFICER_TYPE

	gen foia="may2016"	
	label variable foia "FOIA month"
	duplicates drop
 save "temp_may2016.dta",replace	
 
 
 clear
 set more off
			 use "temp_apr2016.dta"
	append using "temp_may2016.dta"
	
	 gen str2 start_code_2   = substr(INITIAL_CATEGORY_CODE,1,2)
	 gen str1 start_code_1   = substr(INITIAL_CATEGORY_CODE,1,1)
	 keep if start_code_2    =="18"|start_code_2 =="20"|start_code_1 =="S"
    
	drop start*
	duplicates drop
	
	 egen rec=tag(COMPLAINT_NUMBER foia)
	 bys COMPLAINT_NUMBER : egen flag=sum(rec)

cd "~/Dropbox/Research/Police_CPD/Git/CPDB/Shootings/Raw"
	export delimited using "shootings-append.csv", replace 
	
