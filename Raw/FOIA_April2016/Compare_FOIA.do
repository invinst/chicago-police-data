**Contact:bocarba@uchicago.edu
cd "~/Dropbox/Research/Police_CPD/RAW/"

****FOIA_2015
clear
set more off
use "Invisible Institute/extracts/PartII/II_raw_Allegations_V2.dta"
	drop if incident_date==.
	rename	incident_date	inc_incident_date
	gen day=1
	gen double t_inc_date = dofc(inc_incident_date)
	gen inc_year        =year(t_inc_date)
	bys inc_year:distinct crid
	bys inc_year:distinct officer_id
save "Invisible Institute/extracts/PartII/II_temp.dta",replace

clear
set more off
use "Invisible Institute/extracts/PartII/II_temp.dta"
	keep crid inc_year
	duplicates drop
	gen old_II=1
save "Invisible Institute/extracts/PartII/II_old_crid.dta",replace 	

clear
set more off
use "Invisible Institute/extracts/PartII/II_temp.dta"
	keep cat_id
	duplicates drop
	rename cat_id INITIAL_CATEGORY_CODE
	sort INITIAL_CATEGORY_CODE
save "Invisible Institute/extracts/PartII/II_old_category.dta",replace

erase "Invisible Institute/extracts/PartII/II_temp.dta"

********************************************************************************
***Complaints FOIA_April2016
********************************************************************************	
clear
set more off
cd "~/Dropbox/Research/Police_CPD/RAW/"
foreach v in 2012 2013 2014 2015{
	import excel "/Users/bocarba/Dropbox/Research/Police_CPD/RAW/RAW_bb/FOIA_April2016/218 Resp SS_`v'.xls", sheet("Sheet1") firstrow clear
	gen foiayear=`v'
	save "Invisible Institute/extracts/PartII/II_raw_foia_apr2016_`v'.dta",replace
}

clear
import excel "/Users/bocarba/Dropbox/Research/Police_CPD/RAW/RAW_bb/FOIA_April2016/218 Resp SS_2016.xls", sheet("Sheet1") firstrow clear
	gen foiayear=2016
	tostring Discipline_Code ,replace 
	replace Discipline_Code="" if Discipline_Code=="."
save "Invisible Institute/extracts/PartII/II_raw_foia_apr2016_2016.dta",replace

	**Append datasets
	clear
	use "Invisible Institute/extracts/PartII/II_raw_foia_apr2016_2012.dta"
	foreach v in 2013 2014 2015 2016 {
		append using "Invisible Institute/extracts/PartII/II_raw_foia_apr2016_`v'.dta"
	}	

	**Erase tempory datasets
	cd "Invisible Institute/extracts/PartII/"
	foreach v in 2013 2014 2015 2016 {
		erase II_raw_foia_apr2016_`v'.dta
	}	

**Some Modifications
	rename Complaint_Number crid	
	format Incident_Time_Start %tc
	gen double Incident_Date = dofc(Incident_Time_Start)
	gen Incident_Year        =year(Incident_Date)
	gen Notification_Year    =year(Notification_Date)
	format Incident_Date %td
	sort Incident_Date
	gen II_FOIA2015=Incident_Date>=18699 & Incident_Date<=20319
	label variable	II_FOIA2015 "Match with II old FOIA (between March 13, 2011 and  August 19, 2015)"

log using "/Users/bocarba/Dropbox/Research/Police_CPD/RAW/RAW_bb/FOIA_April2016/Review_BB/Codebook_FOIA2016_V1.smcl", replace	
	set more off
	codebook crid Beat LOCATION_CODE Incident_Year Incident_Date ///
		 Notification_Year Notification_Date ///
		 IPRA_Closed_Date IPRA_Closed_Date IPRA_Investigator_LastName ///
		 IPRA_Investigator_FirstName AccsuedOfficer_FName ///
		 AccusedOfficer_LName Accused_Star Accused_Unit ///
		 Accused_Appointment_Date Accused_Position ///
		 INITIAL_CATEGORY CURRENT_CATEGORY ///
		 CURRENT_STATUS FINDING_CODE Discipline_Code ///
		 Recommended_Number_Of_Days Involved_Officer_Lname ///
		 Involved_Officer_FName Involved_Officer_Unit ///
		 Involved_Officer_Position ///
		 Involved_Officer_Age Involved_Officer_Sex Involved_Officer_Race ///
		 foiayear AccusedOfficer_FName, tabulate(80)
log close

log using "/Users/bocarba/Dropbox/Research/Police_CPD/RAW/RAW_bb/FOIA_April2016/Review_BB/Codebook_FOIA2016_V2.smcl", replace		 
codebook crid Beat LOCATION_CODE Incident_Year Incident_Date ///
		 Notification_Year Notification_Date ///
		 IPRA_Closed_Date IPRA_Closed_Date IPRA_Investigator_LastName ///
		 IPRA_Investigator_FirstName AccsuedOfficer_FName ///
		 AccusedOfficer_LName Accused_Star Accused_Unit ///
		 Accused_Appointment_Date Accused_Position ///
		 INITIAL_CATEGORY CURRENT_CATEGORY ///
		 CURRENT_STATUS FINDING_CODE Discipline_Code ///
		 Recommended_Number_Of_Days Involved_Officer_Lname ///
		 Involved_Officer_FName Involved_Officer_Unit ///
		 Involved_Officer_Position ///
		 Involved_Officer_Age Involved_Officer_Sex Involved_Officer_Race ///
		 foiayear AccusedOfficer_FName if II_FOIA2015==1, tabulate(80)	 
log close		 

	bys foiayear: distinct crid

	tostring crid ,replace 
	sort crid
	merge m:m crid using "II_old_crid.dta"
	rename _merge merge_crid
	egen fcrid=tag(crid)
		
	label define mergelabel 1 "FOIA 2016 only" 2  "FOIA 2015 only" 3 "Matched"				 	 				 	 				 	 
	label values merge_crid mergelabel	

	ta  Incident_Year merge_crid if fcrid,m	
	ta  inc_year      merge_crid if fcrid,m
	
	sort INITIAL_CATEGORY_CODE
	merge m:m INITIAL_CATEGORY_CODE using "II_old_category.dta"
	rename _merge merge_category
	label values merge_category mergelabel	
	egen fcat=tag(INITIAL_CATEGORY_CODE)
	
    ta  INITIAL_CATEGORY_CODE merge_category if fcat,m	

********************************************************************************
***Complaints FOIA_April2016, victims
********************************************************************************	
clear
set more off	
import excel "/Users/bocarba/Dropbox/Research/Police_CPD/RAW/RAW_bb/FOIA_April2016/fwdfoia16056217/217 Victim Spreadsheet.xls", sheet("Sheet1") firstrow	
	codebook, tabulate(80)
	
