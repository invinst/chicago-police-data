# DISCLAIMER

These column definitions are based on our interpretations.

These are not official definitions.

# OUR WORKING DEFINITIONS

__COMPLAINT_NUMBER:__
+ Unique identifier for an incident.
+ Can have multiple allegations, multiple officers, multiple witnesses...
+ Synonomous with CRID (Complaint Register ID) and Log #.

__BEAT:__
+ A police beat is an area.
+ Described by integer up to 4 digits.
+ A beat is a subset of a district.
+ The first two digits of a beat represent the District:
  + ___EXAMPLE:___ Beat 0715 is in District 7

__STREET:__
+ Street of the incident

__ADDRESS:__
+ Address of the incident

__LOCATION_CODE:__
+ Where the incident occurred: "Street", "Alley", "Apartment", "CTA" . . .

__INCIDENT_TIME_START:__
+ When the incident began.
+ ___Data quality issue:___ Time encoded in many different formats.
+ ___Data quality issue:___ Many rows are missing this value.

__INCIDENT_TIME_END:__
+ When the incident ended.
+ Same data quality issues as above.

__NOTIFICATION_DATE:__
+ We are assuming that this means when the complaint was reported to IPRA
+ This date could be way past the INCIDENT_TIME_START, for example, if somebody makes a complaint in 2016 about an incident that happend in the 1980s.

__ACCSUEDOFFICER_FNAME:__
+ First name of accused officer.
+ ___Data quality issue:___ Some first names missing. Makes matching difficult.
+ ___Data quality issue:___ Nicknames. Robert vs. Bob, Thomas vs. Tom, Chris vs. Christian/Christopher.

__ACCUSEDOFFICER_LNAME:__
+ Last name of accused officer.
+ ___Data quality issue:___ We don't have middle names. This is far more important than it might sound, because many officers share the same first name, last name combination across the system. (Common names, family relationships, etc.) Missing middle names makes it harder to identify officers.

__ACCUSED_STAR:__
+ Badge number at date of production of FOIA. Note that badge numbers are recycled: Every time a police officer is promoted or changes unit, badge number can change. That badge number can be given to someone else later one.

__ACCUSED_UNIT:__
+ Unit of the accused officer. Some units are districted and some are "roaming" units.

__ACCUSED_DETAIL:__
+ Short-term assignment (?) -- help us define this

__ACCUSED_APPOINTMENT_DATE:__
+ Date the accused officer was appointed, i.e. sworn in and began police service.

__ACCUSED_POSITION:__
+ Officer rank.

__INITIAL_CATEGORY_CODE:__
+ Initial code for complaint category
+ This could mean at time of intake, or beginning of investigation, or at another "initial" time. We are not sure yet.
+ Text description is the next field, if missing use `Context/Categories.xlsx`

__INITIAL_CATEGORY:__
+ Text description of complaint category
+ This could mean at time of intake, or beginning of investigation, or at another "initial" time. We are not sure yet.
+ If missing, use `Context/Categories.xlsx`

__CURRENT_CATEGORY_CODE:__
+ Code for complaint category at the time of production of the data
+ Note that this can __change over time__ from the inital category code, as the IPRA investigation proceeds, new facts are discovered, new judgements or administrative decisions are made...
+ Text description is the next field, if missing use `Context/Categories.xlsx`

__CURRENT_CATEGORY:__
+ Text description of complaint category at the time of production of the data
+ Note that this can __change over time__ from the inital category, as the IPRA investigation proceeds, new facts are discovered, new judgements or administrative decisions are made...
+ If missing, use `Context/Categories.xlsx`

__CURRENT_STATUS:__
+ Status of the investigation at the time of production of the data

__FINDING_CODE:__
+ Finding of the investigation about the incident -- IPRA makes findings and recommendations but cannot enforce or implement disciplinary actions against police officers

__INVOLVED_OFFICER_LNAME:__
+ An involved officer could be a witness, or co-accused officer -- depends on the FOIA. See INVOLVED_OFFICER_TYPE below.
+ Same data quality issues as ACCUSEDOFFICER_LNAME

__INVOLVED_OFFICER_FNAME:__
+ Same data quality issues as ACCUSEDOFFICER_FNAME

__INVOLVED_OFFICER_UNIT:__
+ Involved officer's unit (see ACCUSED_UNIT above)

__INVOLVED_OFFICER_DETAIL:__
+ Involved officer's detail (see ACCUSED_DETAIL above)

__INVOLVED_OFFICER_POSITION:__
+ Involved officer's position (see ACCUSED_POSITION above)

__INVOLVED_OFFICER_AGE:__
+ Age of involved officer -- at date of FOIA or date of incident?

__INVOLVED_OFFICER_SEX:__
+ Sex of involved officer

__INVOLVED_OFFICER_RACE:__
+ Race of involved officer

__foia:__
+ From which FOIA was this row extracted -- May or April?

__INVOLVED_PARTY_DESCRIPTION:__
+ Confusing freetext, not sure how to extract value from this column

__INVOLVED_PARTY_TYPE:__
+ Four categories:
  + Complainant
  + Inmate
  + Victim
  + Witness

__start_code_2:__
+ This column is used for calculation by @ithinkidontknow, was not in the original FOIA

__start_code_1:__
+ This column is used for calculation by @ithinkidontknow, was not in the original FOIA

__rec:__
+ This column is used for calculation by @ithinkidontknow, was not in the original FOIA

__sum_rec:__
+ This column is used for calculation by @ithinkidontknow, was not in the original FOIA
