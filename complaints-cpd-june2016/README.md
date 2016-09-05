# complaints-cpd-june2016

This dataset contains records of complaints against police officers, obtained from CPD in June 2016 filling a FOIA request by the Invisible Institute. The corresponding [**complaints-ipra-apr2016**]() dataset was obtained from IPRA using an indentical FOIA request.

The files in this directory have been formatted and cleaned to facilitate use. The raw files from CPD and copies of the FOIA documents can be found in the **raw/** subdirectory.

## List of Columns

* Complaint\_Number
* Beat
* Location\_Code
* Address
* Street
* Apartment
* City\_State\_Zipcode
* Incident\_Datetime
* Complaint\_Date
* Closed\_Date
* Investigator\_Name
* Investigator\_Current\_Assignment
* Investigator\_Rank
* Investigator\_Star
* Investigator\_Appointed\_Date
* Accused\_Name
* Accused\_Birth\_Yr
* Accused\_Gender
* Accused\_Race\_Code
* Accused\_Date\_of\_Appt
* Accused\_Current\_Unit
* Accused\_Current\_Rank
* Accused\_Star
* Accused\_Complaint\_Category
* Accused\_Finding
* Accused\_Recommended\_Discipline
* Accused\_Final\_Finding	Accused\_Discipline
* PO\_Witness\_Name
* PO\_Witness\_Gender
* PO\_Witness\_Race
* PO\_Witness\_Star
* PO\_Witness\_Birth\_Year
* PO\_Witness\_Date\_Appointed
* Victim\_Gender
* Victim\_Age
* Victim\_Race\_Desc
* Complainant\_Gender
* Complainant\_Age
* Complainant\_Race\_Desc

## Cleaning notes

General notes:
- In each file it was assumed that **"Number:"** corresponded to complaint number
  - The files were organized where a single complaint number might have multiple rows associated with it, yet the number would only be present in the first row
  - Rows below the first row for any complaint number (up to the next complaint number) were NaNs - these were filled in with the above complaint number.
- Rows with **end of record** in a column were otherwise all NaNs, and so were dropped
- Rows or columns that were all NaNs were also dropped

File Specific notes:
- report 1:
  - There existed rows that contained information about the assigned investigator
    - The values in these rows did not correspond to the column headers for those rows
    - These values were separated out and used to create separate columns, designated by the **Investigator** prefix
  - There are four columns with address information, but initially only one of these had a label (the others were blank)
    - The final columns became: Address, Street, Apartment, City\_State\_Zipcode

- report 2:
  - Two columns: **"Finding & Recommdended Discipline"** and **"Final Finding & Discipline"** were adjacent to two unlabeled columns
    - It was assumed that the actual value in the labeled columns was the **"Finding** and **"Final Finding"** values, while the adjacent columns contained the **"Recommdended Discipline"** and **"Discipline"** values

- june2016\_all
  - Reports 2, 3, 4, and 5 all had several overlapping columns (e.g. **Gender**), as these files contained information about the individuals involved in a particular incident
  - To avoid conflicts, the column headers in each of these files were given prefixes that match the type of individual about which the file contained information:
    - report2: **Accused**
    - report3: **PO\_Witness** (where PO = police officer)
    - report4: **Victim**
    - report5: **Complainant**

The entire cleaning process is outlined with the associated code [here](https://gist.github.com/DGalt/6b419549086d3aadff8b7cc072fc60b3)

### Summary Table

| File         | Unique CRID Count |
|--------------|-------------------|
| report1      | 16531             |
| report2      | 10086             |
| report3      | 1471              |
| report4      | 6271              |
| report5      | 13238             |
| june2016\_all | 16531             |
