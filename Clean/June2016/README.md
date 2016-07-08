# June 2016 -

## Complaints

Current data sets used **"/raw dump June 2016"** as raw data source.

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
    - The final columns became: Address, Street, Apartment, City_State_Zipcode

- report 2:
  - Two columns: **"Finding & Recommdended Discipline"** and **"Final Finding & Discipline"** were adjacent to two unlabeled columns
    - It was assumed that the actual value in the labeled columns was the **"Finding** and **"Final Finding"** values, while the adjacent columns contained the **"Recommdended Discipline"** and **"Discipline"** values

- june2016_all
  - Reports 2, 3, 4, and 5 all had several overlapping columns (e.g. **Gender**), as these files contained information about the individuals involved in a particular incident
  - To avoid conflicts, the column headers in each of these files were given prefixes that match the type of individual about which the file contained information:
    - report2: **Accused**
    - report3: **PO_Witness** (where PO = police officer)
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
| june2016_all | 16531             |

