# May 2016 - IPRA

## Shootings, Open + Closed

Current data sets used **"/raw dump May2016"** as raw data source. 

- Original tabs for 2013 through 2016 have all 48 columns aggregated in one sheet
- Original tabs for 200807 through 2012 have **[incid]** and **[parties]** in separate sheets
  -  **[incid]**  and **[parties]** combined include all 48 cols found in 2013-2016
  - the **[incid]** sheets seem to be unique per **complaint_number** and **finding_id**
  - **finding_id** is not found in **[parties]** sheets
  - used **complaint_number** and **accused_star_no** (approx. officer) to merge sheets
    - if match not found, tried to match by complaint_number only
    - for numeric fields, made sure 0 -> blank (to avoid dates turning into 1900's)
