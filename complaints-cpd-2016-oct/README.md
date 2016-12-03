# October 2016 CPD Complaints FOIA Release

Data cleaned from the full FOIA release of CPD complaints in October 2016 with complaints dating back to 1967. Complaints were merged from the files `CR_AllRecords.xlsx` and `foia 14-3668 officers with cr allegations.pdf`, and the process for cleaning the pdf can be found [in this repo](https://github.com/pjsier/chicago-police-data-cleanup). 

Context data not originally included in the release was merged in from the `context_data` folder of this repo based on officer full name and date of appointment, and complaints were structured based off of [this method by @DGalt](https://gist.github.com/DGalt/6b419549086d3aadff8b7cc072fc60b3) in the file `complaints-cpd-2016-oct.csv`. Additionally, cleaned complaints without some additional context data and matching the format in `COLUMN-DICTIONARY.md` are in the file `cleaned_complaints_data.csv`. 

This dataset had fewer details on individual complaints and officers, but more overall complaints overall, so there's still the potential for merging in more context data on complaints and officers through other cleaned datasets here. 
