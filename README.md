# Chicago Police Data

## What is this?

This is a living repository of public data about Chicago’s police officers and their interactions with the public. The  various datasets stored within cover complaints of misconduct, misconduct investigations, use of force reports, awards, promotions, salary, official rosters, unit assignment over time.

The [Invisible Institute](https://invisible.institute/introduction) maintains this repo and uses these datasets to inform [CPDP.co](https://beta.cpdp.co) and @CPDPbot.

## What can I find in here?

Data ready for download can be found in [data/](https://github.com/invinst/chicago-police-data/tree/master/data) zipped up for your convenience.
* cleaned_data contains all our core datasets after the cleaning process
* matched_data contains all our core datasets matched to unique IDs for each officer (see [Workflow](#workflow))
* complaints_nov2016 contains only the raw complaints dataset as it was originally produced in response to our FOIA request.

FOIA response letters can be found in [foia/](https://github.com/invinst/chicago-police-data/tree/master/foia)

### Complaints

This dataset lists complaints about Chicago Police officers between 2000 and mid-2016 as well as the outcomes of those complaints. The data includes complaints made by civilians, which comprise the bulk of the records, as well as complaints made by other officers. Some were investigated by CPD’s Bureau of Internal Affairs. Others were investigated by a succession of civilian review agencies (formerly OPS, then IPRA, now COPA). The names of the officers accused are included.

### Awards

This dataset lists Chicago Police awards between 2005 and mid-2017. The Chicago Police publishes a list of current types of awards in their department directives [here](http://directives.chicagopolice.org/directives/data/a7a57bf0-12d101cb-ad612-d102-2b6676509a070b87.html) and describes the process [here](http://directives.chicagopolice.org/directives/data/a7a57bf0-12cad953-0e212-cada-aafce1abc7fcf520.html). The data includes awards that have been requested but are still in process, awards that were rejected, and awards to non-police employees of the CPD, in addition to awards given to sworn Chicago police officers.

### Salary

This dataset includes salary data for Chicago Police employees by year, spanning from 2002 to 2017. The data is kept by the City of Chicago’s Human Resources Department. Every officer has only one row per year, unless they changed positions in a year.

### Unit History

This dataset tracks the unit assignments of Chicago Police officers over time. The data includes entries dating back to the 1940s, though more recent data appears to be more reliable. Each row includes an officer’s name and date of appointment, plus the units they are leaving and joining. A list of unit numbers and their respective names can be found here.

### Matching Officers

In the raw data, officer identities had no unique identifier. In order to link between data sources, the officers in a single file were deduplicated, and these unique profiles were used to match against officer profiles from other deduplicated data sets. We use an iterative pairwise joining method, which identifies the strongest matches between unique officers in different data sets, removes these matched officers, then repeats the process until the permitted joins are exhausted or there are no unmatched officers left.

## Where did it come from?

The datasets and documents are all sourced originally from the Chicago Police Department (CPD), Civilian Office of Police Accountability (COPA), the Independent Police Review Authority (IPRA), or the City of Chicago. Most of these datasets were released in response to FOIA requests submitted by the Invisible Institute and its partners. Some information was also extracted from public web sites, for example, the [COPA Case Portal](http://www.chicagocopa.org/data-cases/case-portal/), [published reports](http://www.chicagocopa.org/news-publications/publications/) on the COPA web site, and the [City of Chicago Data Portal](https://data.cityofchicago.org/Public-Safety/COPA-Cases-Summary/mft5-nfa8).

# Using the data

This repo aims to be consistent with [Patrick Ball’s Principled Data Processing](https://youtube.com/watch?v=ZSunU9GQdcI).
For a detailed layout of the folder structure and data processing workflow, see [Workflow](#workflow).

Please join us here. Ask questions and share your own work. We have collected these datasets in this repository so that everyone can analyze and investigate police accountability in Chicago using a common body of evidence. If you would like to start a new project using our data, we encourage you to create a [new ticket](https://github.com/invinst/chicago-police-data/issues/new) and use the yellow "independent project" label to [collaborate with others](https://github.com/invinst/chicago-police-data/issues?q=is%3Aopen+is%3Aissue+label%3A%22independent+project%22) here on GitHub.

## Contributing to this repository

Browse the ["repo issues" label on the Issue Tracker](https://github.com/invinst/chicago-police-data/issues?q=is%3Aopen+is%3Aissue+label%3A%22repo+issue%22) to see where help is needed.

If you discover a problem in this repo or if you find something that is inadequately explained, please open a [new ticket](https://github.com/invinst/chicago-police-data/issues/new) in the issue tracker and use the red "repo issue" label.

## I have a question

If you have a question about the information in this repo, where it comes from, or anything else related, open up a [new ticket](https://github.com/invinst/chicago-police-data/issues/new) in the tracker and use the purple "question" label.

# Workflow

The central goal of this repository is to make our data processing workflow transparent, reproducible, and auditable. While some output information may be redacted or removed from final publishing due to privacy concerns, the code will remain in this public repository so that the full process is in public view.

There are two main data processing directories in this repository are [individual/](###individual/) and [merge/](###merge/). Additionally, the [share/](###share/) directory contains files used across multiple tasks. The [frozen/](###frozen/) directory contains raw data and documents.

## Naming Conventions

Data files and directories which are related to a single FOIA or subset of a FOIA (including individual/, merge/, and frozen/ directories) follow a simple naming format: [data description]\_[data start year - data end year]\_[data received year - data received month]\_[FOIA number] (FOIA numbers are only included in directory names).

For example: the directory named individual/complaints-accused\_2000-2016\_2016-11\_p046957/ contains the data processing workflow for accused officers in complaints data, from 2000 to 2016, and this FOIA (number p046957) was received in November of 2016. As multiple types of complaint information came from the same FOIA, there are other directories with similar names, such as individual/complaints-victims\_2000-2016\_2016-11\_p046957/ which refers to the same data but only covers information about victims.

If some information is nonexistent or irrelevant (for example, the unit history dataset has no explicit “start year”), then that part will be left blank.

## Task Folders

The lowest level of directories in both individual/ and merge/ trees contain input/, output/, src/, and sometimes hand/ directories.

* **input**/ contains the data files that will be used for the current task. Tasks often take the previous tasks output as input.
* **src**/ contains all scripts used in the task: the helper functions, the task specific script (ex: if the task directory is clean/ then the main script will be clean.py), and the makefile (always named Makefile) which runs the task.
* **output**/ contains all files created in the task, which may include the processed input files, .log files, and .yaml files.
* **hand**/ contains reference files that are commonly shared between tasks and FOIAs, which may be needed for standardizing race, gender, column names.

### individual/

In **individual**/, there are multiple directories named according to their FOIA number and the month of receipt (if applicable) and the topic and date rage of the data. For some of these FOIAs (for example, the complaint and TRR data), there are multiple types of data contained in a single FOIA. The data description for these is complaints-[specific data] and TRR-[specific data]. The workflow goes as follows: import/ -> clean/ -> assign-unique-ids/ (if there are identifiable individuals) -> export/. All individual/ subdirectories can be run independently of each other.

* **import**/ takes the data in whatever format it has been received in (.csv, .xlsx, etc.), does minor formatting, standardizes column names, and collects initial metadata. Then it writes the data to a .csv.gz file.
* **clean**/ pushes the imported data through various cleaning functions that will standardize names, race, genders, columns that must be integers or dates, etc.
* **assign-unique-ids**/ identifies unique individuals with intra-file identification numbers based on file-specific characteristics. Two files will be written, one \_profiles file which contains one row per unique individual with all relevant demographic information and unique intra-file id, and another file identical to the ingested cleaned data with the addition of intra-file unique identifiers.
* **export**/ performs any final processing and exporting of the dataset that is needed. This may involve identifying individuals for merging from the \_profiles file before it is to be used in the merging process, or removing redundant columns from the main dataset.

### merge/

In merge/, exported files from the individual/ directories are brought together to be unified into the main relational dataset. While there cannot be a direct link between some files (for example awards and complaints) these files are linked through common officers identified within. The main output of each of these tasks (beginning with a number e.x. 01_roster...) is to produce the officer-reference.csv.gz file, which is a collection of \_profiles files combined with a unique ID unifying the same officer. This file is used to compare/collect potentially differing information about the same officer in different files.

Each subdirectory beginning with a number indicating the order in which the merges are run. Generally, the files are merged in order of number of unique officers in the data, beginning with the newest roster data set. Each directory's input contains the relevant 'full' file and the relevant \_profiles file, as well as the officer-reference file from the previous step. After the numbered tasks are run, the non-numbered task can run (in any order).

The `final-profiles` only takes in the officer-reference file from the last merge, and outputs a condensed profiles file that contains the "best" profile for every individual.

`generate_TRR_flags` does not utilize any officer information, but rather generates columns in the main TRR data set using information aggregated from weapon-discharge data and actions-response data.

These files in merge/ destory/change information and are only used for CPDP:

`fill_salary_ranks` fills the salary data with missing observations (year x rank x officer) for missing Lieutenants (output as salary-filled...) and then aggregates the filled data to 1 row per year x rank x officer (output as salary-ranks...)

`resolve_complaints` take core complaints files (complaints and accused) and generates/aggregates the pre-2000, 2016, and 2018 files to determine which data should be utilized for display purposes. Specifically, this ensures 1 row per CR in the complaints data ('complaints-complaints.csv.gz'), and 1 row per CR x officer in the accused data ('complaints-accused.csv.gz').

`resolve_complaints-supplementary` takes non-core complaints files (complainants, witnesses, investigators, victims) and creates resolved/aggregated files, e.g. information from either 2016 or 2018 complaints files. Outputs 'complaints-victims.csv.gz', etc.

`resolve_unit-history` takes all/both unit-history files and resolves conflicts and generates the aggregated 'unit-history.csv.gz'

### share/

share/ contains three sub-directories: src/ , hand/ , and tests/.

* **src**/ contains all helper functions/scripts that are used across multiple datasets or tasks, generally starting with the task to which they are relevant (merge\_functions.py or clean\_functions.py), or utils scripts that contain auxiliary helper functions (clean\_name\_utils.py or general\_utils.py). The file setup.py is used in all main scripts, regardless of dataset or task, because it initializes a logger and yaml file to store important information and it initializes namedtuples for all of the variables used in the main script.

* **hand**/ contains all reference data that is used across multiple datasets or tasks, generally stored in .yaml files. column\_names.yaml contains file specific column name standardization data for the import/ step, and column\_types.yaml stores the types of specific columns designated for cleaning in the clean/ step.

* **tests**/ contains pytest files with the format test_[file from src/].py and a hand/ directory (for cleaning tests).
