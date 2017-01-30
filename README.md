# Chicago Police Incidents Data

## What is this?

This is a living repository of public data about Chicago’s police officers and their interactions with the public, maintained by the Invisible Institute and the ChiHackNight community. The datasets stores in this repository describe  deal with several different forms of data covering shootings, use of force, complaints of misconduct and their investigations. Most was released in response to FOIA requests by the Invisible Institute and its partners.

This repository also serves as a hub to facilitate the Chicago community's efforts to use these data to serve as a common body of evidence to better understand the activities of the Chicago Police Department.

Be sure to look into the `context_data` folder as its contents will help you to understand the full picture. It includes awards, commendations, promotions, crisis intervention trainings, and demographic details for all officers, and their unit assignments over time. More specific details about the transactional datasets here are detailed at the end.

## Where did it come from?

The original data and documents are all sourced directly from the Chicago Police Department (CPD), the Independent Police Review Authority (IPRA), or the City of Chicago. The raw files were released pursuant to [Freedom of Information Act (FOIA)](https://www.wikiwand.com/en/Freedom_of_Information_Act_(United_States)) requests made by the Invisible Institute and its partners. Scraped data from the [IPRA data portal](http://portal.iprachicago.org/) is also included.

[Read more about the process on the repository wiki.](https://github.com/invinst/chicago-police-data/wiki/Data-Sources)

## If you are looking for the older folder setup:

Check the Previous_Format folder. The current format is an attempt at being consistent with [Patrick Ball's talk on Principled Data Processing](https://youtube.com/watch?v=ZSunU9GQdcI)

### Using the data

This repository exists so that the public can use the data in the interest of police accountability. Feel free to explore and ask questions and share your work.

You are welcome to contribute to existing research and analysis projects, posted on the repository's [Issue Tracker under the "independent projects" label](https://github.com/invinst/shootings-data/issues?q=is%3Aopen+is%3Aissue+label%3A%22independent+project%22). If you would like to start a new project, we encourage you to create your own [new ticket](https://github.com/invinst/shootings-data/issues/new) (use the yellow "independent project" label) to collaborate with others.

### Contributing to this repository

We want your help.

Browse the ["repo issues" label on the Issue Tracker](https://github.com/invinst/shootings-data/issues?q=is%3Aopen+is%3Aissue+label%3A%22repo+issue%22) to see where help is needed.

If you come across a problem with the repository, you can open a [new ticket](https://github.com/invinst/shootings-data/issues/new) in the tracker and use the red "repo issue" label.

You are welcome to join our community and chat with us (the repo maintainers) via Slack: https://invisibleinstitute.signup.team/ -- please join us in the #data channel.

### I have a question

If you have a question about the data, where the data comes from, or anything else related, open up a [new ticket](https://github.com/invinst/shootings-data/issues/new) in the tracker and use the purple "question" label.

## Overview of the datasets

* **shootings-cpd-feb2016/** and **shootings-ipra-may2016/** -- records of shooting incidents obtained by the same FOIA request made to CPD and IPRA, respectively.
* **complaints-cpd-june2016/** and **complaints-ipra-apr2016/** -- records of complaints against officers obtained by the same FOIA request made to CPD and IPRA, respectively.
* **cpdb_complaints-cpd/** -- records of complaints against officers that are currently in the database used by Citizens Police Data Project
* **open_investigations-ipra-portal/** -- open cases under investigation by IPRA made available on their data portal since June 3, 2016. The data has been scraped into machine-friendly formats to facilitate analysis.
* **context/** -- additional datasets to supplement analyses, including definitions of incident category codes and lists of CPD employees.

[Read more about the available datasets in the Wiki.](https://github.com/invinst/shootings-data/wiki/Distinct-Datasets-Available)
