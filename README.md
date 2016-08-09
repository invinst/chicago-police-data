# Chicago Police Incidents Data

## What is this?

This is a living repository of public data about Chicago’s police officers and their encounters with the public, maintained by the Invisible Institute and other volunteers from the Chicago community. The datasets deal with several different categories of incidents, including shootings, use-of-force, complaints of misconduct, and open investigations by the Independent Police Review Authority (IPRA). 

This repository also serves as a hub to facilitate the Chicago community's efforts to use the data in the interest of promoting police accountability. Interactions are primarily coordinated via the repository's [Issue Tracker](https://github.com/invinst/shootings-data/issues) and organized using labels. 

More specific details about the datasets here are discussed later in this readme. 

## Where did it come from?

The original data and documents are all sourced directly from the Chicago Police Department (CPD), the Independent Police Review Authority (IPRA), or the City of Chicago. The raw files were released pursuant to [Freedom of Information Act (FOIA)](https://www.wikiwand.com/en/Freedom_of_Information_Act_(United_States)) requests made by the Invisible Institute and its partners. Scraped data from the [IPRA data portal](http://portal.iprachicago.org/) is also included.

[Read more about the process on the respository wiki.](https://github.com/invinst/chicago-police-data/wiki/Data-Sources)

## Why is it relevant right now?

Police accountability and misconduct has long been a major issue in Chicago, with the [Invisible Institute](https://cpdb.co/story/) working on it for more than a decade. It has gained especially increased attention after the [October 2014 shooting of Laquan McDonald](https://www.wikiwand.com/en/Shooting_of_Laquan_McDonald) and is an active issue being investigated and worked on by the community, the city government, and even the U.S. Department of Justice. 

[Read more about the history and recent news of police misconduct in Chicago and recent developments on the repository wiki.](https://github.com/invinst/chicago-police-data/wiki/News)

## What can I do?

### Using the data

This repository exists so that the public can use the data in the interest of police accountability. Feel free to dive into the data. 

You are welcome to contribute to existing research and analysis projects, posted on the repository's [Issue Tracker under the "independent projects" label](https://github.com/invinst/shootings-data/issues?q=is%3Aopen+is%3Aissue+label%3A%22independent+project%22). If you would like to start a new project, we encourage you to create your own [new ticket](https://github.com/invinst/shootings-data/issues/new) (use the yellow "independent project" label) to collaborate with others. 

### Contributing to this repository

We want your help. 

Browse the ["repo issues" label on the Issue Tracker](https://github.com/invinst/shootings-data/issues?q=is%3Aopen+is%3Aissue+label%3A%22repo+issue%22) to see what needs help. 

If you come across a problem with the repository, you can open a [new ticket](https://github.com/invinst/shootings-data/issues/new) in the tracker and use the red "repo issue" label.

You are welcome to talk with us (the repo maintainers) on Slack: https://invisibleinstitute.signup.team/ -- please join us in the #data channel.

### I have a question

If you have a question about the data, where the data comes from, or anything else related, open up a [new ticket](https://github.com/invinst/shootings-data/issues/new) in the tracker and use the purple "question" label. 

## Overview of the datasets

* **shootings-cpd-feb2016/** and **shootings-ipra-may2016/** -- records of shooting incidents obtained by the same FOIA request made to CPD and IPRA, respectively. 
* **complaints-cpd-june2016/** and **complaints-ipra-apr2016/** -- records of complaints against officers obtained by the same FOIA request made to CPD and IPRA, respectively. 
* **cpdb_complaints-cpd/** -- records of complaints against officers that are currently in the database used by Citizens Police Data Project
* **open_investigations-ipra-portal/** -- open cases under investigation by IPRA made available on their data portal since June 3, 2016. The data has been scraped into machine-friendly formats to facilitate analysis. 
* **context/** -- additional datasets to supplement analyses, including definitions of incident category codes and lists of CPD employees. 

[Read more about the available datasets in the Wiki.](https://github.com/invinst/shootings-data/wiki/Distinct-Datasets-Available)