# What is this?

This is a living repository of public data about Chicago’s police officers and their encounters with the public. It includes the names of officers involved in and witness to each incident along with other metadata, for example: location, category, demographic information, and investigation status/outcomes.

This repo also includes working/work-in-progress code intended to help build a legible structure for the data so that it can be more useful to the public.

# Where did it come from?

The original data/documents are all sourced directly from the Chicago Police Department (CPD) and the Independent Police Review Authority (IPRA) and the City of Chicago. The raw files were released pursuant to FOIA requests (Freedom Of Information Act) made by the Invisible Institute and its partners.

__[Read more about the available datasets in the Wiki.](https://github.com/invinst/shootings-data/wiki/Distinct-Datasets-Available)__

If you want to know more about [this process](https://cpdb.co/method/), please post/create an issue here in GitHub about it.

# Why is it relevant right now?

On 3 June 2016, IPRA released a significant dump of audio-video recordings and PDF documents for 101 incidents that they investigated. This mass release is indexed per “log number” in a datatable at [portal.iprachicago.org](http://portal.iprachicago.org/) along with videos hosted on [Vimeo](https://vimeo.com/user51379210/videos/sort:date/format:thumbnail) and audio recordings hosted on [SoundCloud](https://soundcloud.com/ipra-455127423).

__[We've created our own mirror/archive of the June data dump. Read more in the Wiki.](https://github.com/invinst/shootings-data/wiki/Archived-Media-Files)__

Reporters and researchers are currently exploring the released information, documents and recordings but without knowing the identity of police officers involved in each incident. __[(See the News section of the Wiki.)](https://github.com/invinst/shootings-data/wiki/News)__

But this trove of information is incomplete in critical ways.

Our first goal is to use the FOIA’ed shootings data here to identify officers per incident log number in the June 3 trove.

# What are we going to do with the merged data?

We could do at least three important things with this data once it is cleaned and merged:

## 1. Support journalists

__If we can put this data into usable form for journalists, they may be able to learn more by cross-referencing the video, audio, and police reports with officer shooting histories.__

We want to try to do this as quickly as possible.

## 2. Find officers with highest number of shootings

Which Chicago Police Officers have the highest number of shootings (and which _will_ have the highest number)?

Once we have cleaned and merged the data, it will be straightforward to figure out who currently has the most shootings. This data could also be used to predict other officers' liklihoods to commit a unjustified shooting.

## 3. Link data to officer profiles

The Invisible Institute runs the Chicago Police Data Project: https://cpdb.co/

This project collects data about Chicago police officers to create profiles. For example, here is the profile of CPD officer Raymond Piwnicki: https://cpdb.co/officer/raymond-piwnicki/5875

By merging the shootings data into the Chicago Police Data Project, we can create an increasingly complete officer profile encompassing demographics, misconduct allegations, discipline, shootings, salary, employment history, and other important data points.

# Linking officer records

We need to make sure we have enough unique identifying information about each officer to make sure we can merge our three raw shootings data dumps confidently. Different rows in different dumps could contain different and useful information about involved officers.

## Officer identifiers

* Date of appointment
* Name
* Demographics
* Unit Number
* Badge ID: Badge IDs are eventually recycled and reassigned to a different Officer

## Sources of officer ID data

Look in the `Context` folder:

+ `Context/CPD Employees, one row per individual.csv`

+ `Context/CPD Employees, one row per unit assignment per officer.xlsx`

# File/folder organization

+ `shootings-append.csv` is an un-merged append of the different police shootings data dumps.

+ `Raw` --> Raw data

+ `Context` --> Context for working with the data, not all machine-readable

# Contributing

We want your help.

1. Read through the README (you probably just finished doing this).
2. Dive into the data.
3. Share questions or problems by [making an issue](https://github.com/invinst/shootings-data/issues).
4. Talk with us on Slack: https://invisibleinstitute.signup.team/ -- please join us in the #data channel
