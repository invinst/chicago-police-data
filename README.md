# What is this data about?

This is data about shootings by Chicago Police Officers.

# Where did it come from?

IPRA: the Independent Police Review Authority.

This data was obtained by the The Invisible Institute through FOIA requests.

# Why is it relevant right now?

The Invisible Institute obtained this data during February, April, and May 2016.

On June 3, 2016, IPRA released a major trove of video, audio, and police reports from police-involved incidents:

+ [Vimeo: IPRA Chicago’s Videos](https://vimeo.com/user51379210/videos/sort:plays/format:thumbnail)
+ [IPRA Chicago Data Portal](http://portal.iprachicago.org/)

Journalists are currently digging through that data and writing about it:

+ [Chicago Tribune: Video releases include police fatal shooting, off-duty Chicago cop's punches at Portillo's](http://chicago.suntimes.com/politics/city-releases-police-misconduct-files-videos/)
+ [DNAinfo: Was Force Justified? What We Found In The Videos Released By IPRA So Far](https://www.dnainfo.com/chicago/20160603/west-town/chicago-police-misconduct-videos-released-by-ipra-show-shootings-more)
+ [BuzzFeed: Chicago Releases 300 Videos From Police Misconduct Investigations](https://www.buzzfeed.com/mikehayes/chicago-police-video-release?utm_term=.pdlwaZEGYM#.qhvOxpWQv5)
+ [NBC Chicago: IPRA Releases Hundreds of Videos of Police Shootings, Incidents](http://www.nbcchicago.com/news/local/IPRA-to-Release-Police-Misconduct-Videos-381758681.html)
+ _and many more..._

But the June 3 trove is incomplete in critical ways.

According to what we see now, the Officers involved are not identified in the June 3 trove -- incidents are identified by complaint numbers only. That is very frustrating if you want to research a particular Officer and combine information from different sources.

Our first goal is to use the FOIA'ed shootings data to identify officers in the June 3 trove.

# What are we going to do with the merged data?

We could do at least three important things with this data once it is cleaned and merged:

## 1. Assist journalists

__If we can put this data into usable form for journalists, they may be able to learn more by cross-referencing the video, audio, and police reports with officer shooting histories.__

We want to try to do this as quickly as possible.

## 2. Find officers with highest number of shootings

Which Chicago Police Officers have the highest number of shootings?

Once we have cleaned and merged the data, it will be straightforward to answer this question.

## 3. Link data to officer profiles

The Invisible Institute runs the Chicago Police Data Project: https://cpdb.co/

This project collects data about Chicago police officers to create profiles. For example, here is the profile of CPD officer Raymond Piwnicki: https://cpdb.co/officer/raymond-piwnicki/5875

By merging the shootings data into the Chicago Police Data Project, we can create an increasingly complete officer profile encompassing demographics, misconduct allegations, discipline, shootings, salary, employment history, and other important data points.

# Linking officer records

We need to make sure we have enough unique identifiying information about each officer to make sure we can merge our three raw shootings data dumps confidently. Different rows in different dumps could contain different and useful information about involved officers.

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

+ `shootings-append.csv` is a merge of the three data dumps. It needs to be deduplicated, cleaned, analyzed. This is the "final product" of the repo and is a work in progress.

+ `Master.do` --> instruction file for Stata

+ `Raw` --> Raw data

+ `Context` --> Context for working with the data, not all machine-readable

# Data quality

+ Of the May, April, and February dumps, February is the most incomplete.

+ We are still in the discovery phase: actively digging through the data to grok it, analyze it, clean it,  and document it.

# Contributing

We want your help.

1. Read through the README (you probably just finished doing this).
2. Dive into the data.
3. Share questions or problems by [making an issue](https://github.com/invinst/shootings-data/issues).
4. Join us on Slack (we'll share our public Slack invite page soon).
