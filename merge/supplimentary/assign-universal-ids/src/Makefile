# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# merge/core/assign-universal-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-universal-ids.log, assign-universal-ids.yaml,
# officer-reference.csv.gz,
# accused.csv.gz, witnesses.csv.gz, investigators.csv.gz
# trr-officers.csv.gz, trr-statuses.csv.gz
# awards.csv.gz
# salary.csv.gz

all: \
		output/officer-profiles.csv.gz

output/officer-profiles.csv.gz: \
		src/assign-universal-ids.py \
		input/officer-profiles.csv.gz \
		input/officer-reference.csv.gz \
                input/accused_demographics.csv.gz \
                input/accused.csv.gz \
                input/witnesses_demographics.csv.gz \
                input/witnesses.csv.gz \
                input/investigators_demographics.csv.gz \
                input/investigators.csv.gz \
                input/trr-officers_demographics.csv.gz \
                input/trr-officers.csv.gz \
                input/trr-statuses_demographics.csv.gz \
                input/trr-statuses.csv.gz \
                input/awards_demographics.csv.gz \
		input/awards.csv.gz \
		input/salary_demographics.csv.gz \
		input/salary.csv.gz
	python $<


clean:
	rm output/*


# done
