# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# merge/04_TRRs/src/Makefile

.PHONY: all clean
# Additional files created: merge.log, merge.yaml,
# trr-officers.csv.gz, trr-statuses.csv.gz,
# officer-reference.csv.gz

all: \
		output/officer-profiles.csv.gz

output/officer-profiles.csv.gz: \
		src/merge.py \
		input/officer-profiles.csv.gz \
		input/officer-reference.csv.gz \
                input/trr-officers_demographics.csv.gz \
                input/trr-officers.csv.gz \
                input/trr-statuses_demographics.csv.gz \
                input/trr-statuses.csv.gz
	python $<


clean:
	rm output/*


# done
