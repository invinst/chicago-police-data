# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# merge/assign-universal-ids/01_rosters/src/Makefile

.PHONY: all clean
# Additional files created: assign-universal-ids.log, assign-universal-ids.yaml,
# all-members.csv.gz, all-sworn.csv.gz
# officer-reference.csv.gz

all: \
		output/officer-profiles.csv.gz

output/officer-profiles.csv.gz: \
		src/assign-universal-ids.py \
                input/all-members_demographics.csv.gz \
                input/all-members.csv.gz \
                input/all-sworn_demographics.csv.gz \
                input/all-sworn.csv.gz
	python $<


clean:
	rm output/*


# done
