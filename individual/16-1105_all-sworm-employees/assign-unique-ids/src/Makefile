# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/16-1105_all-sworn-members/clean/src/Makefile

.PHONY: all clean
# Additional files created:
# assign-unique-ids.log, assign-unique-ids.yaml, unit-history_demographics.csv.gz

all: \
		output/unit-history.csv.gz

output/unit-history.csv.gz: \
		src/assign-unique-ids.py \
		input/unit-history.csv.gz
	python $<


clean:
	rm output/*


# done
