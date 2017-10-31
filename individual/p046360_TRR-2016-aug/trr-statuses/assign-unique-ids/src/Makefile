# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046360_TRR-2016-aug/trr-statuses/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-unique-ids.log, assign-unique-ids.yaml,
# trr-statuses_demographics.csv.gz

all: \
		output/trr-statuses.csv.gz

output/trr-statuses.csv.gz: \
		src/assign-unique-ids.py \
                input/trr-statuses.csv.gz
	python $<


clean:
	rm output/*


# done
