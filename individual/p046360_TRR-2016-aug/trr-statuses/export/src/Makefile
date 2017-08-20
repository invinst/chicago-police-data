# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046360_TRR-2016-aug/trr-statuses/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,
# trr-statuses_demographics.csv.gz

all: \
		output/trr-statuses.csv.gz

output/trr-statuses.csv.gz: \
		src/export.py \
		input/trr-statuses.csv.gz \
		input/trr-statuses_demographics.csv.gz
	python $<


clean:
	rm output/*


# done
