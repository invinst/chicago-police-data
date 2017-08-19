# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046360_TRR-2016-aug/trr-officers/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,
# trr-officers_demographics.csv.gz

all: \
		output/trr-officers.csv.gz

output/trr-officers.csv.gz: \
		src/export.py \
		input/trr-officers.csv.gz \
		input/trr-officers_demographics.csv.gz
	python $<


clean:
	rm output/*


# done
