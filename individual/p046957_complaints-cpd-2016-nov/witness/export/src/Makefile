# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046957_complaints-cpd-2016-nov/witnesses/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,
# witnesses_demographics.csv.gz

all: \
		output/witnesses.csv.gz

output/witnesses.csv.gz: \
		src/export.py \
		input/witnesses.csv.gz \
		input/witnesses_demographics.csv.gz
	python $<


clean:
	rm output/*


# done
