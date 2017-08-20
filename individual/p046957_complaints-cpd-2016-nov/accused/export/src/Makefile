# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046957_complaints-cpd-2016-nov/accused/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,
# accused_demographics.csv.gz

all: \
		output/accused.csv.gz

output/accused.csv.gz: \
		src/export.py \
		input/accused.csv.gz \
		input/accused_demographics.csv.gz
	python $<


clean:
	rm output/*


# done
