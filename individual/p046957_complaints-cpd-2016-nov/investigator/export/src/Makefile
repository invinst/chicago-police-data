# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046957_complaints-cpd-2016-nov/investigators/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,
# investigators_demographics.csv.gz,
# non-CPD_investigators_demographics.csv.gz

all: \
		output/investigators.csv.gz

output/investigators.csv.gz: \
		src/export.py \
		input/investigators.csv.gz \
		input/investigators_demographics.csv.gz
	python $<


clean:
	rm output/*


# done
