# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046957_complaints-cpd-2016-nov/complaints/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/complaints.csv.gz

output/complaints.csv.gz: \
		src/clean.py \
		input/complaints.csv.gz
	python $<


clean:
	rm output/*


# done
