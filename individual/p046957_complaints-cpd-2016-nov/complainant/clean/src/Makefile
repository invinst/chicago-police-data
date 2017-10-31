# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046957_complaints-cpd-2016-nov/complainants/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/complainants.csv.gz

output/complainants.csv.gz: \
		src/clean.py \
		input/complainants.csv.gz
	python $<


clean:
	rm output/*


# done
