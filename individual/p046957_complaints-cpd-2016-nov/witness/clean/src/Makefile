# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046957_complaints-cpd-2016-nov/witness/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/witnesses.csv.gz

output/witnesses.csv.gz: \
		src/clean.py \
		input/witnesses.csv.gz
	python $<


clean:
	rm output/*


# done
