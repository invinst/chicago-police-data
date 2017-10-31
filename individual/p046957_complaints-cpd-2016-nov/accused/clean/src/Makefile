# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046957_complaints-cpd-2016-nov/accused/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/accused.csv.gz

output/accused.csv.gz: \
		src/clean.py \
		input/accused.csv.gz
	python $<


clean:
	rm output/*


# done
