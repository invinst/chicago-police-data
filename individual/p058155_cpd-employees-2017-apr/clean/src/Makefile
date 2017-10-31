# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/Reporter_cpd-employees-2016-apr/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/cpd-employees.csv.gz

output/cpd-employees.csv.gz: \
		src/clean.py \
		input/cpd-employees.csv.gz
	python $<


clean:
	rm output/*


# done
