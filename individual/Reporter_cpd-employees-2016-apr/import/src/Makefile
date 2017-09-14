# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/Reporter_cpd-employees-2016-apr/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_cpd-employees.csv.gz

all: \
		output/cpd-employees.csv.gz

output/cpd-employees.csv.gz: \
		src/import.py \
		input/cpd_employees-4-1-16.csv
	python $<


clean:
	rm output/*


# done
