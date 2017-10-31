# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p058155_cpd-employees-2017-apr/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_cpd-employees.csv.gz

all: \
		output/cpd-employees.csv.gz

output/cpd-employees.csv.gz: \
		src/import.py \
		input/P058155_-_Kiefer.xlsx
	python $<


clean:
	rm output/*


# done
