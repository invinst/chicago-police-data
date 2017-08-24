# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/Updated-Salary-April-2017/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml, metadata_salary.csv.gz

all: \
		output/salary.csv.gz

output/salary.csv.gz: \
		src/clean.py \
                input/salary.csv.gz
	python $<


clean:
	rm output/*


# done
