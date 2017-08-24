# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/Updated-Salary-April-2017/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-unique-ids.log, assign-unique-ids.yaml
# salary_demographics.csv.gz

all: \
		output/salary.csv.gz

output/salary.csv.gz: \
		src/assign-unique-ids.py \
                input/salary.csv.gz
	python $<


clean:
	rm output/*


# done
