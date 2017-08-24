# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/Updated-Salary-April-2017/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml
# salary_demographics.csv.gz

all: \
		output/salary.csv.gz

output/salary.csv.gz: \
		src/export.py \
                input/salary.csv.gz
	python $<


clean:
	rm output/*


# done
