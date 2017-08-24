# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/Updated-Salary-April-2017/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_salary.csv.gz

all: \
		output/salary.csv.gz

output/salary.csv.gz: \
		src/import.py \
                input/FOIA_Updated_CPD_Data_2002-2007.xls \
                input/FOIA_Updated_CPD_Data_2008-2012.xls \
                input/FOIA_Updated_CPD_Data_2013-2017.xls
	python $<


clean:
	rm output/*


# done
