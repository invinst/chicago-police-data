# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/salary_2002-2017_2017-09_/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml,
# metadata_salary_2002-2017_2017-09.csv.gz

all: \
		output/salary_2002-2017_2017-09.csv.gz

output/salary_2002-2017_2017-09.csv.gz: \
		src/import.py \
		input/9-13-17_FOIA_Updated_CPD_Data-2002-2007.xls \
		input/9-13-17_FOIA_Updated_CPD_Data-2008-2012.xls \
		input/9-13-17_FOIA_Updated_CPD_Data_-2013-2017.xls
	python $<


clean:
	rm output/*


# done
