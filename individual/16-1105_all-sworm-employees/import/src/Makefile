# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/16-1105_all-sworn-members/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_unit-history.csv.gz

all: \
		output/unit-history.csv.gz

output/unit-history.csv.gz: \
		src/import.py \
		input/Kalven_16-1105_All_Sworn_Employees.xlsx
	python $<


clean:
	rm output/*


# done
