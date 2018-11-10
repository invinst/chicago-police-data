# -*- coding: utf-8 -*-

# Author(s):	Roman Rivera (Invisible Institute)
# individual/unit-history__2016-03_16-1105/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml,
# metadata_unit-history__2016-03.csv.gz

all: \
		output/unit-history__2016-03.csv.gz

output/unit-history__2016-03.csv.gz: \
		src/import.py \
		input/Kalven_16-1105_All_Sworn_Employees.xlsx
	python3 $<


clean:
	rm output/*


# done
