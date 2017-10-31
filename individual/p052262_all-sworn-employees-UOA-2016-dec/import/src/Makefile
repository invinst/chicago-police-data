# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p052262_all-sworn-employees-UOA-2016-dec/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_all-sworn-units.csv.gz

all: \
		output/all-sworn-units.csv.gz

output/all-sworn-units.csv.gz: \
		src/import.py \
		input/FOIA_P052262_-_11221-FOIA-P052262-AllSwornEmployeesWithUOA.xlsx
	python $<


clean:
	rm output/*


# done
