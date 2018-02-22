# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/unit-history__2016-12_p052262/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml,
# metadata_unit-history__2016-12.csv.gz

all: \
		output/unit-history__2016-12.csv.gz

output/unit-history__2016-12.csv.gz: \
		src/import.py \
		input/FOIA_P052262_-_11221-FOIA-P052262-AllSwornEmployeesWithUOA.xlsx
	python $<


clean:
	rm output/*


# done
