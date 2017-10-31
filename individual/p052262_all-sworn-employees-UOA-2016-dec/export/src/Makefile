# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p05222_all-sworn-employees-UOA-2016-dec/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,
# all-sworn-units_demographics.csv.gz

all: \
		output/all-sworn-units.csv.gz

output/all-sworn-units.csv.gz: \
		src/export.py \
		input/all-sworn-units.csv.gz \
		input/all-sworn-units_demographics.csv.gz
	python $<


clean:
	rm output/*


# done
