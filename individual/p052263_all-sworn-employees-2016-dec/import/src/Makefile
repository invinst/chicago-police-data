# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p052263_all-sworn-employees-2016-dec/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_all-sworn.csv.gz

all: \
		output/all-sworn.csv.gz

output/all-sworn.csv.gz: \
		src/import.py \
		input/FOIA_P052263_-_11220-FOIA-P052263-AllSwornEmployees.xlsx
	python $<


clean:
	rm output/*


# done
