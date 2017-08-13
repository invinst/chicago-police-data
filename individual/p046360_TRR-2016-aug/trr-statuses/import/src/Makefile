# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046360_TRR-2016-aug/trr-statuses/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_trr-statuses.csv.gz

all: \
		output/trr-statuses.csv.gz

output/trr-statuses.csv.gz: \
		src/import.py \
                input/10655-FOIA-P046360-TRRdata.xlsx
	python $<


clean:
	rm output/*


# done
