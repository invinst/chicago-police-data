# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046360_TRR-2016-aug/trr-main/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_trr-main.csv.gz

all: \
		output/trr-main.csv.gz

output/trr-main.csv.gz: \
		src/import.py \
                input/10655-FOIA-P046360-TRRdata.xlsx
	python $<


clean:
	rm output/*


# done
