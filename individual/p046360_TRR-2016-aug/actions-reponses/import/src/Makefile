# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046360_TRR-2016-aug/actions-responses/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_actions-responses.csv.gz

all: \
		output/actions-responses.csv.gz

output/actions-responses.csv.gz: \
		src/import.py \
                input/10655-FOIA-P046360-TRRdata.xlsx
	python $<


clean:
	rm output/*


# done
