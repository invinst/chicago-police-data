# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046360_TRR-2016-aug/subject-weapons/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_subject-weapons.csv.gz

all: \
		output/subject-weapons.csv.gz

output/subject-weapons.csv.gz: \
		src/import.py \
                input/10655-FOIA-P046360-TRRdata.xlsx
	python $<


clean:
	rm output/*


# done
