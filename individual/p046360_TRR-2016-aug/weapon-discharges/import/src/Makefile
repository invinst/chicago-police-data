# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046360_TRR-2016-aug/weapon-discharges/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_weapon-discharges.csv.gz

all: \
		output/weapon-discharges.csv.gz

output/weapon-discharges.csv.gz: \
		src/import.py \
                input/10655-FOIA-P046360-TRRdata.xlsx
	python $<


clean:
	rm output/*


# done
