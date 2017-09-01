# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p059841_officer-battery-2017-jun/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_officer-battery.csv.gz

all: \
		output/officer-battery.csv.gz

output/officer-battery.csv.gz: \
		src/import.py \
		input/11716-FOIA-P059841-Karr-OfficerBatteryReport.xls
	python $<


clean:
	rm output/*


# done
