# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046957_complaints-cpd-2016-nov/victims/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_victims.csv.gz

all: \
		output/victims.csv.gz

output/victims.csv.gz: \
		src/import.py \
                input/p046957_-_report_4_-_victim_data.xls
	python $<


clean:
	rm output/*


# done
