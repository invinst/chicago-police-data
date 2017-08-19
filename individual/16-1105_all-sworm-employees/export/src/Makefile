# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/16-1105_all-sworn-members/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,
# unit-history_demographics.csv.gz

all: \
		output/unit-history.csv.gz

output/unit-history.csv.gz: \
		src/export.py \
		input/unit-history.csv.gz \
		input/unit-history_demographics.csv.gz
	python $<


clean:
	rm output/*


# done
