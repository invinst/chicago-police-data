# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/TRR-officers_2004-2016_2016-09_p046360/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,
# TRR-officers_2004-2016_2016-09_profiles.csv.gz

all: \
		output/TRR-officers_2004-2016_2016-09.csv.gz

output/TRR-officers_2004-2016_2016-09.csv.gz: \
		src/export.py \
		input/TRR-officers_2004-2016_2016-09.csv.gz \
		input/TRR-officers_2004-2016_2016-09_profiles.csv.gz
	python $<


clean:
	rm output/*


# done
