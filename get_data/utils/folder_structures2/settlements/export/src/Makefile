# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/settlements_1952-2016_2017-01_/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,
# settlements_1952-2016_2017-01_profiles.csv.gz

all: \
		output/settlements_1952-2016_2017-01.csv.gz

output/settlements_1952-2016_2017-01.csv.gz: \
		src/export.py \
		input/settlements_1952-2016_2017-01.csv.gz \
		input/settlements_1952-2016_2017-01_profiles.csv.gz
	python $<


clean:
	rm output/*


# done
