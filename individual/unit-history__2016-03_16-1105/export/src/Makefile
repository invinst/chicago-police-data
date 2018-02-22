# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/unit-history__2016-03_16-1105/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,
# unit-history__2016-03_profiles.csv.gz

all: \
		output/unit-history__2016-03.csv.gz

output/unit-history__2016-03.csv.gz: \
		src/export.py \
		input/unit-history__2016-03.csv.gz \
		input/unit-history__2016-03_profiles.csv.gz
	python $<


clean:
	rm output/*


# done
