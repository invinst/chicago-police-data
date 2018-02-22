# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/unit-history__2016-12_p052262/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,
# unit-history__2016-12_profiles.csv.gz

all: \
		output/unit-history__2016-12.csv.gz

output/unit-history__2016-12.csv.gz: \
		src/export.py \
		input/unit-history__2016-12.csv.gz \
		input/unit-history__2016-12_profiles.csv.gz
	python $<


clean:
	rm output/*


# done
