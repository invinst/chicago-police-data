# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/unit-history__2016-03_16-1105/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/unit-history__2016-03.csv.gz

output/unit-history__2016-03.csv.gz: \
		src/clean.py \
		input/unit-history__2016-03.csv.gz
	python3 $<


clean:
	rm output/*


# done
