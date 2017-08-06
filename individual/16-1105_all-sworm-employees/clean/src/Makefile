# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/16-1105_all-sworn-members/clean/src/Makefile

.PHONY: all clean
# Additional files creates: clean.log, clean.yaml

all: \
		output/unit-history.csv.gz

output/unit-history.csv.gz: \
		src/clean.py \
		input/unit-history.csv.gz
	python $<


clean:
	rm output/*


# done
