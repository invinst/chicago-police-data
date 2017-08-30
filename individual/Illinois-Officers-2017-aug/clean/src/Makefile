# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/Illinois-Officers_2017-aug/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/il-officers.csv.gz

output/il-officers.csv.gz: \
		src/clean.py \
		input/il-officers.csv.gz
	python $<


clean:
	rm output/*


# done
