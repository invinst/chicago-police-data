# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/Illinois-Officers_2017-aug/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_il-officers.csv.gz

all: \
		output/il-officers.csv.gz

output/il-officers.csv.gz: \
		src/import.py \
		input/All_Officers08142017.xlsx
	python $<


clean:
	rm output/*


# done
