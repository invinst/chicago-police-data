# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/Chicago-Police-Board-2017-jul/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,
# full_police-board.csv.gz

all: \
		output/police-board.csv.gz

output/police-board.csv.gz: \
		src/export.py \
		input/police-board.csv.gz \
		input/accused.csv.gz
	python $<


clean:
	rm output/*


# done
