# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/Chicago-Police-Board-2017-jul/clean/src/Makefile

.PHONY: all clean
# Additional files created: clean.log, clean.yaml

all: \
		output/police-board.csv.gz

output/police-board.csv.gz: \
		src/clean.py \
		input/police-board.csv.gz
	python $<


clean:
	rm output/*


# done
