# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/Chicago-Police-Board-2017-jul/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_police-board.csv.gz

all: \
		output/police-board.csv.gz

output/police-board.csv.gz: \
		src/import.py \
		input/Stecklow_2017_05_01.xlsx \
		input/Stecklow_2017_02_28.xlsx
	python $<


clean:
	rm output/*


# done
