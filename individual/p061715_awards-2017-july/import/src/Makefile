# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p061715_awards-2017-july/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_awards.csv.gz

all: \
		output/awards.csv.gz

output/awards.csv.gz: \
		src/import.py \
                input/Part_1.xlsx \
                input/Part_2.xlsx \
                input/Part_3.xlsx \
                input/Part_4.xlsx \
                input/Part_5.xlsx \
                input/Part_6.xlsx
	python $<


clean:
	rm output/*


# done
