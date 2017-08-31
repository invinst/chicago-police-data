# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p061715_awards-2017-july/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_awards.csv.gz

all: \
		output/awards.csv.gz

output/awards.csv.gz: \
		src/import.py \
                input/Awards_Data_(New_Copy).csv
	python $<


clean:
	rm output/*


# done
