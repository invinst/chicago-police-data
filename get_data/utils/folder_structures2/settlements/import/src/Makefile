# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/settlements_1952-2016_2017-01_/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml,
# metadata_settlements_1952-2016_2017-01.csv.gz

all: \
		output/settlements_1952-2016_2017-01.csv.gz

output/settlements_1952-2016_2017-01.csv.gz: \
		src/import.py \
                input/settlements.csv
	python $<


clean:
	rm output/*


# done
