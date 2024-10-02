# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-investigators_2000-2018_2018-08_18-060-425/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml,
# metadata_complaints-investigators_2000-2018_2018-08.csv.gz

all: \
		output/complaints-investigators_2000-2018_2018-08.csv.gz

output/complaints-investigators_2000-2018_2018-08.csv.gz: \
		src/import.py \
		input/08_28_2018_investigators.xlsx
	python3 $<


clean:
	rm output/*


# done
