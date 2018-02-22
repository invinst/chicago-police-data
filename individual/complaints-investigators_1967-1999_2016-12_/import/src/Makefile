# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-investigators_1967-1999_2016-12_/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml,
# metadata_complaints-investigators_1967-1999_2016-12.csv.gz

all: \
		output/complaints-investigators_1967-1999_2016-12.csv.gz

output/complaints-investigators_1967-1999_2016-12.csv.gz: \
		src/import.py \
		input/MAINFRAME_COMPLAINTS_REVISED_30NOV2016.CSV
	python $<


clean:
	rm output/*


# done
