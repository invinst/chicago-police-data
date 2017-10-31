# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p428703_officer-complaints-2017-sep/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_officer-complaints.csv.gz

all: \
		output/officer-complaints.csv.gz

output/officer-complaints.csv.gz: \
		src/import.py \
		input/tabula-FOIA_P428703_Responsive_Records.csv
	python $<


clean:
	rm output/*


# done
