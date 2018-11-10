# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/officer-filed-complaints__2017-09_p428703/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml,
# metadata_officer-filed-complaints__2017-09.csv.gz

all: \
		output/officer-filed-complaints__2017-09.csv.gz

output/officer-filed-complaints__2017-09.csv.gz: \
		src/import.py \
		input/tabula-FOIA_P428703_Responsive_Records.csv
	python $<


clean:
	rm output/*


# done
