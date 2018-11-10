# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/officer-filed-complaints__2017-09_p428703/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,

all: \
		output/officer-filed-complaints__2017-09.csv.gz

output/officer-filed-complaints__2017-09.csv.gz: \
		src/export.py \
		input/officer-filed-complaints__2017-09.csv.gz
	python $<


clean:
	rm output/*


# done
