# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-complainants_1967-1999_2016-12_/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,

all: \
		output/complaints-complainants_1967-1999_2016-12.csv.gz

output/complaints-complainants_1967-1999_2016-12.csv.gz: \
		src/export.py \
		input/complaints-complainants_1967-1999_2016-12.csv.gz
	python $<


clean:
	rm output/*


# done
