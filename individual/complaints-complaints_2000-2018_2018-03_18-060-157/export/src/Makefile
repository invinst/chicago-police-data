# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-complaints_2000-2018_2018-03_18-060-157/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,
# complaints-complaints_2000-2018_2018-03_profiles.csv.gz

all: \
		output/complaints-complaints_2000-2018_2018-03.csv.gz

output/complaints-complaints_2000-2018_2018-03.csv.gz: \
		src/export.py \
		input/complaints-complaints_2000-2018_2018-03.csv.gz
	python $<


clean:
	rm output/*


# done
