# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-complaints_2000-2016_2016-11_p046957/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml

all: \
		output/complaints-complaints_2000-2016_2016-11.csv.gz

output/complaints-complaints_2000-2016_2016-11.csv.gz: \
		src/export.py \
		input/complaints-complaints_2000-2016_2016-11.csv.gz
	python $<


clean:
	rm output/*


# done
