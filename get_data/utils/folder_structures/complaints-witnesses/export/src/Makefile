# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-witnesses_2000-2016_2016-11_p046957es/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,
# complaints-witnesses_2000-2016_2016-11_profiles.csv.gz

all: \
		output/complaints-witnesses_2000-2016_2016-11.csv.gz

output/complaints-witnesses_2000-2016_2016-11.csv.gz: \
		src/export.py \
		input/complaints-witnesses_2000-2016_2016-11.csv.gz \
		input/complaints-witnesses_2000-2016_2016-11_profiles.csv.gz
	python3 $<


clean:
	rm output/*


# done
