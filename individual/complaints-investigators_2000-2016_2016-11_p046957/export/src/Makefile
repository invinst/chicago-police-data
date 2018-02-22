# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-investigators_2000-2016_2016-11_p046957/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,
# complaints-investigators_2000-2016_2016-11_profiles.csv.gz,
# non-CPD_complaints-investigators_2000-2016_2016-11_profiles.csv.gz

all: \
		output/complaints-investigators_2000-2016_2016-11.csv.gz

output/complaints-investigators_2000-2016_2016-11.csv.gz: \
		src/export.py \
		input/complaints-investigators_2000-2016_2016-11.csv.gz \
		input/complaints-investigators_2000-2016_2016-11_profiles.csv.gz
	python $<


clean:
	rm output/*


# done
