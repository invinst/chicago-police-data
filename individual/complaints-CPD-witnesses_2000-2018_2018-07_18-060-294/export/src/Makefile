# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-CPD-witnesses_2000-2018_2018-07_18-060-294/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,
# complaints-CPD-witnesses_2000-2018_2018-07_profiles.csv.gz

all: \
		output/complaints-CPD-witnesses_2000-2018_2018-07.csv.gz

output/complaints-CPD-witnesses_2000-2018_2018-07.csv.gz: \
		src/export.py \
		input/complaints-CPD-witnesses_2000-2018_2018-07.csv.gz \
		input/complaints-CPD-witnesses_2000-2018_2018-07_profiles.csv.gz
	python3 $<


clean:
	rm output/*


# done
