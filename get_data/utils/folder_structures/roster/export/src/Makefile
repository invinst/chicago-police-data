# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/roster__2018-03_p441436/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,
# roster__2018-03_profiles.csv.gz,

all: \
		output/roster__2018-03.csv.gz

output/roster__2018-03.csv.gz: \
		src/export.py \
		input/roster__2018-03.csv.gz \
		input/roster__2018-03_profiles.csv.gz
	python3 $<


clean:
	rm output/*


# done
