# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/awards_1967-2017_2017-08_p061715/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,
# awards_1967-2017_2017-08_profiles.csv.gz

all: \
		output/awards_1967-2017_2017-08.csv.gz

output/awards_1967-2017_2017-08.csv.gz: \
		src/export.py \
		input/awards_1967-2017_2017-08.csv.gz \
		input/awards_1967-2017_2017-08_profiles.csv.gz
	python $<


clean:
	rm output/*


# done
