# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-witnesses_1967-1999_2016-12_/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,
# complaints-witnesses_1967-1999_2016-12_profiles.csv.gz

all: \
		output/complaints-witnesses_1967-1999_2016-12.csv.gz

output/complaints-witnesses_1967-1999_2016-12.csv.gz: \
		src/export.py \
		input/complaints-witnesses_1967-1999_2016-12.csv.gz \
		input/complaints-witnesses_1967-1999_2016-12_profiles.csv.gz
	python $<


clean:
	rm output/*


# done
